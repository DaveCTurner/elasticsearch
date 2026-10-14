/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.fs.FsBlobContainer;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThrottledIterator;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotsServiceUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo.canonicalName;

/**
 * <p>
 * Represents the process of deleting some collection of snapshots within a repository which since 7.6.0 looks like this:
 * </p>
 * <ul>
 *     <li>Write a new {@link BlobStoreIndexShardSnapshots} for each affected shard, and compute the blobs to delete.</li>
 *     <li>Update the {@link RepositoryData} to remove references to deleted snapshots/indices and point to the new
 *     {@link BlobStoreIndexShardSnapshots} files.</li>
 *     <li>Remove up any now-unreferenced blobs.</li>
 * </ul>
 * <p>
 *     Until the {@link RepositoryData} is updated there should be no other activities in the repository, and in particular the root
 *     blob must not change until it is updated by this deletion and the {@code repositoryDataUpdateListener} is completed.
 * </p>
 */
class SnapshotsDeletion {

    private static final Logger logger = LogManager.getLogger(SnapshotsDeletion.class);

    /**
     * The repository from which we're deleting the snapshots.
     */
    private final BlobStoreRepository repository;

    /**
     * Executor to use for all repository interactions.
     */
    private final Executor snapshotExecutor;

    /**
     * Executor to use to perform the actual deletions of stale blobs.
     */
    private final ThrottledTaskRunner staleBlobDeleteRunner;

    /**
     * NamedXContentRegistry to use when reading XContent blobs.
     */
    private final NamedXContentRegistry namedXContentRegistry;

    /**
     * The IDs of the snapshots to delete. This set is empty if the deletion is a repository cleanup.
     */
    private final Set<SnapshotId> snapshotIds;

    /**
     * The {@link RepositoryData} generation at the start of the process, to ensure that the {@link RepositoryData} does not change
     * while the new {@link BlobStoreIndexShardSnapshots} are being written.
     */
    private final long originalRepositoryDataGeneration;

    /**
     * The minimum {@link IndexVersion} of the nodes in the cluster and the snapshots remaining in the repository. The repository must
     * remain readable by all node versions which support this {@link IndexVersion}.
     */
    private final IndexVersion repositoryFormatIndexVersion;

    /**
     * Whether the {@link #repositoryFormatIndexVersion} is new enough to support naming {@link BlobStoreIndexShardSnapshots} blobs with
     * UUIDs (i.e. does not need to remain compatible with versions before v7.6.0). Older repositories use (unsafe) numeric indices for
     * these blobs instead.
     */
    private final boolean useShardGenerations;

    /**
     * All blobs in the repository root at the start of the operation, obtained by listing the repository contents. Note that this may
     * include some blobs which are no longer referenced by the current {@link RepositoryData}, but which have not yet been removed by
     * the cleanup that follows an earlier deletion. This cleanup may still be ongoing (we do not wait for it to complete before
     * starting the next repository operation) or it may have failed before completion (it could have been running on a different node,
     * which crashed for unrelated reasons) so we track all the blobs here and clean them up again at the end.
     */
    private final Map<String, BlobMetadata> originalRootBlobs;

    /**
     * All index containers at the start of the operation, obtained by listing the repository contents. Note that this may include some
     * containers which are no longer referenced by the current {@link RepositoryData}, but which have not yet been removed by
     * the cleanup that follows an earlier deletion. This cleanup may or may not still be ongoing (it could have been running on a
     * different node, which died before completing it) so we track all the blobs here and clean them up again at the end.
     */
    private final Map<String, BlobContainer> originalIndexContainers;

    /**
     * The {@link RepositoryData} at the start of the operation, obtained after verifying that {@link #originalRootBlobs} contains no
     * {@link RepositoryData} blob newer than the one identified by {@link #originalRepositoryDataGeneration}.
     */
    private final RepositoryData originalRepositoryData;

    /**
     * Accumulates the number of blobs deleted by this operation.
     */
    // NB only counts stale root blobs today, not shard-level blobs
    private final AtomicLong blobsDeleted = new AtomicLong();

    /**
     * Accumulates the total size of the blobs deleted by this operation.
     */
    // NB only counts stale root blobs today, not shard-level blobs
    private final AtomicLong bytesDeleted = new AtomicLong();

    /**
     * Tracks the shard-level blobs which can be deleted once all the metadata updates have completed.
     */
    private final BlobStoreRepository.BlobsToDelete blobsToDelete;

    SnapshotsDeletion(
        BlobStoreRepository repository,
        Executor snapshotExecutor,
        ThrottledTaskRunner staleBlobDeleteRunner,
        NamedXContentRegistry namedXContentRegistry,
        Set<SnapshotId> snapshotIds,
        long originalRepositoryDataGeneration,
        IndexVersion repositoryFormatIndexVersion,
        Map<String, BlobMetadata> originalRootBlobs,
        Map<String, BlobContainer> originalIndexContainers,
        RepositoryData originalRepositoryData
    ) {
        this.repository = repository;
        this.snapshotExecutor = snapshotExecutor;
        this.staleBlobDeleteRunner = staleBlobDeleteRunner;
        this.namedXContentRegistry = namedXContentRegistry;
        this.snapshotIds = snapshotIds;
        this.originalRepositoryDataGeneration = originalRepositoryDataGeneration;
        this.repositoryFormatIndexVersion = repositoryFormatIndexVersion;
        this.useShardGenerations = SnapshotsServiceUtils.useShardGenerations(repositoryFormatIndexVersion);
        this.originalRootBlobs = originalRootBlobs;
        this.originalIndexContainers = originalIndexContainers;
        this.originalRepositoryData = originalRepositoryData;
        this.blobsToDelete = repository.new BlobsToDelete();
    }

    // ---------------------------------------------------------------------------------------------------------------------------------
    // The overall flow of execution

    void runDelete(ActionListener<RepositoryData> repositoryDataUpdateListener, Runnable onCompletion) {
        final var releasingListener = repositoryDataUpdateListener.delegateResponse((l, e) -> {
            try {
                blobsToDelete.close();
            } finally {
                l.onFailure(e);
            }
        });
        final Runnable releasingOnCompletion = () -> {
            try {
                blobsToDelete.close();
            } finally {
                onCompletion.run();
            }
        };
        if (useShardGenerations) {
            runWithUniqueShardMetadataNaming(releasingListener, releasingOnCompletion);
        } else {
            runWithLegacyNumericShardMetadataNaming(repository.wrapWithWeakConsistencyProtection(releasingListener), releasingOnCompletion);
        }
    }

    private void runWithUniqueShardMetadataNaming(ActionListener<RepositoryData> repositoryDataUpdateListener, Runnable onCompletion) {
        SubscribableListener

            // First write the new shard state metadata (without the removed snapshots) and compute deletion targets
            .newForked(this::writeUpdatedShardMetadataAndComputeDeletes)

            .<RepositoryData>andThen(l -> {
                // Once we have put the new shard-level metadata into place, we can update the repository metadata as follows:
                // 1. Remove the snapshots from the list of existing snapshots
                // 2. Update the index shard generations of all updated shard folders
                //
                // Note: If we fail updating any of the individual shard paths, it may not have a new BlobStoreIndexShardSnapshots blob,
                // but if it does then that doesn't matter, we're naming them uniquely as index-{$shard-generation-uuid} and it won't be
                // referenced by the new RepositoryData and will be cleaned up by a subsequent delete.
                //
                // TODO should we even write the new RepositoryData unless all shard paths have been successfully updated? See #100569.
                updateRepositoryData(originalRepositoryData.removeSnapshots(snapshotIds, blobsToDelete.getUpdatedShardGenerations()), l);
            })

            .<RepositoryData>andThen(
                // writeIndexGen finishes on master-service thread so must fork here.
                snapshotExecutor,
                repository.threadPool().getThreadContext(),
                (l, newRepositoryData) -> {
                    l.onResponse(newRepositoryData);
                    // Once we have updated the repository, run the unreferenced blobs cleanup in parallel to shard-level snapshot
                    // deletion
                    try (var refs = new RefCountingRunnable(onCompletion)) {
                        cleanupUnlinkedRootAndIndicesBlobs(newRepositoryData, refs.acquireListener());
                        cleanupUnlinkedShardLevelBlobs(refs.acquireListener());
                    }
                }
            )

            .addListener(repositoryDataUpdateListener);
    }

    private void runWithLegacyNumericShardMetadataNaming(
        ActionListener<RepositoryData> repositoryDataUpdateListener,
        Runnable onCompletion
    ) {
        // Write the new repository data first (with the removed snapshot), using no shard generations
        updateRepositoryData(
            originalRepositoryData.removeSnapshots(snapshotIds, ShardGenerations.EMPTY),
            repositoryDataUpdateListener.delegateFailure((delegate, newRepositoryData) -> {
                try (var refs = new RefCountingRunnable(() -> {
                    delegate.onResponse(newRepositoryData);
                    onCompletion.run();
                })) {
                    // Run unreferenced blobs cleanup in parallel to shard-level snapshot deletion
                    cleanupUnlinkedRootAndIndicesBlobs(newRepositoryData, refs.acquireListener());

                    // writeIndexGen finishes on master-service thread so must fork here.
                    snapshotExecutor.execute(
                        ActionRunnable.wrap(
                            refs.acquireListener(),
                            l0 -> writeUpdatedShardMetadataAndComputeDeletes(
                                l0.delegateFailure((l, ignored) -> cleanupUnlinkedShardLevelBlobs(l))
                            )
                        )
                    );
                }
            })
        );
    }

    void runCleanup(ActionListener<DeleteResult> listener) {
        final Set<String> survivingIndexIds = originalRepositoryData.getIndices()
            .values()
            .stream()
            .map(IndexId::getId)
            .collect(Collectors.toSet());
        final List<String> staleRootBlobs = staleRootBlobs(originalRepositoryData, originalRootBlobs.keySet());
        final var releasingListener = ActionListener.releaseAfter(listener, blobsToDelete);
        if (survivingIndexIds.equals(originalIndexContainers.keySet()) && staleRootBlobs.isEmpty()) {
            // Nothing to clean up we return
            releasingListener.onResponse(DeleteResult.ZERO);
        } else {
            // write new index-N blob to ensure concurrent operations will fail
            updateRepositoryData(
                originalRepositoryData,
                releasingListener.delegateFailureAndWrap(
                    // TODO should we pass newRepositoryData to cleanupStaleBlobs()?
                    (l, newRepositoryData) -> cleanupUnlinkedRootAndIndicesBlobs(
                        originalRepositoryData,
                        l.map(ignored -> DeleteResult.of(blobsDeleted.get(), bytesDeleted.get()))
                    )
                )
            );
        }
    }

    // ---------------------------------------------------------------------------------------------------------------------------------
    // Updating the shard-level metadata and accumulating results

    private void writeUpdatedShardMetadataAndComputeDeletes(ActionListener<Void> listener) {
        // noinspection resource -- closed safely at the end of the iteration
        final var listeners = new RefCountingListener(listener);

        // Each per-index process takes some nonzero amount of working memory to hold the relevant snapshot IDs and metadata generations
        // etc. which we can keep under tighter limits and release sooner if we limit the number of concurrently processing indices.
        // Each one needs at least one snapshot thread at all times, so threadPool.info(SNAPSHOT).getMax() of them at once is enough to
        // keep the threadpool fully utilized.
        ThrottledIterator.run(
            originalRepositoryData.getIndices().values().iterator(),
            (ref, indexId) -> ActionListener.run(
                ActionListener.releaseAfter(listeners.acquire(), ref),
                new IndexSnapshotsDeletionLauncher(indexId)
            ),
            repository.threadPool().info(ThreadPool.Names.SNAPSHOT).getMax(),
            listeners::close
        );
    }

    /// Works out whether we need to process the index at all, and if it does need processing then computes some needed data and invokes
    /// [IndexSnapshotsDeletion].
    private class IndexSnapshotsDeletionLauncher implements CheckedConsumer<ActionListener<Void>, RuntimeException> {
        private final IndexId indexId;
        private final List<SnapshotId> indexSnapshotIds;

        IndexSnapshotsDeletionLauncher(IndexId indexId) {
            this.indexId = indexId;
            this.indexSnapshotIds = originalRepositoryData.getSnapshots(indexId);
        }

        @Override
        public void accept(ActionListener<Void> listener) {
            if (requiresProcessing() == false) {
                listener.onResponse(null);
                return;
            }

            final var survivingSnapshots = Sets.<SnapshotId>newHashSetWithExpectedSize(indexSnapshotIds.size());
            final var survivingMetaBlobIds = Sets.<String>newHashSetWithExpectedSize(indexSnapshotIds.size());
            final var affectedMetaBlobIds = Sets.<String>newHashSetWithExpectedSize(Math.min(indexSnapshotIds.size(), snapshotIds.size()));
            for (final var indexSnapshotId : indexSnapshotIds) {
                final var metaBlobId = originalRepositoryData.indexMetaDataGenerations().indexMetaBlobId(indexSnapshotId, indexId);
                if (snapshotIds.contains(indexSnapshotId)) {
                    // This meta blob is not necessarily deleted because of deduplication: it might be in use by surviving snapshots.
                    affectedMetaBlobIds.add(metaBlobId);
                } else {
                    survivingSnapshots.add(indexSnapshotId);
                    survivingMetaBlobIds.add(metaBlobId);
                }
            }

            assert survivingMetaBlobIds.isEmpty() == false;
            assert affectedMetaBlobIds.isEmpty() == false;
            assert survivingSnapshots.isEmpty() == false;

            deleteUnreferencedMetaBlobs(affectedMetaBlobIds, survivingMetaBlobIds);
            new IndexSnapshotsDeletion(indexId, survivingSnapshots).run(affectedMetaBlobIds, listener);
        }

        /// An index requires processing (i.e. by [IndexSnapshotsDeletion]) if some of its snapshots are removed but others survive. If
        /// all its snapshots survive then no further processing is needed. If all its snapshots are removed then the whole
        /// `indices/${UUID}` tree is dropped by [SnapshotsDeletion#cleanupUnlinkedRootAndIndicesBlobs] without having to read anything.
        private boolean requiresProcessing() {
            assert snapshotIds.isEmpty() == false; // we could further optimize this case if needed, but it never happens
            final var indexSnapshotIdIterator = indexSnapshotIds.iterator();
            if (indexSnapshotIdIterator.hasNext()) {
                final var deletingFirstSnapshot = snapshotIds.contains(indexSnapshotIdIterator.next());
                while (indexSnapshotIdIterator.hasNext()) {
                    if (snapshotIds.contains(indexSnapshotIdIterator.next()) != deletingFirstSnapshot) {
                        return true;
                    }
                }
            }
            return false;
        }

        private void deleteUnreferencedMetaBlobs(Set<String> affectedMetaBlobIds, Set<String> survivingMetaBlobIds) {
            // TODO also list existing index metadata blobs here to clean up leaks
            final var unreferencedMetaBlobIds = new HashSet<>(affectedMetaBlobIds);
            unreferencedMetaBlobIds.removeAll(survivingMetaBlobIds);
            if (unreferencedMetaBlobIds.isEmpty() == false) {
                blobsToDelete.addIndexDeleteResult(indexId, unreferencedMetaBlobIds);
            }
        }
    }

    private class IndexSnapshotsDeletion {
        private final IndexId indexId;
        private final Set<SnapshotId> survivingSnapshots;
        private final BlobContainer indexContainer;

        IndexSnapshotsDeletion(IndexId indexId, Set<SnapshotId> survivingSnapshots) {
            this.indexId = indexId;
            this.survivingSnapshots = survivingSnapshots;
            this.indexContainer = repository.indexContainer(indexId);
        }

        // Computed by reading the relevant index metadata, which happens-before this value is used to determine the shards to process.
        private int shardCount;

        private synchronized void updateShardCount(int newShardCount) {
            shardCount = Math.max(shardCount, newShardCount);
        }

        void run(Set<String> affectedMetaBlobIds, ActionListener<Void> listener) {
            determineShardCount(affectedMetaBlobIds, listener.delegateFailureAndWrap((l, v) -> processShards(l)));
        }

        // -----------------------------------------------------------------------------------------------------------------------------
        // Determining the shard count by reading all the relevant index metadata

        private void determineShardCount(Set<String> affectedMetaBlobIds, ActionListener<Void> listener) {
            try (var listeners = new RefCountingListener(listener)) {
                // Only need to check the affected meta blobs to determine the number of shards to be processed: if there are snapshots
                // of an index with the same name and a higher shard count, and none of those snapshots are being deleted, then those
                // higher-numbered shards need no processing.
                for (final var metaBlobId : affectedMetaBlobIds) {
                    // NB since 7.9.0 we deduplicate index metadata blobs, and one of the components of the deduplication key is the
                    // index UUID; the shard count is going to be the same for all metadata with the same index UUID, so it is
                    // unnecessary to read multiple metadata blobs corresponding to the same index UUID.
                    // TODO Skip this unnecessary work? Maybe track the shard count in RepositoryData?
                    snapshotExecutor.execute(ActionRunnable.run(listeners.acquire(), () -> getOneShardCount(metaBlobId)));
                }
            }
        }

        private void getOneShardCount(String metaBlobId) {
            try {
                updateShardCount(
                    BlobStoreRepository.INDEX_SHARD_COUNT_FORMAT.read(
                        repository.getProjectRepo(),
                        indexContainer,
                        metaBlobId,
                        namedXContentRegistry
                    ).count()
                );
            } catch (Exception ex) {
                logger.warn(() -> format("[%s] [%s] failed to read shard count for index", metaBlobId, indexId.getName()), ex);
                // Definitely indicates something fairly badly wrong with the repo, but not immediately fatal here: we might get the
                // shard count from another metadata blob, or we might just not process these shards. If we skip these shards
                // then the repository will technically enter an invalid state (these shards' index-XXX blobs will refer to snapshots
                // that no longer exist) and may contain dangling blobs too. A subsequent delete that hits this index may repair
                // the state if the metadata read error is transient, but if not then the stale indices cleanup will eventually
                // remove this index and all its extra data anyway.
                // TODO: Should we fail the delete here? See https://github.com/elastic/elasticsearch/issues/100569.
            }
        }

        private void processShards(ActionListener<Void> listener) {
            try (var listeners = new RefCountingListener(listener)) {
                for (int shardId = 0; shardId < shardCount; shardId++) {
                    snapshotExecutor.execute(new IndexSnapshotsDeletion.ShardSnapshotsDeletion(shardId, listeners.acquire()));
                }
            }
        }

        // -----------------------------------------------------------------------------------------------------------------------------
        // Updating each shard

        private class ShardSnapshotsDeletion extends AbstractRunnable {

            private final int shardId;
            private final ActionListener<Void> listener;

            // Computed at the start of doRun(), after forking, rather than in the constructor. TODO make these local variables perhaps?
            private BlobContainer shardContainer;
            private Set<String> originalShardBlobs;

            ShardSnapshotsDeletion(int shardId, ActionListener<Void> listener) {
                this.shardId = shardId;
                this.listener = listener;
            }

            @Override
            protected void doRun() throws Exception {
                shardContainer = repository.shardContainer(indexId, shardId);
                originalShardBlobs = shardContainer.listBlobs(OperationPurpose.SNAPSHOT_DATA).keySet();
                final BlobStoreIndexShardSnapshots blobStoreIndexShardSnapshots;
                final long newGen;
                if (useShardGenerations) {
                    newGen = -1L;
                    blobStoreIndexShardSnapshots = repository.buildBlobStoreIndexShardSnapshots(
                        indexId,
                        shardId,
                        originalShardBlobs,
                        shardContainer,
                        originalRepositoryData.shardGenerations().getShardGen(indexId, shardId)
                    ).v1();
                } else {
                    Tuple<BlobStoreIndexShardSnapshots, Long> tuple = repository.buildBlobStoreIndexShardSnapshots(
                        originalShardBlobs,
                        shardContainer
                    );
                    newGen = tuple.v2() + 1;
                    blobStoreIndexShardSnapshots = tuple.v1();
                }
                deleteFromShardSnapshotMeta(blobStoreIndexShardSnapshots.withRetainedSnapshots(survivingSnapshots), newGen);
            }

            /**
             * Delete snapshot from shard level metadata.
             *
             * @param indexGeneration generation to write the new shard level level metadata to. If negative a uuid id shard generation
             *                        should be used
             */
            private void deleteFromShardSnapshotMeta(BlobStoreIndexShardSnapshots updatedSnapshots, long indexGeneration) {
                ShardGeneration writtenGeneration = null;
                try {
                    if (updatedSnapshots.snapshots().isEmpty()) {
                        blobsToDelete.addShardDeleteResult(indexId, shardId, ShardGenerations.DELETED_SHARD_GEN, originalShardBlobs);
                    } else {
                        if (indexGeneration < 0L) {
                            writtenGeneration = ShardGeneration.newGeneration();
                            BlobStoreRepository.INDEX_SHARD_SNAPSHOTS_FORMAT.write(
                                updatedSnapshots,
                                shardContainer,
                                writtenGeneration.getGenerationUUID(),
                                repository.isCompress()
                            );
                        } else {
                            writtenGeneration = new ShardGeneration(indexGeneration);
                            repository.writeShardIndexBlobAtomic(shardContainer, indexGeneration, updatedSnapshots, Collections.emptyMap());
                        }
                        final Set<String> survivingSnapshotUUIDs = survivingSnapshots.stream()
                            .map(SnapshotId::getUUID)
                            .collect(Collectors.toSet());
                        blobsToDelete.addShardDeleteResult(
                            indexId,
                            shardId,
                            writtenGeneration,
                            unusedBlobs(originalShardBlobs, survivingSnapshotUUIDs, updatedSnapshots)
                        );
                    }
                } catch (IOException e) {
                    throw new RepositoryException(
                        repository.getMetadata().name(),
                        "Failed to finalize snapshot deletion "
                            + snapshotIds
                            + " with shard index ["
                            + BlobStoreRepository.INDEX_SHARD_SNAPSHOTS_FORMAT.blobName(writtenGeneration.getGenerationUUID())
                            + "]",
                        e
                    );
                }
            }

            // Unused blobs are all previous index-, data- and meta-blobs and that are not referenced by the new index- as well as all
            // temporary blobs
            private static List<String> unusedBlobs(
                Set<String> originalShardBlobs,
                Set<String> survivingSnapshotUUIDs,
                BlobStoreIndexShardSnapshots updatedSnapshots
            ) {
                return originalShardBlobs.stream()
                    .filter(
                        blob -> blob.startsWith(BlobStoreRepository.SNAPSHOT_INDEX_PREFIX)
                            || (blob.startsWith(BlobStoreRepository.SNAPSHOT_PREFIX)
                                && blob.endsWith(BlobStoreRepository.METADATA_BLOB_NAME_SUFFIX)
                                && survivingSnapshotUUIDs.contains(
                                    blob.substring(
                                        BlobStoreRepository.SNAPSHOT_PREFIX.length(),
                                        blob.length() - BlobStoreRepository.METADATA_BLOB_NAME_SUFFIX.length()
                                    )
                                ) == false)
                            || (blob.startsWith(BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX)
                                && updatedSnapshots.findNameFile(canonicalName(blob)) == null)
                            || FsBlobContainer.isTempBlobName(blob)
                    )
                    .toList();
            }

            @Override
            public void onFailure(Exception ex) {
                // TODO: Should we fail the delete here? See https://github.com/elastic/elasticsearch/issues/100569.
                logger.warn(() -> format("%s failed to delete shard data for shard [%s][%s]", snapshotIds, indexId.getName(), shardId), ex);
            }

            @Override
            public void onAfter() {
                listener.onResponse(null);
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------------------------------------
    // Updating the root RepositoryData blob

    private void updateRepositoryData(RepositoryData newRepositoryData, ActionListener<RepositoryData> listener) {
        repository.writeIndexGen(
            newRepositoryData,
            originalRepositoryDataGeneration,
            repositoryFormatIndexVersion,
            Function.identity(),
            listener
        );
    }

    // ---------------------------------------------------------------------------------------------------------------------------------
    // Cleaning up dangling blobs

    private void cleanupUnlinkedShardLevelBlobs(ActionListener<Void> listener) {
        final Iterator<String> blobPathsToDelete = blobsToDelete.getBlobPaths();
        if (blobPathsToDelete.hasNext() == false) {
            listener.onResponse(null);
            return;
        }
        snapshotExecutor.execute(ActionRunnable.wrap(listener, l -> {
            try {
                repository.deleteFromContainer(OperationPurpose.SNAPSHOT_DATA, repository.blobContainer(), blobPathsToDelete);
                l.onResponse(null);
            } catch (Exception e) {
                logger.warn(() -> format("%s Failed to delete some blobs during snapshot delete", snapshotIds), e);
                throw e;
            }
        }));
    }

    /**
     * Cleans up stale blobs directly under the repository root as well as all indices paths that aren't referenced by any existing
     * snapshots. This method is only to be called directly after a new {@link RepositoryData} was written to the repository.
     *
     * @param newRepositoryData new repository data that was just written
     * @param listener          listener to invoke when the deletion is complete
     */
    private void cleanupUnlinkedRootAndIndicesBlobs(RepositoryData newRepositoryData, ActionListener<Void> listener) {
        try (var listeners = new RefCountingListener(listener)) {
            final List<String> staleRootBlobs = staleRootBlobs(newRepositoryData, originalRootBlobs.keySet());
            if (staleRootBlobs.isEmpty() == false) {
                staleBlobDeleteRunner.enqueueTask(listeners.acquire(ref -> {
                    try (ref) {
                        logStaleRootLevelBlobs(newRepositoryData.getGenId() - 1, snapshotIds, staleRootBlobs);
                        repository.deleteFromContainer(
                            OperationPurpose.SNAPSHOT_METADATA,
                            repository.blobContainer(),
                            staleRootBlobs.iterator()
                        );
                        for (final var staleRootBlob : staleRootBlobs) {
                            bytesDeleted.addAndGet(originalRootBlobs.get(staleRootBlob).length());
                        }
                        blobsDeleted.addAndGet(staleRootBlobs.size());
                    } catch (Exception e) {
                        logger.warn(
                            () -> format(
                                "%s The following blobs are no longer part of any snapshot [%s] but failed to remove them",
                                repository.toStringShort(),
                                staleRootBlobs
                            ),
                            e
                        );
                    }
                }));
            }

            final var survivingIndexIds = newRepositoryData.getIndices().values().stream().map(IndexId::getId).collect(Collectors.toSet());
            for (final var indexEntry : originalIndexContainers.entrySet()) {
                final var indexId = indexEntry.getKey();
                if (survivingIndexIds.contains(indexId)) {
                    continue;
                }
                staleBlobDeleteRunner.enqueueTask(listeners.acquire(ref -> {
                    try (ref) {
                        logger.debug("[{}] Found stale index [{}]. Cleaning it up", repository.getMetadata().name(), indexId);
                        final var deleteResult = indexEntry.getValue().delete(OperationPurpose.SNAPSHOT_DATA);
                        blobsDeleted.addAndGet(deleteResult.blobsDeleted());
                        bytesDeleted.addAndGet(deleteResult.bytesDeleted());
                        logger.debug("[{}] Cleaned up stale index [{}]", repository.getMetadata().name(), indexId);
                    } catch (IOException e) {
                        logger.warn(() -> format("""
                            %s index %s is no longer part of any snapshot in the repository, \
                            but failed to clean up its index folder""", repository.toStringShort(), indexId), e);
                    }
                }));
            }
        }

        // If we did the cleanup of stale indices purely using a throttled executor then there would be no backpressure to prevent us
        // from falling arbitrarily far behind. But nor do we want to dedicate all the SNAPSHOT threads to stale index cleanups because
        // that would slow down other snapshot operations in situations that do not need backpressure.
        //
        // The solution is to dedicate one SNAPSHOT thread to doing the cleanups eagerly, alongside the throttled executor which spreads
        // the rest of the work across the other threads if they are free. If the eager cleanup loop doesn't finish before the next one
        // starts then we dedicate another SNAPSHOT thread to the deletions, and so on, until eventually either we catch up or the
        // SNAPSHOT pool is fully occupied with blob deletions, which pushes back on other snapshot operations.

        staleBlobDeleteRunner.runSyncTasksEagerly(snapshotExecutor);
    }

    // Finds all blobs directly under the repository root path that are not referenced by the current RepositoryData
    private static List<String> staleRootBlobs(RepositoryData newRepositoryData, Set<String> originalRootBlobNames) {
        final Set<String> allSnapshotIds = newRepositoryData.getSnapshotIds().stream().map(SnapshotId::getUUID).collect(Collectors.toSet());
        return originalRootBlobNames.stream().filter(blob -> {
            if (FsBlobContainer.isTempBlobName(blob)) {
                return true;
            }
            if (blob.endsWith(BlobStoreRepository.METADATA_BLOB_NAME_SUFFIX)) {
                final String foundUUID;
                if (blob.startsWith(BlobStoreRepository.SNAPSHOT_PREFIX)) {
                    foundUUID = blob.substring(
                        BlobStoreRepository.SNAPSHOT_PREFIX.length(),
                        blob.length() - BlobStoreRepository.METADATA_BLOB_NAME_SUFFIX.length()
                    );
                    assert BlobStoreRepository.SNAPSHOT_FORMAT.blobName(foundUUID).equals(blob);
                } else if (blob.startsWith(BlobStoreRepository.METADATA_PREFIX)) {
                    foundUUID = blob.substring(
                        BlobStoreRepository.METADATA_PREFIX.length(),
                        blob.length() - BlobStoreRepository.METADATA_BLOB_NAME_SUFFIX.length()
                    );
                    assert BlobStoreRepository.GLOBAL_METADATA_FORMAT.blobName(foundUUID).equals(blob);
                } else {
                    return false;
                }
                return allSnapshotIds.contains(foundUUID) == false;
            } else if (blob.startsWith(BlobStoreRepository.INDEX_FILE_PREFIX)) {
                // TODO: Include the current generation here once we remove keeping index-(N-1) around from #writeIndexGen
                try {
                    return newRepositoryData.getGenId() > Long.parseLong(blob.substring(BlobStoreRepository.INDEX_FILE_PREFIX.length()));
                } catch (NumberFormatException nfe) {
                    // odd case of an extra file with the index- prefix that we can't identify
                    return false;
                }
            }
            return false;
        }).toList();
    }

    private void logStaleRootLevelBlobs(long newestStaleRepositoryDataGeneration, Set<SnapshotId> snapshotIds, List<String> blobsToDelete) {
        if (logger.isInfoEnabled()) {
            // If we're running root level cleanup as part of a snapshot delete we should not log the snapshot- and global metadata
            // blobs associated with the just deleted snapshots as they are expected to exist and not stale. Otherwise every snapshot
            // delete would also log a confusing INFO message about "stale blobs".
            final Set<String> blobNamesToIgnore = snapshotIds.stream()
                .flatMap(
                    snapshotId -> Stream.of(
                        BlobStoreRepository.GLOBAL_METADATA_FORMAT.blobName(snapshotId.getUUID()),
                        BlobStoreRepository.SNAPSHOT_FORMAT.blobName(snapshotId.getUUID()),
                        BlobStoreRepository.getRepositoryDataBlobName(newestStaleRepositoryDataGeneration)
                    )
                )
                .collect(Collectors.toSet());
            final List<String> blobsToLog = blobsToDelete.stream().filter(b -> blobNamesToIgnore.contains(b) == false).toList();
            if (blobsToLog.isEmpty() == false) {
                logger.info("{} Found stale root level blobs {}. Cleaning them up", repository.toStringShort(), blobsToLog);
            }
        }
    }
}
