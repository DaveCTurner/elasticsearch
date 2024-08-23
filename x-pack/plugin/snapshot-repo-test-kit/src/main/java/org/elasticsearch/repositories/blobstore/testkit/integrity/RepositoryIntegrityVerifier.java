/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ThrottledIterator;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.index.snapshots.blobstore.RateLimitingInputStream;
import org.elasticsearch.index.snapshots.blobstore.SlicedInputStream;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

public class RepositoryIntegrityVerifier {
    private static final Logger logger = LogManager.getLogger(RepositoryIntegrityVerifier.class);

    private final BlobStoreRepository blobStoreRepository;
    private final RepositoryVerifyIntegrityResponseChunk.Writer responseChunkWriter;
    private final String repositoryName;
    private final RepositoryVerifyIntegrityParams requestParams;
    private final RepositoryData repositoryData;
    private final BooleanSupplier isCancelledSupplier;
    private final CancellableRunner metadataTaskRunner;
    private final CancellableRunner snapshotTaskRunner;
    private final RateLimiter rateLimiter;

    private final Set<String> unreadableSnapshotInfoUuids = ConcurrentCollections.newConcurrentSet();
    private final long snapshotCount;
    private final AtomicLong snapshotProgress = new AtomicLong();
    private final long indexCount;
    private final AtomicLong indexProgress = new AtomicLong();
    private final long indexSnapshotCount;
    private final AtomicLong indexSnapshotProgress = new AtomicLong();
    private final AtomicLong blobsVerified = new AtomicLong();
    private final AtomicLong blobBytesVerified = new AtomicLong();
    private final AtomicLong throttledNanos;

    RepositoryIntegrityVerifier(
        BlobStoreRepository blobStoreRepository,
        RepositoryVerifyIntegrityResponseChunk.Writer responseChunkWriter,
        RepositoryVerifyIntegrityParams requestParams,
        RepositoryData repositoryData,
        CancellableThreads cancellableThreads
    ) {
        this.blobStoreRepository = blobStoreRepository;
        this.repositoryName = blobStoreRepository.getMetadata().name();
        this.responseChunkWriter = responseChunkWriter;
        this.requestParams = requestParams;
        this.repositoryData = repositoryData;
        this.isCancelledSupplier = cancellableThreads::isCancelled;
        this.snapshotTaskRunner = new CancellableRunner(
            new ThrottledTaskRunner(
                "verify-blob",
                requestParams.blobThreadPoolConcurrency(),
                blobStoreRepository.threadPool().executor(ThreadPool.Names.SNAPSHOT)
            ),
            cancellableThreads
        );
        this.metadataTaskRunner = new CancellableRunner(
            new ThrottledTaskRunner(
                "verify-metadata",
                requestParams.metaThreadPoolConcurrency(),
                blobStoreRepository.threadPool().executor(ThreadPool.Names.SNAPSHOT_META)
            ),
            cancellableThreads
        );

        this.snapshotCount = repositoryData.getSnapshotIds().size();
        this.indexCount = repositoryData.getIndices().size();
        this.indexSnapshotCount = repositoryData.getIndexSnapshotCount();
        this.rateLimiter = new RateLimiter.SimpleRateLimiter(requestParams.maxBytesPerSec().getMbFrac());

        this.throttledNanos = new AtomicLong(requestParams.verifyBlobContents() ? 1 : 0); // nonzero if verifying so status reported
    }

    RepositoryVerifyIntegrityTask.Status getStatus() {
        return new RepositoryVerifyIntegrityTask.Status(
            repositoryName,
            repositoryData.getGenId(),
            repositoryData.getUuid(),
            snapshotCount,
            snapshotProgress.get(),
            indexCount,
            indexProgress.get(),
            indexSnapshotCount,
            indexSnapshotProgress.get(),
            blobsVerified.get(),
            blobBytesVerified.get(),
            throttledNanos.get()
        );
    }

    void start(ActionListener<RepositoryVerifyIntegrityResponse> listener) {
        logger.info(
            """
                [{}] verifying metadata integrity for index generation [{}]: \
                repo UUID [{}], cluster UUID [{}], snapshots [{}], indices [{}], index snapshots [{}]""",
            repositoryName,
            repositoryData.getGenId(),
            repositoryData.getUuid(),
            repositoryData.getClusterUUID(),
            getSnapshotCount(),
            getIndexCount(),
            getIndexSnapshotCount()
        );

        SubscribableListener
            // first verify the top-level properties of the snapshots
            .newForked(this::verifySnapshots)
            // then verify the restorability of each index
            .andThen(this::verifyIndices)
            .andThenAccept(v -> this.ensureNotCancelled())
            // see if the repository data has changed
            .<RepositoryData>andThen(
                l -> blobStoreRepository.getRepositoryData(blobStoreRepository.threadPool().executor(ThreadPool.Names.MANAGEMENT), l)
            )
            // log the completion and return the result
            .addListener(new ActionListener<>() {
                @Override
                public void onResponse(RepositoryData finalRepositoryData) {
                    logger.info(
                        "[{}] completed verifying metadata integrity for index generation [{}]: repo UUID [{}], cluster UUID [{}]",
                        repositoryName,
                        repositoryData.getGenId(),
                        repositoryData.getUuid(),
                        repositoryData.getClusterUUID()
                    );
                    listener.onResponse(new RepositoryVerifyIntegrityResponse(repositoryData.getGenId(), finalRepositoryData.getGenId()));
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn(
                        () -> Strings.format(
                            "[%s] failed verifying metadata integrity for index generation [%d]: repo UUID [%s], cluster UUID [%s]",
                            repositoryName,
                            repositoryData.getGenId(),
                            repositoryData.getUuid(),
                            repositoryData.getClusterUUID()
                        ),
                        e
                    );
                    listener.onFailure(e);
                }
            });
    }

    private void ensureNotCancelled() {
        if (isCancelledSupplier.getAsBoolean()) {
            throw new TaskCancelledException("task cancelled");
        }
    }

    private void verifySnapshots(ActionListener<Void> listener) {
        var listeners = new RefCountingListener(listener);
        runThrottled(
            Iterators.failFast(
                repositoryData.getSnapshotIds().iterator(),
                () -> isCancelledSupplier.getAsBoolean() || listeners.isFailing()
            ),
            (releasable, snapshotId) -> verifySnapshot(
                snapshotId,
                ActionListener.assertOnce(ActionListener.releaseAfter(listeners.acquire(), releasable))
            ),
            requestParams.snapshotVerificationConcurrency(),
            snapshotProgress,
            listeners
        );
    }

    private void verifySnapshot(SnapshotId snapshotId, ActionListener<Void> listener) {
        if (isCancelledSupplier.getAsBoolean()) {
            // getSnapshotInfo does its own forking so we must check for cancellation here
            listener.onResponse(null);
            return;
        }

        blobStoreRepository.getSnapshotInfo(snapshotId, new ActionListener<>() {
            @Override
            public void onResponse(SnapshotInfo snapshotInfo) {
                final var chunkBuilder = new RepositoryVerifyIntegrityResponseChunk.Builder(
                    responseChunkWriter,
                    RepositoryVerifyIntegrityResponseChunk.Type.SNAPSHOT_INFO
                ).snapshotInfo(snapshotInfo);

                // record the SnapshotInfo in the response
                final var chunkWrittenStep = SubscribableListener.<Void>newForked(chunkBuilder::write);

                // check the global metadata is readable if present
                final var globalMetadataOkStep = Boolean.TRUE.equals(snapshotInfo.includeGlobalState())
                    ? chunkWrittenStep.<Void>andThen(l -> verifySnapshotGlobalMetadata(snapshotId, l))
                    : chunkWrittenStep;

                globalMetadataOkStep.addListener(listener);
            }

            @Override
            public void onFailure(Exception e) {
                unreadableSnapshotInfoUuids.add(snapshotId.getUUID());
                anomaly("failed to load snapshot info").snapshotId(snapshotId).exception(e).write(listener);
            }
        });
    }

    private void verifySnapshotGlobalMetadata(SnapshotId snapshotId, ActionListener<Void> listener) {
        metadataTaskRunner.run(ActionRunnable.wrap(listener, l -> {
            try {
                blobStoreRepository.getSnapshotGlobalMetadata(snapshotId);
                // no checks here, loading it is enough
                l.onResponse(null);
            } catch (Exception e) {
                anomaly("failed to load global metadata").snapshotId(snapshotId).exception(e).write(l);
            }
        }));
    }

    private void verifyIndices(ActionListener<Void> listener) {
        var listeners = new RefCountingListener(listener);
        runThrottled(
            Iterators.failFast(
                repositoryData.getIndices().values().iterator(),
                () -> isCancelledSupplier.getAsBoolean() || listeners.isFailing()
            ),
            (releasable, indexId) -> new IndexVerifier(indexId).run(ActionListener.releaseAfter(listeners.acquire(), releasable)),
            requestParams.indexVerificationConcurrency(),
            indexProgress,
            listeners
        );
    }

    private class IndexVerifier {
        private final IndexId indexId;
        private final Map<Integer, SubscribableListener<ShardContainerContents>> shardContainerContentsListener = newConcurrentMap();
        private final Map<String, SubscribableListener<IndexDescription>> indexDescriptionListenersByBlobId = newConcurrentMap();
        private final AtomicInteger totalSnapshotCounter = new AtomicInteger();
        private final AtomicInteger restorableSnapshotCounter = new AtomicInteger();

        IndexVerifier(IndexId indexId) {
            this.indexId = indexId;
        }

        private record ShardContainerContents(
            int shardId,
            Map<String, BlobMetadata> blobsByName,
            // shard gen contents
            @Nullable // if shard gen blob could not be read
            BlobStoreIndexShardSnapshots blobStoreIndexShardSnapshots,
            Map<String, SubscribableListener<Void>> blobContentsListeners
        ) {}

        void run(ActionListener<Void> listener) {
            SubscribableListener

                .<Void>newForked(l -> {
                    var listeners = new RefCountingListener(1, l);
                    runThrottled(
                        Iterators.failFast(
                            repositoryData.getSnapshots(indexId).iterator(),
                            () -> isCancelledSupplier.getAsBoolean() || listeners.isFailing()
                        ),
                        (releasable, snapshotId) -> verifyIndexSnapshot(
                            snapshotId,
                            ActionListener.releaseAfter(listeners.acquire(), releasable)
                        ),
                        requestParams.indexSnapshotVerificationConcurrency(),
                        indexSnapshotProgress,
                        listeners
                    );
                })
                .<Void>andThen(l -> {
                    ensureNotCancelled();
                    int totalSnapshotCount = totalSnapshotCounter.get();
                    int restorableSnapshotCount = restorableSnapshotCounter.get();
                    logger.info("--> reporting restorability of [{}]: [{}] vs [{}]", indexId, totalSnapshotCount, restorableSnapshotCount);
                    new RepositoryVerifyIntegrityResponseChunk.Builder(
                        responseChunkWriter,
                        RepositoryVerifyIntegrityResponseChunk.Type.INDEX_RESTORABILITY
                    ).indexRestorability(indexId, totalSnapshotCount, restorableSnapshotCount).write(l);
                })
                .addListener(listener);
        }

        private void verifyIndexSnapshot(SnapshotId snapshotId, ActionListener<Void> listener) {
            totalSnapshotCounter.incrementAndGet();
            final var indexMetaBlobId = repositoryData.indexMetaDataGenerations().indexMetaBlobId(snapshotId, indexId);
            logger.info("--> snapshot [{}] index [{}] metadata blob [{}]", snapshotId, indexId, indexMetaBlobId);
            indexDescriptionListeners(snapshotId, indexMetaBlobId)
                // deduplicating reads of index metadata blobs
                .<Void>andThen((l, indexDescription) -> {
                    if (indexDescription == null) {
                        // index metadata was unreadable; anomaly already reported, skip further verification of this index snapshot
                        l.onResponse(null);
                    } else {
                        verifyShardSnapshots(snapshotId, indexDescription, l);
                    }
                })
                .addListener(listener);
        }

        private void verifyShardSnapshots(SnapshotId snapshotId, IndexDescription indexDescription, ActionListener<Void> listener) {
            final var restorableShardCount = new AtomicInteger();
            try (var listeners = new RefCountingListener(1, listener.map(v -> {
                if (unreadableSnapshotInfoUuids.contains(snapshotId.getUUID()) == false
                    && indexDescription.shardCount() == restorableShardCount.get()) {
                    logger.info(
                        "--> snapshot [{}] index [{}] all [{}] shards restorable",
                        snapshotId,
                        indexDescription,
                        indexDescription.shardCount()
                    );
                    restorableSnapshotCounter.incrementAndGet();
                }
                return v;
            }))) {
                for (int shardId = 0; shardId < indexDescription.shardCount(); shardId++) {
                    shardContainerContentsListeners(indexDescription, shardId)
                        // deduplicating reads of shard container contents
                        .<Void>andThen((l, shardContainerContents) -> {
                            if (shardContainerContents == null) {
                                // shard container contents was unreadable; anomaly already reported, skip further verification
                                l.onResponse(null);
                            } else {
                                metadataTaskRunner.run(
                                    ActionRunnable.wrap(
                                        l,
                                        ll -> verifyShardSnapshot(
                                            snapshotId,
                                            indexDescription,
                                            shardContainerContents,
                                            restorableShardCount,
                                            ll
                                        )
                                    )
                                );
                            }
                        })
                        .addListener(listeners.acquire());
                }
            }
        }

        private void verifyShardSnapshot(
            SnapshotId snapshotId,
            IndexDescription indexDescription,
            ShardContainerContents shardContainerContents,
            AtomicInteger restorableShardCount,
            ActionListener<Void> listener
        ) {
            final var shardId = shardContainerContents.shardId();
            final BlobStoreIndexShardSnapshot blobStoreIndexShardSnapshot;
            try {
                blobStoreIndexShardSnapshot = blobStoreRepository.loadShardSnapshot(
                    blobStoreRepository.shardContainer(indexId, shardId),
                    snapshotId
                );
            } catch (Exception e) {
                anomaly("failed to load shard snapshot").snapshotId(snapshotId)
                    .shardDescription(indexDescription, shardId)
                    .exception(e)
                    .write(listener);
                return;
            }

            final var restorable = new AtomicBoolean(true);
            final var listeners = new RefCountingListener(1, listener.map(v -> {
                if (restorable.get()) {
                    logger.info(
                        "--> snapshot [{}] index [{}] shard [{}] restorable",
                        snapshotId,
                        indexDescription,
                        shardContainerContents.shardId()
                    );
                    restorableShardCount.incrementAndGet();
                }
                return v;
            }));
            final var startedListener = listeners.acquire();

            runThrottled(
                Iterators.failFast(
                    blobStoreIndexShardSnapshot.indexFiles().iterator(),
                    () -> isCancelledSupplier.getAsBoolean() || listeners.isFailing()
                ),
                (releasable, fileInfo) -> verifyFileInfo(
                    snapshotId,
                    indexDescription,
                    shardContainerContents,
                    restorable,
                    fileInfo,
                    ActionListener.releaseAfter(listeners.acquire(), releasable)
                ),
                1,
                blobsVerified,
                listeners
            );

            verifyConsistentShardFiles(snapshotId, indexDescription, shardContainerContents, blobStoreIndexShardSnapshot, listeners);
            startedListener.onResponse(null);
        }

        private void verifyConsistentShardFiles(
            SnapshotId snapshotId,
            IndexDescription indexDescription,
            ShardContainerContents shardContainerContents,
            BlobStoreIndexShardSnapshot blobStoreIndexShardSnapshot,
            RefCountingListener listeners
        ) {
            final var blobStoreIndexShardSnapshots = shardContainerContents.blobStoreIndexShardSnapshots();
            if (blobStoreIndexShardSnapshots == null) {
                // already reported
                return;
            }

            final var shardId = shardContainerContents.shardId();
            for (SnapshotFiles summary : blobStoreIndexShardSnapshots.snapshots()) {
                if (summary.snapshot().equals(snapshotId.getName()) == false) {
                    continue;
                }

                final var snapshotFiles = blobStoreIndexShardSnapshot.indexFiles()
                    .stream()
                    .collect(Collectors.toMap(BlobStoreIndexShardSnapshot.FileInfo::physicalName, Function.identity()));

                for (final var summaryFile : summary.indexFiles()) {
                    final var snapshotFile = snapshotFiles.get(summaryFile.physicalName());
                    if (snapshotFile == null) {
                        // TODO test needed
                        anomaly("blob in shard generation but not snapshot").snapshotId(snapshotId)
                            .shardDescription(indexDescription, shardId)
                            .physicalFileName(summaryFile.physicalName())
                            .write(listeners.acquire());
                    } else if (summaryFile.isSame(snapshotFile) == false) {
                        // TODO test needed
                        anomaly("snapshot shard generation mismatch").snapshotId(snapshotId)
                            .shardDescription(indexDescription, shardId)
                            .physicalFileName(summaryFile.physicalName())
                            .write(listeners.acquire());
                    }
                }

                final var summaryFiles = summary.indexFiles()
                    .stream()
                    .collect(Collectors.toMap(BlobStoreIndexShardSnapshot.FileInfo::physicalName, Function.identity()));
                for (final var snapshotFile : blobStoreIndexShardSnapshot.indexFiles()) {
                    if (summaryFiles.get(snapshotFile.physicalName()) == null) {
                        // TODO test needed
                        anomaly("blob in snapshot but not shard generation").snapshotId(snapshotId)
                            .shardDescription(indexDescription, shardId)
                            .physicalFileName(snapshotFile.physicalName())
                            .write(listeners.acquire());
                    }
                }

                return;
            }

            // TODO test needed
            anomaly("snapshot not in shard generation").snapshotId(snapshotId)
                .shardDescription(indexDescription, shardId)
                .write(listeners.acquire());
        }

        private void verifyFileInfo(
            SnapshotId snapshotId,
            IndexDescription indexDescription,
            ShardContainerContents shardContainerContents,
            AtomicBoolean restorable,
            BlobStoreIndexShardSnapshot.FileInfo fileInfo,
            ActionListener<Void> listener
        ) {
            if (fileInfo.metadata().hashEqualsContents()) {
                listener.onResponse(null);
                return;
            }

            final var shardId = shardContainerContents.shardId();
            final var shardBlobs = shardContainerContents.blobsByName();
            final var fileLength = ByteSizeValue.ofBytes(fileInfo.length());
            for (int partIndex = 0; partIndex < fileInfo.numberOfParts(); partIndex++) {
                final var blobName = fileInfo.partName(partIndex);
                final var blobInfo = shardBlobs.get(blobName);
                final var partLength = ByteSizeValue.ofBytes(fileInfo.partBytes(partIndex));
                if (blobInfo == null) {
                    restorable.set(false);
                    String physicalFileName = fileInfo.physicalName();
                    anomaly("missing blob").snapshotId(snapshotId)
                        .shardDescription(indexDescription, shardId)
                        .blobName(blobName, physicalFileName)
                        .part(partIndex, fileInfo.numberOfParts())
                        .fileLength(fileLength)
                        .partLength(partLength)
                        .write(listener);
                    return;
                } else if (blobInfo.length() != partLength.getBytes()) {
                    restorable.set(false);
                    String physicalFileName = fileInfo.physicalName();
                    ByteSizeValue blobLength = ByteSizeValue.ofBytes(blobInfo.length());
                    anomaly("mismatched blob length").snapshotId(snapshotId)
                        .shardDescription(indexDescription, shardId)
                        .blobName(blobName, physicalFileName)
                        .part(partIndex, fileInfo.numberOfParts())
                        .fileLength(fileLength)
                        .partLength(partLength)
                        .blobLength(blobLength)
                        .write(listener);
                    return;
                }
            }

            blobContentsListeners(indexDescription, shardContainerContents, fileInfo).addListener(listener.delegateResponse((l, e) -> {
                restorable.set(false);
                String physicalFileName = fileInfo.physicalName();
                anomaly("corrupt data blob").snapshotId(snapshotId)
                    .shardDescription(indexDescription, shardId)
                    .blobName(fileInfo.name(), physicalFileName)
                    .part(-1, fileInfo.numberOfParts())
                    .fileLength(fileLength)
                    .exception(e)
                    .write(l);
            }));
        }

        private SubscribableListener<IndexDescription> indexDescriptionListeners(SnapshotId snapshotId, String indexMetaBlobId) {
            return indexDescriptionListenersByBlobId.computeIfAbsent(
                indexMetaBlobId,
                ignored -> SubscribableListener.newForked(
                    indexDescriptionListener -> metadataTaskRunner.run(
                        ActionRunnable.wrap(indexDescriptionListener, l -> readIndexDescription(snapshotId, indexMetaBlobId, l))
                    )
                )
            );
        }

        private void readIndexDescription(SnapshotId snapshotId, String indexMetaBlobId, ActionListener<IndexDescription> listener) {
            try {
                listener.onResponse(
                    new IndexDescription(
                        indexId,
                        indexMetaBlobId,
                        blobStoreRepository.getSnapshotIndexMetaData(repositoryData, snapshotId, indexId).getNumberOfShards()
                    )
                );
            } catch (Exception e) {
                anomaly("failed to load index metadata").indexDescription(new IndexDescription(indexId, indexMetaBlobId, 0))
                    .exception(e)
                    .write(listener.map(v -> null));
            }
        }

        private SubscribableListener<ShardContainerContents> shardContainerContentsListeners(
            IndexDescription indexDescription,
            int shardId
        ) {
            return shardContainerContentsListener.computeIfAbsent(
                shardId,
                ignored -> SubscribableListener.newForked(
                    shardContainerContentsListener -> metadataTaskRunner.run(
                        ActionRunnable.wrap(shardContainerContentsListener, l -> loadShardContainerContents(indexDescription, shardId, l))
                    )
                )
            );
        }

        private void loadShardContainerContents(
            IndexDescription indexDescription,
            int shardId,
            ActionListener<ShardContainerContents> listener
        ) {
            final Map<String, BlobMetadata> blobsByName;
            try {
                blobsByName = blobStoreRepository.shardContainer(indexId, shardId).listBlobs(OperationPurpose.REPOSITORY_ANALYSIS);
            } catch (Exception e) {
                // TODO test needed
                anomaly("failed to list shard container contents").shardDescription(indexDescription, shardId)
                    .exception(e)
                    .write(listener.map(v -> null));
                return;
            }

            final var shardGen = repositoryData.shardGenerations().getShardGen(indexId, shardId);
            if (shardGen == null) {
                // TODO test needed
                anomaly("shard generation not defined").shardDescription(indexDescription, shardId).write(listener.map(v -> null));
                return;
            }

            SubscribableListener
                // try and load the shard gen blob
                .<BlobStoreIndexShardSnapshots>newForked(l -> {
                    try {
                        l.onResponse(blobStoreRepository.getBlobStoreIndexShardSnapshots(indexId, shardId, shardGen));
                    } catch (Exception e) {
                        // failing here is not fatal to snapshot restores, only to creating/deleting snapshots, so we can return null
                        // and carry on with the analysis
                        anomaly("failed to load shard generation").shardDescription(indexDescription, shardId)
                            .shardGeneration(shardGen)
                            .exception(e)
                            .write(l.map(v -> null));
                    }
                })
                .andThenApply(
                    blobStoreIndexShardSnapshots -> new ShardContainerContents(
                        shardId,
                        blobsByName,
                        blobStoreIndexShardSnapshots,
                        ConcurrentCollections.newConcurrentMap()
                    )
                )
                .addListener(listener);
        }

        private SubscribableListener<Void> blobContentsListeners(
            IndexDescription indexDescription,
            ShardContainerContents shardContainerContents,
            BlobStoreIndexShardSnapshot.FileInfo fileInfo
        ) {
            return shardContainerContents.blobContentsListeners().computeIfAbsent(fileInfo.name(), ignored -> {
                if (requestParams.verifyBlobContents()) {
                    return SubscribableListener.newForked(listener -> {
                        // TODO do this on a remote node?
                        snapshotTaskRunner.run(ActionRunnable.run(listener, () -> {
                            try (var slicedStream = new SlicedInputStream(fileInfo.numberOfParts()) {
                                @Override
                                protected InputStream openSlice(int slice) throws IOException {
                                    return blobStoreRepository.shardContainer(indexDescription.indexId(), shardContainerContents.shardId())
                                        .readBlob(OperationPurpose.REPOSITORY_ANALYSIS, fileInfo.partName(slice));
                                }
                            };
                                var rateLimitedStream = new RateLimitingInputStream(
                                    slicedStream,
                                    () -> rateLimiter,
                                    throttledNanos::addAndGet
                                );
                                var indexInput = new IndexInputWrapper(rateLimitedStream, fileInfo.length())
                            ) {
                                CodecUtil.checksumEntireFile(indexInput);
                            }
                        }));
                    });
                } else {
                    blobBytesVerified.addAndGet(fileInfo.length());
                    return SubscribableListener.newSucceeded(null);
                }
            });
        }
    }

    private static <T> void runThrottled(
        Iterator<T> iterator,
        BiConsumer<Releasable, T> itemConsumer,
        int maxConcurrency,
        AtomicLong progressCounter,
        Releasable onCompletion
    ) {
        ThrottledIterator.run(iterator, itemConsumer, maxConcurrency, progressCounter::incrementAndGet, onCompletion::close);
    }

    private RepositoryVerifyIntegrityResponseChunk.Builder anomaly(String anomaly) {
        return new RepositoryVerifyIntegrityResponseChunk.Builder(responseChunkWriter, RepositoryVerifyIntegrityResponseChunk.Type.ANOMALY)
            .anomaly(anomaly);
    }

    public long getSnapshotCount() {
        return snapshotCount;
    }

    public long getIndexCount() {
        return indexCount;
    }

    public long getIndexSnapshotCount() {
        return indexSnapshotCount;
    }

    private class IndexInputWrapper extends IndexInput {
        private final InputStream inputStream;
        private final long length;
        long filePointer = 0L;

        IndexInputWrapper(InputStream inputStream, long length) {
            super("");
            this.inputStream = inputStream;
            this.length = length;
        }

        @Override
        public byte readByte() throws IOException {
            if (isCancelledSupplier.getAsBoolean()) {
                throw new TaskCancelledException("task cancelled");
            }
            final var read = inputStream.read();
            if (read == -1) {
                throw new EOFException();
            }
            filePointer += 1;
            blobBytesVerified.incrementAndGet();
            return (byte) read;
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            while (len > 0) {
                if (isCancelledSupplier.getAsBoolean()) {
                    throw new TaskCancelledException("task cancelled");
                }
                final var read = inputStream.read(b, offset, len);
                if (read == -1) {
                    throw new EOFException();
                }
                filePointer += read;
                blobBytesVerified.addAndGet(read);
                len -= read;
                offset += read;
            }
        }

        @Override
        public void close() {}

        @Override
        public long getFilePointer() {
            return filePointer;
        }

        @Override
        public void seek(long pos) {
            if (filePointer != pos) {
                assert false : "cannot seek";
                throw new UnsupportedOperationException("seek");
            }
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) {
            assert false;
            throw new UnsupportedOperationException("slice");
        }
    }

    private static class CancellableRunner {
        private final ThrottledTaskRunner delegate;
        private final CancellableThreads cancellableThreads;

        CancellableRunner(ThrottledTaskRunner delegate, CancellableThreads cancellableThreads) {
            this.delegate = delegate;
            this.cancellableThreads = cancellableThreads;
        }

        void run(AbstractRunnable runnable) {
            delegate.enqueueTask(new ActionListener<>() {
                @Override
                public void onResponse(Releasable releasable) {
                    try (releasable) {
                        if (cancellableThreads.isCancelled()) {
                            runnable.onFailure(new TaskCancelledException("task cancelled"));
                        } else {
                            cancellableThreads.execute(runnable::run);
                        }
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    runnable.onFailure(e);
                }
            });
        }
    }
}
