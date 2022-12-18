/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.blobstore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.repositories.integrity.VerifyRepositoryIntegrityAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ThrottledIterator;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.elasticsearch.core.Strings.format;

class MetadataVerifier implements Releasable {
    private static final Logger logger = LogManager.getLogger(MetadataVerifier.class);

    private final BlobStoreRepository blobStoreRepository;
    private final Client client;
    private final ActionListener<List<RepositoryVerificationException>> finalListener;
    private final RefCounted finalRefs = AbstractRefCounted.of(this::onCompletion);
    private final String repositoryName;
    private final VerifyRepositoryIntegrityAction.Request verifyRequest;
    private final RepositoryData repositoryData;
    private final BooleanSupplier isCancelledSupplier;
    private final Queue<RepositoryVerificationException> failures = ConcurrentCollections.newQueue();
    private final AtomicLong failureCount = new AtomicLong();
    private final Map<String, SnapshotDescription> snapshotDescriptionsById = ConcurrentCollections.newConcurrentMap();
    private final Map<String, Set<SnapshotId>> snapshotsByIndex;
    private final Semaphore threadPoolPermits;
    private final Queue<AbstractRunnable> executorQueue = ConcurrentCollections.newQueue();
    private final ProgressLogger snapshotProgressLogger;
    private final ProgressLogger indexProgressLogger;
    private final ProgressLogger indexSnapshotProgressLogger;
    private final Set<String> requestedIndices;

    MetadataVerifier(
        BlobStoreRepository blobStoreRepository,
        Client client,
        VerifyRepositoryIntegrityAction.Request verifyRequest,
        RepositoryData repositoryData,
        BooleanSupplier isCancelledSupplier,
        ActionListener<List<RepositoryVerificationException>> finalListener
    ) {
        this.blobStoreRepository = blobStoreRepository;
        this.repositoryName = blobStoreRepository.metadata.name();
        this.client = client;
        this.verifyRequest = verifyRequest;
        this.repositoryData = repositoryData;
        this.isCancelledSupplier = isCancelledSupplier;
        this.finalListener = finalListener;
        this.snapshotsByIndex = this.repositoryData.getIndices()
            .values()
            .stream()
            .collect(Collectors.toMap(IndexId::getName, indexId -> Set.copyOf(this.repositoryData.getSnapshots(indexId))));

        this.threadPoolPermits = new Semaphore(Math.max(1, verifyRequest.getThreadpoolConcurrency()));
        this.snapshotProgressLogger = new ProgressLogger("snapshots", repositoryData.getSnapshotIds().size(), 100);
        this.indexProgressLogger = new ProgressLogger("indices", repositoryData.getIndices().size(), 20);
        this.indexSnapshotProgressLogger = new ProgressLogger("index snapshots", repositoryData.getIndexSnapshotCount(), 1000);

        this.requestedIndices = Set.of(verifyRequest.getIndices());
    }

    @Override
    public void close() {
        finalRefs.decRef();
    }

    private void addFailure(String format, Object... args) {
        final var failureNumber = failureCount.incrementAndGet();
        final var failure = format(format, args);
        logger.debug("[{}] found metadata verification failure [{}]: {}", repositoryName, failureNumber, failure);
        if (failureNumber <= verifyRequest.getMaxFailures()) {
            failures.add(new RepositoryVerificationException(repositoryName, failure));
        }
    }

    private void addFailure(Exception exception) {
        if (isCancelledSupplier.getAsBoolean() && exception instanceof TaskCancelledException) {
            return;
        }
        final var failureNumber = failureCount.incrementAndGet();
        logger.debug(() -> format("[%s] exception [%d] during metadata verification", repositoryName, failureNumber), exception);
        if (failureNumber <= verifyRequest.getMaxFailures()) {
            failures.add(
                exception instanceof RepositoryVerificationException rve
                    ? rve
                    : new RepositoryVerificationException(repositoryName, "exception during metadata verification", exception)
            );
        }
    }

    private static final String RESULTS_INDEX = "metadata_verification_results";

    public void run() {
        logger.info(
            "[{}] verifying metadata integrity for index generation [{}]: "
                + "repo UUID [{}], cluster UUID [{}], snapshots [{}], indices [{}], index snapshots [{}]",
            repositoryName,
            repositoryData.getGenId(),
            repositoryData.getUuid(),
            repositoryData.getClusterUUID(),
            snapshotProgressLogger.getExpectedMax(),
            indexProgressLogger.getExpectedMax(),
            indexSnapshotProgressLogger.getExpectedMax()
        );

        client.admin().indices().prepareCreate(RESULTS_INDEX).execute(makeListener(finalRefs, createIndexResponse -> verifySnapshots()));
    }

    private void verifySnapshots() {
        runThrottled(
            repositoryData.getSnapshotIds().iterator(),
            this::verifySnapshot,
            verifyRequest.getSnapshotVerificationConcurrency(),
            snapshotProgressLogger,
            wrapRunnable(finalRefs, this::verifyIndices)
        );
    }

    private void verifySnapshot(RefCounted snapshotRefs, SnapshotId snapshotId) {
        if (isCancelledSupplier.getAsBoolean()) {
            // getSnapshotInfo does its own forking so we must check for cancellation here
            return;
        }

        blobStoreRepository.getSnapshotInfo(snapshotId, makeListener(snapshotRefs, snapshotInfo -> {
            final var snapshotDescription = new SnapshotDescription(snapshotId, snapshotInfo.startTime(), snapshotInfo.endTime());
            snapshotDescriptionsById.put(snapshotId.getUUID(), snapshotDescription);
            forkSupply(snapshotRefs, () -> getSnapshotGlobalMetadata(snapshotRefs, snapshotDescription), metadata -> {
                // no checks here, loading it is enough
            });
        }));
    }

    private Metadata getSnapshotGlobalMetadata(RefCounted snapshotRefs, SnapshotDescription snapshotDescription) {
        try {
            return blobStoreRepository.getSnapshotGlobalMetadata(snapshotDescription.snapshotId());
        } catch (Exception e) {
            addResult(snapshotRefs, (builder, params) -> {
                snapshotDescription.writeXContent(builder);
                builder.field("failure", "failed to get snapshot global metadata");
                ElasticsearchException.generateFailureXContent(builder, params, e, true);
                return builder;
            });
            return null;
        }
    }

    private void verifyIndices() {
        runThrottled(
            repositoryData.getIndices().values().iterator(),
            (refCounted, indexId) -> new IndexVerifier(refCounted, indexId).run(),
            verifyRequest.getIndexVerificationConcurrency(),
            indexProgressLogger,
            wrapRunnable(
                finalRefs,
                () -> client.admin()
                    .indices()
                    .prepareFlush(RESULTS_INDEX)
                    .execute(
                        makeListener(
                            finalRefs,
                            ignored1 -> client.admin()
                                .indices()
                                .prepareRefresh(RESULTS_INDEX)
                                .execute(makeListener(finalRefs, ignored2 -> {}))
                        )
                    )
            )
        );
    }

    private record ShardContainerContents(
        Map<String, BlobMetadata> blobsByName,
        BlobStoreIndexShardSnapshots blobStoreIndexShardSnapshots
    ) {}

    private class IndexVerifier {
        private final RefCounted indexRefs;
        private final IndexId indexId;
        private final Set<SnapshotId> expectedSnapshots;
        private final Map<Integer, ListenableActionFuture<ShardContainerContents>> shardContainerContentsListener = newConcurrentMap();
        private final Map<String, ListenableActionFuture<IndexDescription>> indexDescriptionListenersByBlobId = newConcurrentMap();
        private final AtomicInteger totalSnapshotCounter = new AtomicInteger();
        private final AtomicInteger restorableSnapshotCounter = new AtomicInteger();

        IndexVerifier(RefCounted indexRefs, IndexId indexId) {
            this.indexRefs = indexRefs;
            this.indexId = indexId;
            this.expectedSnapshots = snapshotsByIndex.get(indexId.getName());
        }

        void run() {
            if (requestedIndices.isEmpty() == false && requestedIndices.contains(indexId.getName()) == false) {
                return;
            }

            runThrottled(
                repositoryData.getSnapshots(indexId).iterator(),
                this::verifyIndexSnapshot,
                verifyRequest.getIndexSnapshotVerificationConcurrency(),
                indexSnapshotProgressLogger,
                wrapRunnable(indexRefs, () -> logRestorability(totalSnapshotCounter.get(), restorableSnapshotCounter.get()))
            );
        }

        private void logRestorability(int totalSnapshotCount, int restorableSnapshotCount) {
            if (isCancelledSupplier.getAsBoolean() == false) {
                addResult(indexRefs, (builder, params) -> {
                    new IndexDescription(indexId.getId(), indexId.getName(), 0).writeXContent(builder);
                    builder.field(
                        "restorability",
                        totalSnapshotCount == restorableSnapshotCount ? "full" : 0 < restorableSnapshotCount ? "partial" : "none"
                    );
                    builder.field("snapshots", totalSnapshotCount);
                    builder.field("restorable_snapshots", restorableSnapshotCount);
                    builder.field("unrestorable_snapshots", totalSnapshotCount - restorableSnapshotCount);
                    return builder;
                });
            }
        }

        private void verifyIndexSnapshot(RefCounted indexSnapshotRefs, SnapshotId snapshotId) {
            totalSnapshotCounter.incrementAndGet();

            final var snapshotDescription = snapshotDescriptionsById.get(snapshotId.getUUID());
            if (snapshotDescription == null) {
                addResult(indexSnapshotRefs, (builder, params) -> {
                    new IndexDescription(indexId.getId(), indexId.getName(), 0).writeXContent(builder);
                    new SnapshotDescription(snapshotId, 0, 0).writeXContent(builder);
                    builder.field("failure", "unknown snapshot for index");
                    return builder;
                });
                return;
            }

            final var indexMetaBlobId = repositoryData.indexMetaDataGenerations().indexMetaBlobId(snapshotId, indexId);
            indexDescriptionListenersByBlobId.computeIfAbsent(indexMetaBlobId, ignored -> {
                final var indexDescriptionFuture = new ListenableActionFuture<IndexDescription>();
                forkSupply(() -> {
                    final var shardCount = getNumberOfShards(indexMetaBlobId, snapshotId);
                    final var indexDescription = new IndexDescription(indexId.getId(), indexId.getName(), shardCount);
                    for (int i = 0; i < shardCount; i++) {
                        shardContainerContentsListener.computeIfAbsent(i, shardId -> {
                            final var shardContainerContentsFuture = new ListenableActionFuture<ShardContainerContents>();
                            forkSupply(
                                () -> new ShardContainerContents(
                                    blobStoreRepository.shardContainer(indexId, shardId).listBlobs(),
                                    getBlobStoreIndexShardSnapshots(indexDescription, shardId)
                                ),
                                shardContainerContentsFuture
                            );
                            return shardContainerContentsFuture;
                        });
                    }
                    return indexDescription;
                }, indexDescriptionFuture);
                return indexDescriptionFuture;
            }).addListener(makeListener(indexSnapshotRefs, indexDescription -> {
                final var restorableShardCount = new AtomicInteger();
                final var shardSnapshotsRefs = AbstractRefCounted.of(wrapRunnable(indexSnapshotRefs, () -> {
                    if (indexDescription.shardCount() > 0 && indexDescription.shardCount() == restorableShardCount.get()) {
                        restorableSnapshotCounter.incrementAndGet();
                    }
                }));
                try {
                    for (int i = 0; i < indexDescription.shardCount(); i++) {
                        final var shardId = i;
                        shardContainerContentsListener.get(i)
                            .addListener(
                                makeListener(
                                    shardSnapshotsRefs,
                                    shardContainerContents -> forkSupply(
                                        shardSnapshotsRefs,
                                        () -> getBlobStoreIndexShardSnapshot(
                                            shardSnapshotsRefs,
                                            snapshotDescription,
                                            indexDescription,
                                            shardId
                                        ),
                                        shardSnapshot -> verifyShardSnapshot(
                                            shardSnapshotsRefs,
                                            indexDescription,
                                            snapshotDescription,
                                            shardId,
                                            shardContainerContents,
                                            shardSnapshot,
                                            restorableShardCount::incrementAndGet
                                        )
                                    )
                                )
                            );
                    }
                } finally {
                    shardSnapshotsRefs.decRef();
                }
            }));
        }

        private BlobStoreIndexShardSnapshot getBlobStoreIndexShardSnapshot(
            RefCounted shardSnapshotRefs,
            SnapshotDescription snapshotDescription,
            IndexDescription indexDescription,
            int shardId
        ) {
            try {
                return blobStoreRepository.loadShardSnapshot(
                    blobStoreRepository.shardContainer(indexId, shardId),
                    snapshotDescription.snapshotId()
                );
            } catch (Exception e) {
                addResult(shardSnapshotRefs, (builder, params) -> {
                    snapshotDescription.writeXContent(builder);
                    indexDescription.writeXContent(builder);
                    builder.field("shard", shardId);
                    builder.field("failure", "could not load shard snapshot");
                    ElasticsearchException.generateFailureXContent(builder, params, e, true);
                    return builder;
                });
                return null;
            }
        }

        private List<SnapshotId> getSnapshotsWithIndexMetadataBlob(String indexMetaBlobId) {
            final var indexMetaDataGenerations = repositoryData.indexMetaDataGenerations();
            return repositoryData.getSnapshotIds()
                .stream()
                .filter(s -> indexMetaBlobId.equals(indexMetaDataGenerations.indexMetaBlobId(s, indexId)))
                .toList();
        }

        private int getNumberOfShards(String indexMetaBlobId, SnapshotId snapshotId) {
            try {
                return blobStoreRepository.getSnapshotIndexMetaData(repositoryData, snapshotId, indexId).getNumberOfShards();
            } catch (Exception e) {
                addResult(indexRefs, (builder, params) -> {
                    new IndexDescription(indexId.getId(), indexId.getName(), 0).writeXContent(builder);
                    builder.field("failure", "could not load index metadata");
                    builder.field("metadata_blob", indexMetaBlobId);
                    ElasticsearchException.generateFailureXContent(builder, params, e, true);
                    return builder;
                });
                return 0;
            }
        }

        private BlobStoreIndexShardSnapshots getBlobStoreIndexShardSnapshots(IndexDescription indexDescription, int shardId) {
            final var shardGen = repositoryData.shardGenerations().getShardGen(indexId, shardId);
            if (shardGen == null) {
                addResult(indexRefs, (builder, params) -> {
                    indexDescription.writeXContent(builder);
                    builder.field("shard", shardId);
                    builder.field("failure", "shard generation not defined");
                    return builder;
                });
                return null;
            }
            try {
                return blobStoreRepository.getBlobStoreIndexShardSnapshots(indexId, shardId, shardGen);
            } catch (Exception e) {
                addResult(indexRefs, (builder, params) -> {
                    indexDescription.writeXContent(builder);
                    builder.field("shard", shardId);
                    builder.field("failure", "could not load shard generation");
                    ElasticsearchException.generateFailureXContent(builder, params, e, true);
                    return builder;
                });
                return null;
            }
        }

        private void verifyShardSnapshot(
            RefCounted shardSnapshotRefs,
            IndexDescription indexDescription,
            SnapshotDescription snapshotDescription,
            int shardId,
            ShardContainerContents shardContainerContents,
            BlobStoreIndexShardSnapshot shardSnapshot,
            Runnable runIfRestorable
        ) {
            if (shardSnapshot == null) {
                return;
            }

            if (shardSnapshot.snapshot().equals(snapshotDescription.snapshotId().getName()) == false) {
                addResult(shardSnapshotRefs, ((builder, params) -> {
                    snapshotDescription.writeXContent(builder);
                    indexDescription.writeXContent(builder);
                    builder.field("shard", shardId);
                    builder.field("error", "mismatched snapshot name");
                    builder.field("shard_snapshot_name", shardSnapshot.snapshot());
                    return builder;
                }));
            }

            var restorable = true;
            for (final var fileInfo : shardSnapshot.indexFiles()) {
                restorable &= verifyFileInfo(
                    shardSnapshotRefs,
                    snapshotDescription,
                    indexDescription,
                    shardId,
                    shardContainerContents.blobsByName(),
                    fileInfo
                );
            }
            if (restorable) {
                runIfRestorable.run();
            }

            final var blobStoreIndexShardSnapshots = shardContainerContents.blobStoreIndexShardSnapshots();
            if (blobStoreIndexShardSnapshots != null) {
                boolean foundSnapshot = false;
                for (SnapshotFiles summary : blobStoreIndexShardSnapshots.snapshots()) {
                    if (summary.snapshot().equals(snapshotDescription.snapshotId().getName())) {
                        foundSnapshot = true;
                        verifyConsistentShardFiles(snapshotDescription.snapshotId(), shardId, shardSnapshot, summary);
                        break;
                    }
                }

                if (foundSnapshot == false) {
                    addResult(shardSnapshotRefs, ((builder, params) -> {
                        snapshotDescription.writeXContent(builder);
                        indexDescription.writeXContent(builder);
                        builder.field("shard", shardId);
                        builder.field("error", "missing in shard-level summary");
                        return builder;
                    }));
                }
            }
        }

        private void verifyConsistentShardFiles(
            SnapshotId snapshotId,
            int shardId,
            BlobStoreIndexShardSnapshot shardSnapshot,
            SnapshotFiles summary
        ) {
            final var snapshotFiles = shardSnapshot.indexFiles()
                .stream()
                .collect(Collectors.toMap(BlobStoreIndexShardSnapshot.FileInfo::physicalName, Function.identity()));

            for (final var summaryFile : summary.indexFiles()) {
                final var snapshotFile = snapshotFiles.get(summaryFile.physicalName());
                if (snapshotFile == null) {
                    addFailure(
                        "snapshot [%s] for shard %s[%d] has no entry for file [%s] found in summary",
                        snapshotId,
                        indexId,
                        shardId,
                        summaryFile.physicalName()
                    );
                } else if (summaryFile.isSame(snapshotFile) == false) {
                    addFailure(
                        "snapshot [%s] for shard %s[%d] has a mismatched entry for file [%s]",
                        snapshotId,
                        indexId,
                        shardId,
                        summaryFile.physicalName()
                    );
                }
            }

            final var summaryFiles = summary.indexFiles()
                .stream()
                .collect(Collectors.toMap(BlobStoreIndexShardSnapshot.FileInfo::physicalName, Function.identity()));
            for (final var snapshotFile : shardSnapshot.indexFiles()) {
                if (summaryFiles.get(snapshotFile.physicalName()) == null) {
                    addFailure(
                        "snapshot [%s] for shard %s[%d] has no entry in the shard-level summary for file [%s]",
                        snapshotId,
                        indexId,
                        shardId,
                        snapshotFile.physicalName()
                    );
                }
            }
        }

        private boolean verifyFileInfo(
            RefCounted shardSnapshotRefs,
            SnapshotDescription snapshotDescription,
            IndexDescription indexDescription,
            int shardId,
            Map<String, BlobMetadata> shardBlobs,
            BlobStoreIndexShardSnapshot.FileInfo fileInfo
        ) {
            final var fileLength = ByteSizeValue.ofBytes(fileInfo.length());
            if (fileInfo.metadata().hashEqualsContents()) {
                final var actualLength = ByteSizeValue.ofBytes(fileInfo.metadata().hash().length);
                if (fileLength.getBytes() != actualLength.getBytes()) {
                    addResult(shardSnapshotRefs, ((builder, params) -> {
                        snapshotDescription.writeXContent(builder);
                        indexDescription.writeXContent(builder);
                        builder.field("shard", shardId);
                        builder.field("error", "mismatched virtual blob length");
                        builder.humanReadableField("actual_length_in_bytes", "actual_length", actualLength);
                        builder.humanReadableField("expected_length_in_bytes", "expected_length", fileLength);
                        return builder;
                    }));
                    return false;
                }
            } else {
                for (int part = 0; part < fileInfo.numberOfParts(); part++) {
                    final var finalPart = part;
                    final var blobName = fileInfo.partName(part);
                    final var blobInfo = shardBlobs.get(blobName);
                    final var partLength = ByteSizeValue.ofBytes(fileInfo.partBytes(part));
                    if (blobInfo == null) {
                        addResult(shardSnapshotRefs, ((builder, params) -> {
                            snapshotDescription.writeXContent(builder);
                            indexDescription.writeXContent(builder);
                            builder.field("shard", shardId);
                            builder.field("error", "missing blob");
                            builder.field("blob_name", blobName);
                            builder.field("file_name", fileInfo.physicalName());
                            builder.field("part", finalPart);
                            builder.field("number_of_parts", fileInfo.numberOfParts());
                            builder.humanReadableField("file_length_in_bytes", "file_length", fileLength);
                            builder.humanReadableField("part_length_in_bytes", "part_length", partLength);
                            return builder;
                        }));
                        return false;
                    } else if (blobInfo.length() != partLength.getBytes()) {
                        addResult(shardSnapshotRefs, ((builder, params) -> {
                            snapshotDescription.writeXContent(builder);
                            indexDescription.writeXContent(builder);
                            builder.field("shard", shardId);
                            builder.field("error", "unexpected blob length");
                            builder.field("blob_name", blobName);
                            builder.field("file_name", fileInfo.physicalName());
                            builder.field("part", finalPart);
                            builder.field("number_of_parts", fileInfo.numberOfParts());
                            builder.humanReadableField("file_length_in_bytes", "file_length", fileLength);
                            builder.humanReadableField("part_length_in_bytes", "part_length", partLength);
                            builder.humanReadableField("actual_length_in_bytes", "actual_length", ByteSizeValue.ofBytes(blobInfo.length()));
                            return builder;
                        }));
                        return false;
                    }
                }
            }
            return true;
        }
    }

    private <T> ActionListener<T> makeListener(RefCounted refCounted, CheckedConsumer<T, Exception> consumer) {
        refCounted.incRef();
        return ActionListener.runAfter(
            ActionListener.wrap(consumer, exception -> addExceptionResult(refCounted, exception)),
            refCounted::decRef
        );
    }

    private void addExceptionResult(RefCounted refCounted, Exception exception) {
        if (isCancelledSupplier.getAsBoolean() && exception instanceof TaskCancelledException) {
            return;
        }
        addResult(refCounted, (builder, params) -> {
            ElasticsearchException.generateFailureXContent(builder, params, exception, true);
            return builder;
        });
    }

    private Runnable wrapRunnable(RefCounted refCounted, Runnable runnable) {
        refCounted.incRef();
        return () -> {
            try {
                runnable.run();
            } finally {
                refCounted.decRef();
            }
        };
    }

    private <T> void forkSupply(CheckedSupplier<T, Exception> supplier, ActionListener<T> listener) {
        fork(ActionRunnable.supply(listener, supplier));
    }

    private void fork(AbstractRunnable runnable) {
        executorQueue.add(runnable);
        tryProcessQueue();
    }

    private void tryProcessQueue() {
        while (threadPoolPermits.tryAcquire()) {
            final var runnable = executorQueue.poll();
            if (runnable == null) {
                threadPoolPermits.release();
                return;
            }

            if (isCancelledSupplier.getAsBoolean()) {
                try {
                    runnable.onFailure(new TaskCancelledException("task cancelled"));
                    continue;
                } finally {
                    threadPoolPermits.release();
                }
            }

            blobStoreRepository.threadPool().executor(ThreadPool.Names.SNAPSHOT_META).execute(new AbstractRunnable() {
                @Override
                public void onRejection(Exception e) {
                    try {
                        runnable.onRejection(e);
                    } finally {
                        threadPoolPermits.release();
                        // no need to call tryProcessQueue() again here, we're still running it
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        runnable.onFailure(e);
                    } finally {
                        onCompletion();
                    }
                }

                @Override
                protected void doRun() {
                    runnable.run();
                    onCompletion();
                }

                @Override
                public String toString() {
                    return runnable.toString();
                }

                private void onCompletion() {
                    threadPoolPermits.release();
                    tryProcessQueue();
                }
            });
        }
    }

    private <T> void forkSupply(RefCounted refCounted, CheckedSupplier<T, Exception> supplier, CheckedConsumer<T, Exception> consumer) {
        forkSupply(supplier, makeListener(refCounted, consumer));
    }

    private void onCompletion() {
        final var finalFailureCount = failureCount.get();
        if (finalFailureCount > verifyRequest.getMaxFailures()) {
            failures.add(
                new RepositoryVerificationException(
                    repositoryName,
                    format(
                        "found %d verification failures in total, %d suppressed",
                        finalFailureCount,
                        finalFailureCount - verifyRequest.getMaxFailures()
                    )
                )
            );
        }

        if (isCancelledSupplier.getAsBoolean()) {
            failures.add(new RepositoryVerificationException(repositoryName, "verification task cancelled before completion"));
        }

        finalListener.onResponse(failures.stream().toList());
    }

    private static <T> void runThrottled(
        Iterator<T> iterator,
        BiConsumer<RefCounted, T> itemConsumer,
        int maxConcurrency,
        ProgressLogger progressLogger,
        Runnable onCompletion
    ) {
        ThrottledIterator.run(iterator, itemConsumer, maxConcurrency, progressLogger::maybeLogProgress, onCompletion);
    }

    private class ProgressLogger {
        private final String type;
        private final long expectedMax;
        private final long logFrequency;
        private final AtomicLong currentCount = new AtomicLong();

        ProgressLogger(String type, long expectedMax, long logFrequency) {
            this.type = type;
            this.expectedMax = expectedMax;
            this.logFrequency = logFrequency;
        }

        long getExpectedMax() {
            return expectedMax;
        }

        void maybeLogProgress() {
            final var count = currentCount.incrementAndGet();
            if (count == expectedMax || count % logFrequency == 0 && isCancelledSupplier.getAsBoolean() == false) {
                logger.info("[{}] processed [{}] of [{}] {}", repositoryName, count, expectedMax, type);
            }
        }
    }

    private final Queue<Tuple<IndexRequest, Runnable>> pendingResults = ConcurrentCollections.newQueue();
    private final Semaphore resultsIndexingSemaphore = new Semaphore(1);

    private void indexResultDoc(IndexRequest indexRequest, Runnable onCompletion) {
        if (indexRequest == null) {
            onCompletion.run();
            return;
        }
        pendingResults.add(Tuple.tuple(indexRequest, onCompletion));
        processPendingResults();
    }

    private void processPendingResults() {
        while (resultsIndexingSemaphore.tryAcquire()) {
            final var bulkRequest = new BulkRequest();
            final var completionActions = new ArrayList<Runnable>();

            Tuple<IndexRequest, Runnable> nextItem;
            while ((nextItem = pendingResults.poll()) != null) {
                bulkRequest.add(nextItem.v1());
                completionActions.add(nextItem.v2());
            }

            if (completionActions.isEmpty()) {
                resultsIndexingSemaphore.release();
                return;
            }

            final var isRecursing = new AtomicBoolean(true);
            client.bulk(bulkRequest, ActionListener.<BulkResponse>wrap(() -> {
                resultsIndexingSemaphore.release();
                for (final var completionAction : completionActions) {
                    completionAction.run();
                }
                if (isRecursing.get() == false) {
                    processPendingResults();
                }
            }).delegateResponse((l, e) -> {
                logger.error("error indexing results", e);
                l.onFailure(e);
            }));
            isRecursing.set(false);
        }
    }

    private IndexRequest buildResultDoc(ToXContent toXContent) {
        try (var builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("@timestamp", blobStoreRepository.threadPool().absoluteTimeInMillis());
            builder.field("repository", repositoryName);
            builder.field("uuid", repositoryData.getUuid());
            builder.field("repository_generation", repositoryData.getGenId());
            toXContent.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            return new IndexRequestBuilder(client, IndexAction.INSTANCE, RESULTS_INDEX).setSource(builder).request();
        } catch (Exception e) {
            logger.error("error generating failure output", e);
            return null;
        }
    }

    private void addResult(RefCounted refCounted, ToXContent toXContent) {
        refCounted.incRef();
        indexResultDoc(buildResultDoc(toXContent), refCounted::decRef);
    }

    private record SnapshotDescription(SnapshotId snapshotId, long startTimeMillis, long endTimeMillis) {
        void writeXContent(XContentBuilder builder) throws IOException {
            builder.startObject("snapshot");
            builder.field("id", snapshotId.getUUID());
            builder.field("name", snapshotId.getName());
            builder.field("start_time_millis", startTimeMillis);
            builder.field("end_time_millis", startTimeMillis);
            builder.endObject();
        }
    }

    private record IndexDescription(String indexId, String indexName, int shardCount) {
        void writeXContent(XContentBuilder builder) throws IOException {
            builder.startObject("index");
            builder.field("id", indexId);
            builder.field("name", indexName);
            builder.field("shards", shardCount);
            builder.endObject();
        }
    }

}
