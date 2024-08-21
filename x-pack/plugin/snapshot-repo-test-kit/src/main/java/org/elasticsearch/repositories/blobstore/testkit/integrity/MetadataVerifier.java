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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThrottledIterator;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.CheckedConsumer;
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
import org.elasticsearch.repositories.ShardGeneration;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

public class MetadataVerifier implements Releasable {
    private static final Logger logger = LogManager.getLogger(MetadataVerifier.class);

    public static void run(
        BlobStoreRepository blobStoreRepository,
        ResponseWriter responseWriter,
        RepositoryVerifyIntegrityParams verifyRequest,
        CancellableThreads cancellableThreads,
        RepositoryVerifyIntegrityTask task,
        ActionListener<VerificationResult> finalListener
    ) {
        logger.info("[{}] verifying metadata integrity", verifyRequest.repository());

        final var repositoryDataFuture = new SubscribableListener<RepositoryData>();
        blobStoreRepository.getRepositoryData(EsExecutors.DIRECT_EXECUTOR_SERVICE, repositoryDataFuture);

        repositoryDataFuture.addListener(finalListener.map(repositoryData -> {
            try (
                var metadataVerifier = new MetadataVerifier(
                    blobStoreRepository,
                    responseWriter,
                    verifyRequest,
                    repositoryData,
                    cancellableThreads,
                    task,
                    createLoggingListener(verifyRequest, finalListener, repositoryData)
                )
            ) {
                logger.info(
                    "[{}] verifying metadata integrity for index generation [{}]: "
                        + "repo UUID [{}], cluster UUID [{}], snapshots [{}], indices [{}], index snapshots [{}]",
                    verifyRequest.repository(),
                    repositoryData.getGenId(),
                    repositoryData.getUuid(),
                    repositoryData.getClusterUUID(),
                    metadataVerifier.getSnapshotCount(),
                    metadataVerifier.getIndexCount(),
                    metadataVerifier.getIndexSnapshotCount()
                );
                metadataVerifier.start();
                return null;
            }
        }));
    }

    private static ActionListener<VerificationResult> createLoggingListener(
        RepositoryVerifyIntegrityParams verifyRequest,
        ActionListener<VerificationResult> l,
        RepositoryData repositoryData
    ) {
        return new ActionListener<>() {
            @Override
            public void onResponse(VerificationResult verificationResult) {
                logger.info(
                    "[{}] completed verifying metadata integrity for index generation [{}]: "
                        + "repo UUID [{}], cluster UUID [{}], anomalies [{}]",
                    verifyRequest.repository(),
                    repositoryData.getGenId(),
                    repositoryData.getUuid(),
                    repositoryData.getClusterUUID(),
                    verificationResult.totalAnomalies()
                );
                l.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(
                    () -> Strings.format(
                        "[%s] failed verifying metadata integrity for index generation [%d]: repo UUID [%s], cluster UUID [%s]",
                        verifyRequest.repository(),
                        repositoryData.getGenId(),
                        repositoryData.getUuid(),
                        repositoryData.getClusterUUID()
                    )
                );
                l.onFailure(e);
            }
        };
    }

    private final BlobStoreRepository blobStoreRepository;
    private final ResponseWriter responseWriter;
    private final ActionListener<VerificationResult> finalListener;
    private final RefCountingRunnable finalRefs = new RefCountingRunnable(this::onCompletion);
    private final String repositoryName;
    private final RepositoryVerifyIntegrityParams verifyRequest;
    private final RepositoryData repositoryData;
    private final BooleanSupplier isCancelledSupplier;
    private final RepositoryVerifyIntegrityTask task;
    private final AtomicLong anomalyCount = new AtomicLong();
    private final Map<String, SnapshotDescription> snapshotDescriptionsById = ConcurrentCollections.newConcurrentMap();
    private final CancellableRunner metadataTaskRunner;
    private final CancellableRunner snapshotTaskRunner;
    private final RateLimiter rateLimiter;

    private final long snapshotCount;
    private final AtomicLong snapshotProgress = new AtomicLong();
    private final long indexCount;
    private final AtomicLong indexProgress = new AtomicLong();
    private final long indexSnapshotCount;
    private final AtomicLong indexSnapshotProgress = new AtomicLong();
    private final AtomicLong blobsVerified = new AtomicLong();
    private final AtomicLong blobBytesVerified = new AtomicLong();
    private final AtomicLong throttledNanos;

    MetadataVerifier(
        BlobStoreRepository blobStoreRepository,
        ResponseWriter responseWriter,
        RepositoryVerifyIntegrityParams verifyRequest,
        RepositoryData repositoryData,
        CancellableThreads cancellableThreads,
        RepositoryVerifyIntegrityTask task,
        ActionListener<VerificationResult> finalListener
    ) {
        this.blobStoreRepository = blobStoreRepository;
        this.repositoryName = blobStoreRepository.getMetadata().name();
        this.responseWriter = responseWriter;
        this.verifyRequest = verifyRequest;
        this.repositoryData = repositoryData;
        this.isCancelledSupplier = cancellableThreads::isCancelled;
        this.task = task;
        this.finalListener = finalListener;
        this.snapshotTaskRunner = new CancellableRunner(
            new ThrottledTaskRunner(
                "verify-blob",
                verifyRequest.blobThreadPoolConcurrency(),
                blobStoreRepository.threadPool().executor(ThreadPool.Names.SNAPSHOT)
            ),
            cancellableThreads
        );
        this.metadataTaskRunner = new CancellableRunner(
            new ThrottledTaskRunner(
                "verify-metadata",
                verifyRequest.metaThreadPoolConcurrency(),
                blobStoreRepository.threadPool().executor(ThreadPool.Names.SNAPSHOT_META)
            ),
            cancellableThreads
        );

        this.snapshotCount = repositoryData.getSnapshotIds().size();
        this.indexCount = repositoryData.getIndices().size();
        this.indexSnapshotCount = repositoryData.getIndexSnapshotCount();
        this.rateLimiter = new RateLimiter.SimpleRateLimiter(verifyRequest.maxBytesPerSec().getMbFrac());

        this.throttledNanos = new AtomicLong(verifyRequest.verifyBlobContents() ? 1 : 0); // nonzero if verifying so status reported
    }

    @Override
    public void close() {
        finalRefs.close();
    }

    private RepositoryVerifyIntegrityTask.Status getStatus() {
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
            throttledNanos.get(),
            anomalyCount.get()
        );
    }

    private void start() {
        task.setStatusSupplier(this::getStatus);
        verifySnapshots(this::verifyIndices);
    }

    private void verifySnapshots(Runnable onCompletion) {
        runThrottled(
            repositoryData.getSnapshotIds().iterator(),
            this::verifySnapshot,
            verifyRequest.snapshotVerificationConcurrency(),
            snapshotProgress,
            wrapRunnable(finalRefs.acquire(), onCompletion)
        );
    }

    private void verifySnapshot(Releasable releasable, SnapshotId snapshotId) {
        try (var snapshotRefs = new RefCountingRunnable(releasable::close)) {
            if (isCancelledSupplier.getAsBoolean()) {
                // getSnapshotInfo does its own forking so we must check for cancellation here
                return;
            }

            blobStoreRepository.getSnapshotInfo(snapshotId, ActionListener.releaseAfter(new ActionListener<>() {
                @Override
                public void onResponse(SnapshotInfo snapshotInfo) {
                    final var snapshotDescription = new SnapshotDescription(snapshotId, snapshotInfo.startTime(), snapshotInfo.endTime());
                    snapshotDescriptionsById.put(snapshotId.getUUID(), snapshotDescription);
                    metadataTaskRunner.run(ActionRunnable.run(snapshotRefs.acquireListener(), () -> {
                        try {
                            blobStoreRepository.getSnapshotGlobalMetadata(snapshotDescription.snapshotId());
                            // no checks here, loading it is enough
                        } catch (Exception e) {
                            anomalyCount.incrementAndGet();
                            responseWriter.writeResponseChunk(
                                new ResponseWriter.ResponseChunk(
                                    "failed to load global metadata",
                                    snapshotDescription,
                                    null,
                                    -1,
                                    null,
                                    null,
                                    null,
                                    -1,
                                    -1,
                                    ByteSizeValue.MINUS_ONE,
                                    ByteSizeValue.MINUS_ONE,
                                    ByteSizeValue.MINUS_ONE,
                                    -1,
                                    -1,
                                    e
                                ),
                                snapshotRefs.acquire()
                            );
                        }
                    }));
                }

                @Override
                public void onFailure(Exception e) {
                    anomalyCount.incrementAndGet();
                    responseWriter.writeResponseChunk(
                        new ResponseWriter.ResponseChunk(
                            "failed to load snapshot info",
                            new SnapshotDescription(snapshotId, 0, 0),
                            null,
                            -1,
                            null,
                            null,
                            null,
                            -1,
                            -1,
                            ByteSizeValue.MINUS_ONE,
                            ByteSizeValue.MINUS_ONE,
                            ByteSizeValue.MINUS_ONE,
                            -1,
                            -1,
                            e
                        ),
                        snapshotRefs.acquire()
                    );
                }
            }, snapshotRefs.acquire()));
        }
    }

    private void verifyIndices() {
        runThrottled(
            repositoryData.getIndices().values().iterator(),
            (releasable, indexId) -> new IndexVerifier(releasable, indexId).run(),
            verifyRequest.indexVerificationConcurrency(),
            indexProgress,
            wrapRunnable(finalRefs.acquire(), () -> {})
        );
    }

    private record ShardContainerContents(
        int shardId,
        Map<String, BlobMetadata> blobsByName,
        // shard gen contents
        @Nullable // if shard gen blob could not be read
        BlobStoreIndexShardSnapshots blobStoreIndexShardSnapshots,
        Map<String, SubscribableListener<Void>> blobContentsListeners
    ) {}

    private class IndexVerifier {
        private final RefCountingRunnable indexRefs;
        private final IndexId indexId;
        private final Map<Integer, SubscribableListener<ShardContainerContents>> shardContainerContentsListener = newConcurrentMap();
        private final Map<String, SubscribableListener<IndexDescription>> indexDescriptionListenersByBlobId = newConcurrentMap();
        private final AtomicInteger totalSnapshotCounter = new AtomicInteger();
        private final AtomicInteger restorableSnapshotCounter = new AtomicInteger();

        IndexVerifier(Releasable releasable, IndexId indexId) {
            this.indexRefs = new RefCountingRunnable(releasable::close);
            this.indexId = indexId;
        }

        void run() {
            runThrottled(
                repositoryData.getSnapshots(indexId).iterator(),
                this::verifyIndexSnapshot,
                verifyRequest.indexSnapshotVerificationConcurrency(),
                indexSnapshotProgress,
                wrapRunnable(indexRefs, () -> recordRestorability(totalSnapshotCounter.get(), restorableSnapshotCounter.get()))
            );
        }

        private void recordRestorability(int totalSnapshotCount, int restorableSnapshotCount) {
            if (isCancelledSupplier.getAsBoolean() == false) {
                responseWriter.writeResponseChunk(
                    new ResponseWriter.ResponseChunk(
                        null,
                        null,
                        new IndexDescription(indexId, null, 0),
                        -1,
                        null,
                        null,
                        null,
                        -1,
                        -1,
                        ByteSizeValue.MINUS_ONE,
                        ByteSizeValue.MINUS_ONE,
                        ByteSizeValue.MINUS_ONE,
                        totalSnapshotCount,
                        restorableSnapshotCount,
                        null
                    ),
                    indexRefs.acquire()
                );
            }
        }

        private void verifyIndexSnapshot(Releasable releasable, SnapshotId snapshotId) {
            try (var indexSnapshotRefs = new RefCountingRunnable(releasable::close)) {
                totalSnapshotCounter.incrementAndGet();

                final var snapshotDescription = snapshotDescriptionsById.get(snapshotId.getUUID());
                if (snapshotDescription == null) {
                    anomalyCount.incrementAndGet();
                    responseWriter.writeResponseChunk(
                        new ResponseWriter.ResponseChunk(
                            "unknown snapshot for index",
                            new SnapshotDescription(snapshotId, 0, 0),
                            new IndexDescription(indexId, null, 0),
                            -1,
                            null,
                            null,
                            null,
                            -1,
                            -1,
                            ByteSizeValue.MINUS_ONE,
                            ByteSizeValue.MINUS_ONE,
                            ByteSizeValue.MINUS_ONE,
                            -1,
                            -1,
                            null
                        ),
                        indexSnapshotRefs.acquire()
                    );
                    return;
                }

                final var indexMetaBlobId = repositoryData.indexMetaDataGenerations().indexMetaBlobId(snapshotId, indexId);
                indexDescriptionListeners(snapshotId, indexMetaBlobId).addListener(
                    makeListener(
                        indexSnapshotRefs.acquire(),
                        indexDescription -> verifyShardSnapshots(snapshotDescription, indexDescription, indexSnapshotRefs.acquire())
                    )
                );
            }
        }

        private void verifyShardSnapshots(
            SnapshotDescription snapshotDescription,
            IndexDescription indexDescription,
            Releasable releasable
        ) {
            final var restorableShardCount = new AtomicInteger();
            try (var shardSnapshotsRefs = new RefCountingRunnable(wrapRunnable(releasable, () -> {
                if (indexDescription.shardCount() == restorableShardCount.get()) {
                    restorableSnapshotCounter.incrementAndGet();
                }
            }))) {
                for (int shardId = 0; shardId < indexDescription.shardCount(); shardId++) {
                    shardContainerContentsListeners(indexDescription, shardId).addListener(
                        makeListener(
                            shardSnapshotsRefs.acquire(),
                            shardContainerContents -> metadataTaskRunner.run(
                                ActionRunnable.run(
                                    shardSnapshotsRefs.acquireListener(),
                                    () -> verifyShardSnapshot(
                                        snapshotDescription,
                                        indexDescription,
                                        shardContainerContents,
                                        restorableShardCount,
                                        shardSnapshotsRefs
                                    )
                                )
                            )
                        )
                    );
                }
            }
        }

        private void verifyShardSnapshot(
            SnapshotDescription snapshotDescription,
            IndexDescription indexDescription,
            ShardContainerContents shardContainerContents,
            AtomicInteger restorableShardCount,
            RefCountingRunnable shardSnapshotsRefs
        ) throws AnomalyException {
            final var shardId = shardContainerContents.shardId();
            final BlobStoreIndexShardSnapshot blobStoreIndexShardSnapshot;
            try {
                blobStoreIndexShardSnapshot = blobStoreRepository.loadShardSnapshot(
                    blobStoreRepository.shardContainer(indexId, shardId),
                    snapshotDescription.snapshotId()
                );
            } catch (Exception e) {
                anomalyCount.incrementAndGet();
                responseWriter.writeResponseChunk(
                    new ResponseWriter.ResponseChunk(
                        "failed to load shard snapshot",
                        snapshotDescription,
                        indexDescription,
                        shardId,
                        null,
                        null,
                        null,
                        -1,
                        -1,
                        ByteSizeValue.MINUS_ONE,
                        ByteSizeValue.MINUS_ONE,
                        ByteSizeValue.MINUS_ONE,
                        -1,
                        -1,
                        e
                    ),
                    shardSnapshotsRefs.acquire()
                );
                throw new AnomalyException(e);
            }

            final var restorable = new AtomicBoolean(true);
            runThrottled(
                blobStoreIndexShardSnapshot.indexFiles().iterator(),
                (releasable, fileInfo) -> verifyFileInfo(
                    releasable,
                    snapshotDescription,
                    indexDescription,
                    shardContainerContents,
                    restorable,
                    fileInfo
                ),
                1,
                blobsVerified,
                wrapRunnable(shardSnapshotsRefs.acquire(), () -> {
                    if (restorable.get()) {
                        restorableShardCount.incrementAndGet();
                    }
                })
            );

            verifyConsistentShardFiles(
                snapshotDescription,
                indexDescription,
                shardContainerContents,
                blobStoreIndexShardSnapshot,
                shardSnapshotsRefs
            );
        }

        private void verifyConsistentShardFiles(
            SnapshotDescription snapshotDescription,
            IndexDescription indexDescription,
            ShardContainerContents shardContainerContents,
            BlobStoreIndexShardSnapshot blobStoreIndexShardSnapshot,
            RefCountingRunnable shardSnapshotsRefs
        ) {
            final var blobStoreIndexShardSnapshots = shardContainerContents.blobStoreIndexShardSnapshots();
            if (blobStoreIndexShardSnapshots == null) {
                // already reported
                return;
            }

            final var shardId = shardContainerContents.shardId();
            for (SnapshotFiles summary : blobStoreIndexShardSnapshots.snapshots()) {
                if (summary.snapshot().equals(snapshotDescription.snapshotId().getName()) == false) {
                    continue;
                }

                final var snapshotFiles = blobStoreIndexShardSnapshot.indexFiles()
                    .stream()
                    .collect(Collectors.toMap(BlobStoreIndexShardSnapshot.FileInfo::physicalName, Function.identity()));

                for (final var summaryFile : summary.indexFiles()) {
                    final var snapshotFile = snapshotFiles.get(summaryFile.physicalName());
                    if (snapshotFile == null) {
                        anomalyCount.incrementAndGet();
                        responseWriter.writeResponseChunk(
                            new ResponseWriter.ResponseChunk(
                                "blob in shard generation but not snapshot",
                                snapshotDescription,
                                indexDescription,
                                shardId,
                                null,
                                null,
                                summaryFile.physicalName(),
                                -1,
                                -1,
                                ByteSizeValue.MINUS_ONE,
                                ByteSizeValue.MINUS_ONE,
                                ByteSizeValue.MINUS_ONE,
                                -1,
                                -1,
                                null
                            ),
                            shardSnapshotsRefs.acquire()
                        );
                    } else if (summaryFile.isSame(snapshotFile) == false) {
                        anomalyCount.incrementAndGet();
                        responseWriter.writeResponseChunk(
                            new ResponseWriter.ResponseChunk(
                                "snapshot shard generation mismatch",
                                snapshotDescription,
                                indexDescription,
                                shardId,
                                null,
                                null,
                                summaryFile.physicalName(),
                                -1,
                                -1,
                                ByteSizeValue.MINUS_ONE,
                                ByteSizeValue.MINUS_ONE,
                                ByteSizeValue.MINUS_ONE,
                                -1,
                                -1,
                                null
                            ),
                            shardSnapshotsRefs.acquire()
                        );
                    }
                }

                final var summaryFiles = summary.indexFiles()
                    .stream()
                    .collect(Collectors.toMap(BlobStoreIndexShardSnapshot.FileInfo::physicalName, Function.identity()));
                for (final var snapshotFile : blobStoreIndexShardSnapshot.indexFiles()) {
                    if (summaryFiles.get(snapshotFile.physicalName()) == null) {
                        anomalyCount.incrementAndGet();
                        responseWriter.writeResponseChunk(
                            new ResponseWriter.ResponseChunk(
                                "blob in snapshot but not shard generation",
                                snapshotDescription,
                                indexDescription,
                                shardId,
                                null,
                                null,
                                snapshotFile.physicalName(),
                                -1,
                                -1,
                                ByteSizeValue.MINUS_ONE,
                                ByteSizeValue.MINUS_ONE,
                                ByteSizeValue.MINUS_ONE,
                                -1,
                                -1,
                                null
                            ),
                            shardSnapshotsRefs.acquire()
                        );
                    }
                }

                return;
            }

            anomalyCount.incrementAndGet();
            responseWriter.writeResponseChunk(
                new ResponseWriter.ResponseChunk(
                    "snapshot not in shard generation",
                    snapshotDescription,
                    indexDescription,
                    shardId,
                    null,
                    null,
                    null,
                    -1,
                    -1,
                    ByteSizeValue.MINUS_ONE,
                    ByteSizeValue.MINUS_ONE,
                    ByteSizeValue.MINUS_ONE,
                    -1,
                    -1,
                    null
                ),
                shardSnapshotsRefs.acquire()
            );
        }

        private void verifyFileInfo(
            Releasable releasable,
            SnapshotDescription snapshotDescription,
            IndexDescription indexDescription,
            ShardContainerContents shardContainerContents,
            AtomicBoolean restorable,
            BlobStoreIndexShardSnapshot.FileInfo fileInfo
        ) {
            try (var fileRefs = new RefCountingRunnable(releasable::close)) {
                if (fileInfo.metadata().hashEqualsContents()) {
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
                        anomalyCount.incrementAndGet();
                        responseWriter.writeResponseChunk(
                            new ResponseWriter.ResponseChunk(
                                "missing blob",
                                snapshotDescription,
                                indexDescription,
                                shardId,
                                null,
                                blobName,
                                fileInfo.physicalName(),
                                partIndex,
                                fileInfo.numberOfParts(),
                                fileLength,
                                partLength,
                                ByteSizeValue.MINUS_ONE,
                                -1,
                                -1,
                                null
                            ),
                            fileRefs.acquire()
                        );
                        return;
                    } else if (blobInfo.length() != partLength.getBytes()) {
                        restorable.set(false);
                        anomalyCount.incrementAndGet();
                        responseWriter.writeResponseChunk(
                            new ResponseWriter.ResponseChunk(
                                "mismatched blob length",
                                snapshotDescription,
                                indexDescription,
                                shardId,
                                null,
                                blobName,
                                fileInfo.physicalName(),
                                partIndex,
                                fileInfo.numberOfParts(),
                                fileLength,
                                partLength,
                                ByteSizeValue.ofBytes(blobInfo.length()),
                                -1,
                                -1,
                                null
                            ),
                            fileRefs.acquire()
                        );
                        return;
                    }
                }

                blobContentsListeners(indexDescription, shardContainerContents, fileInfo).addListener(
                    makeListener(fileRefs.acquire(), (Void ignored) -> {}).delegateResponse((l, e) -> {
                        restorable.set(false);
                        anomalyCount.incrementAndGet();
                        responseWriter.writeResponseChunk(
                            new ResponseWriter.ResponseChunk(
                                "corrupt data blob",
                                snapshotDescription,
                                indexDescription,
                                shardId,
                                null,
                                fileInfo.name(),
                                fileInfo.physicalName(),
                                -1,
                                fileInfo.numberOfParts(),
                                fileLength,
                                ByteSizeValue.MINUS_ONE,
                                ByteSizeValue.MINUS_ONE,
                                -1,
                                -1,
                                e
                            ),
                            fileRefs.acquire()
                        );
                        l.onResponse(null);
                    })
                );
            }
        }

        private SubscribableListener<IndexDescription> indexDescriptionListeners(SnapshotId snapshotId, String indexMetaBlobId) {
            return indexDescriptionListenersByBlobId.computeIfAbsent(
                indexMetaBlobId,
                ignored -> SubscribableListener.newForked(
                    indexDescriptionListener -> metadataTaskRunner.run(ActionRunnable.supply(indexDescriptionListener, () -> {
                        try {
                            return new IndexDescription(
                                indexId,
                                indexMetaBlobId,
                                blobStoreRepository.getSnapshotIndexMetaData(repositoryData, snapshotId, indexId).getNumberOfShards()
                            );
                        } catch (Exception e) {
                            anomalyCount.incrementAndGet();
                            responseWriter.writeResponseChunk(
                                new ResponseWriter.ResponseChunk(
                                    "failed to load index metadata",
                                    null,
                                    new IndexDescription(indexId, indexMetaBlobId, 0),
                                    -1,
                                    null,
                                    null,
                                    null,
                                    -1,
                                    -1,
                                    ByteSizeValue.MINUS_ONE,
                                    ByteSizeValue.MINUS_ONE,
                                    ByteSizeValue.MINUS_ONE,
                                    -1,
                                    -1,
                                    e
                                ),
                                indexRefs.acquire()
                            );
                            throw new AnomalyException(e);
                        }
                    }))
                )
            );
        }

        private SubscribableListener<ShardContainerContents> shardContainerContentsListeners(
            IndexDescription indexDescription,
            int shardId
        ) {
            return shardContainerContentsListener.computeIfAbsent(
                shardId,
                ignored -> SubscribableListener.newForked(
                    shardContainerContentsListener -> metadataTaskRunner.run(ActionRunnable.supply(shardContainerContentsListener, () -> {
                        final Map<String, BlobMetadata> blobsByName;
                        try {
                            blobsByName = blobStoreRepository.shardContainer(indexId, shardId)
                                .listBlobs(OperationPurpose.REPOSITORY_ANALYSIS);
                        } catch (Exception e) {
                            anomalyCount.incrementAndGet();
                            responseWriter.writeResponseChunk(
                                new ResponseWriter.ResponseChunk(
                                    "failed to list shard container contents",
                                    null,
                                    indexDescription,
                                    shardId,
                                    null,
                                    null,
                                    null,
                                    -1,
                                    -1,
                                    ByteSizeValue.MINUS_ONE,
                                    ByteSizeValue.MINUS_ONE,
                                    ByteSizeValue.MINUS_ONE,
                                    -1,
                                    -1,
                                    e
                                ),
                                indexRefs.acquire()
                            );
                            throw new AnomalyException(e);
                        }

                        final var shardGen = repositoryData.shardGenerations().getShardGen(indexId, shardId);
                        if (shardGen == null) {
                            anomalyCount.incrementAndGet();
                            responseWriter.writeResponseChunk(
                                new ResponseWriter.ResponseChunk(
                                    "shard generation not defined",
                                    null,
                                    indexDescription,
                                    shardId,
                                    null,
                                    null,
                                    null,
                                    -1,
                                    -1,
                                    ByteSizeValue.MINUS_ONE,
                                    ByteSizeValue.MINUS_ONE,
                                    ByteSizeValue.MINUS_ONE,
                                    -1,
                                    -1,
                                    null
                                ),
                                indexRefs.acquire()
                            );
                            throw new AnomalyException(
                                new ElasticsearchException("undefined shard generation for " + indexId + "[" + shardId + "]")
                            );
                        }

                        return new ShardContainerContents(
                            shardId,
                            blobsByName,
                            loadShardGeneration(indexDescription, shardId, shardGen),
                            ConcurrentCollections.newConcurrentMap()
                        );
                    }))
                )
            );
        }

        private BlobStoreIndexShardSnapshots loadShardGeneration(IndexDescription indexDescription, int shardId, ShardGeneration shardGen) {
            try {
                return blobStoreRepository.getBlobStoreIndexShardSnapshots(indexId, shardId, shardGen);
            } catch (Exception e) {
                anomalyCount.incrementAndGet();
                responseWriter.writeResponseChunk(
                    new ResponseWriter.ResponseChunk(
                        "failed to load shard generation",
                        null,
                        indexDescription,
                        shardId,
                        shardGen,
                        null,
                        null,
                        -1,
                        -1,
                        ByteSizeValue.MINUS_ONE,
                        ByteSizeValue.MINUS_ONE,
                        ByteSizeValue.MINUS_ONE,
                        -1,
                        -1,
                        e
                    ),
                    indexRefs.acquire()
                );
                // failing here is not fatal to snapshot restores, only to creating/deleting snapshots, so we can carry on with the analysis
                return null;
            }
        }

        private SubscribableListener<Void> blobContentsListeners(
            IndexDescription indexDescription,
            ShardContainerContents shardContainerContents,
            BlobStoreIndexShardSnapshot.FileInfo fileInfo
        ) {
            return shardContainerContents.blobContentsListeners().computeIfAbsent(fileInfo.name(), ignored -> {
                if (verifyRequest.verifyBlobContents()) {
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

    private <T> ActionListener<T> makeListener(Releasable releasable, CheckedConsumer<T, Exception> consumer) {
        try (var refs = new RefCountingRunnable(releasable::close)) {
            return ActionListener.releaseAfter(ActionListener.wrap(consumer, exception -> {
                if (isCancelledSupplier.getAsBoolean() && exception instanceof TaskCancelledException) {
                    return;
                }
                if (exception instanceof AnomalyException) {
                    // already reported
                    return;
                }
                anomalyCount.incrementAndGet();
                responseWriter.writeResponseChunk(
                    new ResponseWriter.ResponseChunk(
                        "unexpected exception",
                        null,
                        null,
                        -1,
                        null,
                        null,
                        null,
                        -1,
                        -1,
                        ByteSizeValue.MINUS_ONE,
                        ByteSizeValue.MINUS_ONE,
                        ByteSizeValue.MINUS_ONE,
                        -1,
                        -1,
                        exception
                    ),
                    refs.acquire()
                );
            }), refs.acquire());
        }
    }

    private Runnable wrapRunnable(Releasable releasable, Runnable runnable) {
        return () -> {
            try (releasable) {
                runnable.run();
            }
        };
    }

    private void onCompletion() {
        SubscribableListener

            .<RepositoryData>newForked(
                l -> blobStoreRepository.getRepositoryData(blobStoreRepository.threadPool().executor(ThreadPool.Names.SNAPSHOT), l)
            )
            .andThenApply(
                finalRepositoryData -> new VerificationResult(
                    repositoryData.getGenId(),
                    finalRepositoryData.getGenId(),
                    isCancelledSupplier.getAsBoolean(),
                    anomalyCount.get()
                )
            )
            .addListener(finalListener);
    }

    public record VerificationResult(
        long originalRepositoryGeneration,
        long finalRepositoryGeneration,
        boolean isCancelled,
        long totalAnomalies
    ) implements Writeable {

        public VerificationResult(StreamInput in) throws IOException {
            this(in.readLong(), in.readLong(), in.readBoolean(), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(originalRepositoryGeneration);
            out.writeLong(finalRepositoryGeneration);
            out.writeBoolean(isCancelled);
            out.writeVLong(totalAnomalies);
        }
    }

    private static <T> void runThrottled(
        Iterator<T> iterator,
        BiConsumer<Releasable, T> itemConsumer,
        int maxConcurrency,
        AtomicLong progressCounter,
        Runnable onCompletion
    ) {
        ThrottledIterator.run(iterator, itemConsumer, maxConcurrency, progressCounter::incrementAndGet, onCompletion);
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

    private static class AnomalyException extends Exception {
        AnomalyException(Exception cause) {
            super(cause);
        }
    }

}
