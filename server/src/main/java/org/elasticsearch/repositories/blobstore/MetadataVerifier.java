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
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.elasticsearch.core.Strings.format;

class MetadataVerifier implements Releasable {
    private static final Logger logger = LogManager.getLogger(MetadataVerifier.class);

    private static final int THREADPOOL_CONCURRENCY = 5;
    private static final int SNAPSHOT_VERIFICATION_CONCURRENCY = 5;
    private static final int INDEX_VERIFICATION_CONCURRENCY = 5;
    private static final int INDEX_SNAPSHOT_VERIFICATION_CONCURRENCY = 5;

    private final BlobStoreRepository blobStoreRepository;
    private final ActionListener<List<ToXContentObject>> finalListener;
    private final RefCounted finalRefs = AbstractRefCounted.of(this::onCompletion);
    private final String repositoryName;
    private final RepositoryData repositoryData;
    // TODO more structured output, not just strings
    private final Queue<ToXContentObject> failures = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean isComplete = new AtomicBoolean();
    private final Map<String, Set<SnapshotId>> snapshotsByIndex;
    private final Semaphore threadPoolPermits = new Semaphore(THREADPOOL_CONCURRENCY);
    private final Queue<AbstractRunnable> executorQueue = new ConcurrentLinkedQueue<>();

    MetadataVerifier(
        BlobStoreRepository blobStoreRepository,
        RepositoryData repositoryData,
        ActionListener<List<ToXContentObject>> finalListener
    ) {
        this.blobStoreRepository = blobStoreRepository;
        this.repositoryName = blobStoreRepository.metadata.name();
        this.repositoryData = repositoryData;
        this.finalListener = finalListener;
        this.snapshotsByIndex = this.repositoryData.getIndices()
            .values()
            .stream()
            .collect(Collectors.toMap(IndexId::getName, indexId -> Set.copyOf(this.repositoryData.getSnapshots(indexId))));
    }

    @Override
    public void close() {
        finalRefs.decRef();
    }

    private void addFailure(ToXContentFragment failureBody) {
        final ToXContentObject failureObject = (builder, params) -> {
            builder.startObject();
            failureBody.toXContent(builder, params);
            builder.endObject();
            return builder;
        };
        logger.info("[{}] found metadata verification failure: {}", repositoryName, Strings.toString(failureObject));
        failures.add(failureObject);
    }

    public void run() {
        logger.info(
            "[{}] verifying metadata integrity for index generation [{}]: repo UUID [{}], cluster UUID [{}]",
            repositoryName,
            repositoryData.getGenId(),
            repositoryData.getUuid(),
            repositoryData.getClusterUUID()
        );

        verifySnapshots();
    }

    private void verifySnapshots() {
        runThrottled(
            makeVoidListener(finalRefs, this::verifyIndices, "verifying [%d] snapshots", repositoryData.getSnapshotIds().size()),
            repositoryData.getSnapshotIds().iterator(),
            this::verifySnapshot,
            SNAPSHOT_VERIFICATION_CONCURRENCY
        );
    }

    private void verifySnapshot(RefCounted snapshotRefs, SnapshotId snapshotId) {
        if (repositoryData.hasMissingDetails(snapshotId)) {
            // may not always be true for repositories that haven't been touched by newer versions; TODO make this check optional
            addFailure((builder, params) -> {
                builder.field("snapshot", snapshotId);
                builder.field("error", "missing snapshot details");
                return builder;
            });
        }

        blobStoreRepository.getSnapshotInfo(snapshotId, makeListener(snapshotRefs, snapshotInfo -> {
            if (snapshotInfo.snapshotId().equals(snapshotId) == false) {
                addFailure((builder, params) -> {
                    builder.field("snapshot", snapshotId);
                    builder.field("error", "unexpected ID in info blob");
                    builder.field("bad_id", snapshotInfo.snapshotId());
                    return builder;
                });
            }
            for (final var index : snapshotInfo.indices()) {
                if (snapshotsByIndex.get(index).contains(snapshotId) == false) {
                    addFailure((builder, params) -> {
                        builder.field("snapshot", snapshotId);
                        builder.field("error", "unexpected index");
                        builder.field("index_name", index);
                        return builder;
                    });
                }
            }
        }, "verify snapshot info for %s", snapshotId));

        forkSupply(snapshotRefs, () -> blobStoreRepository.getSnapshotGlobalMetadata(snapshotId), metadata -> {
            if (metadata.indices().isEmpty() == false) {
                addFailure((builder, params) -> {
                    builder.field("snapshot", snapshotId);
                    builder.field("error", "unexpected index metadata within global metadata");
                    return builder;
                });
            }
        }, "verify global metadata for %s", snapshotId);
    }

    private void verifyIndices() {
        final var indicesMap = repositoryData.getIndices();

        for (final var indicesEntry : indicesMap.entrySet()) {
            final var name = indicesEntry.getKey();
            final var indexId = indicesEntry.getValue();
            if (name.equals(indexId.getName()) == false) {
                addFailure((builder, params) -> {
                    builder.field("index", indexId);
                    builder.field("error", "index name mismatch");
                    builder.field("bad_name", name);
                    return builder;
                });
            }
        }

        runThrottled(
            makeVoidListener(finalRefs, () -> {}, "verifying [%d] indices", indicesMap.size()),
            indicesMap.values().iterator(),
            (refCounted, indexId) -> new IndexVerifier(refCounted, indexId).run(),
            INDEX_VERIFICATION_CONCURRENCY
        );
    }

    private class IndexVerifier {
        private final RefCounted indexRefs;
        private final IndexId indexId;
        private final Set<SnapshotId> expectedSnapshots;
        private final Map<Integer, ListenableActionFuture<Map<String, BlobMetadata>>> shardBlobsListenersByShard = newConcurrentMap();
        private final Map<String, ListenableActionFuture<Integer>> shardCountListenersByBlobId = new HashMap<>();

        IndexVerifier(RefCounted indexRefs, IndexId indexId) {
            this.indexRefs = indexRefs;
            this.indexId = indexId;
            this.expectedSnapshots = snapshotsByIndex.get(this.indexId.getName());
        }

        void run() {

            // TODO consider distributing the workload, giving each node a subset of indices to process

            final var indexSnapshots = repositoryData.getSnapshots(indexId);

            try (
                var indexMetadataChecksRef = wrap(
                    makeVoidListener(
                        indexRefs,
                        this::onIndexMetadataChecksComplete,
                        "waiting for verification of snapshots of index [%s]",
                        indexId
                    )
                )
            ) {
                runThrottled(
                    makeVoidListener(
                        indexMetadataChecksRef,
                        () -> {},
                        "checking [%d] snapshots for index [%s]",
                        indexSnapshots.size(),
                        indexId
                    ),
                    indexSnapshots.iterator(),
                    this::verifyIndexSnapshot,
                    INDEX_SNAPSHOT_VERIFICATION_CONCURRENCY
                );
            }
        }

        private void verifyIndexSnapshot(RefCounted indexSnapshotRefs, SnapshotId snapshotId) {
            if (expectedSnapshots.contains(snapshotId) == false) {
                addFailure((builder, params) -> {
                    builder.field("index", indexId);
                    builder.field("snapshot", snapshotId);
                    builder.field("error", "unexpected snapshot");
                    return builder;
                });
            }

            final var indexMetaBlobId = repositoryData.indexMetaDataGenerations().indexMetaBlobId(snapshotId, indexId);
            shardCountListenersByBlobId.computeIfAbsent(indexMetaBlobId, ignored -> {
                final var shardCountFuture = new ListenableActionFuture<Integer>();
                forkSupply(() -> {
                    final var shardCount = blobStoreRepository.getSnapshotIndexMetaData(repositoryData, snapshotId, indexId)
                        .getNumberOfShards();
                    for (int i = 0; i < shardCount; i++) {
                        shardBlobsListenersByShard.computeIfAbsent(i, shardId -> {
                            final var shardBlobsFuture = new ListenableActionFuture<Map<String, BlobMetadata>>();
                            forkSupply(() -> blobStoreRepository.shardContainer(indexId, shardId).listBlobs(), shardBlobsFuture);
                            return shardBlobsFuture;
                        });
                    }
                    return shardCount;
                }, shardCountFuture);
                return shardCountFuture;
            }).addListener(makeListener(indexSnapshotRefs, shardCount -> {
                for (int i = 0; i < shardCount; i++) {
                    final var shardId = i;
                    shardBlobsListenersByShard.get(i)
                        .addListener(
                            makeListener(
                                indexSnapshotRefs,
                                shardBlobs -> forkSupply(
                                    indexSnapshotRefs,
                                    () -> blobStoreRepository.loadShardSnapshot(
                                        blobStoreRepository.shardContainer(indexId, shardId),
                                        snapshotId
                                    ),
                                    shardSnapshot -> verifyShardSnapshot(snapshotId, shardId, shardBlobs, shardSnapshot),
                                    "verify snapshot [%s] for shard %s/%d",
                                    snapshotId,
                                    indexId,
                                    shardId
                                ),
                                "await listing for %s/%d before verifying snapshot [%s]",
                                indexId,
                                shardId,
                                snapshotId
                            )
                        );
                }
            }, "await index metadata for %s before verifying shards", indexId));
        }

        private void verifyShardSnapshot(
            SnapshotId snapshotId,
            int shardId,
            Map<String, BlobMetadata> shardBlobs,
            BlobStoreIndexShardSnapshot shardSnapshot
        ) {
            if (shardSnapshot.snapshot().equals(snapshotId.getName()) == false) {
                addFailure((builder, params) -> {
                    builder.field("index", indexId);
                    builder.field("shard", shardId);
                    builder.field("snapshot", snapshotId);
                    builder.field("error", "mismatched index shard snapshot name");
                    builder.field("bad_name", shardSnapshot.snapshot());
                    return builder;
                });
            }

            for (final var fileInfo : shardSnapshot.indexFiles()) {
                verifyFileInfo(snapshotId.toString(), shardId, shardBlobs, fileInfo);
            }
        }

        private void verifyFileInfo(
            String snapshot,
            int shardId,
            Map<String, BlobMetadata> shardBlobs,
            BlobStoreIndexShardSnapshot.FileInfo fileInfo
        ) {
            if (fileInfo.metadata().hashEqualsContents()) {
                if (fileInfo.length() != fileInfo.metadata().length()) {
                    addFailure((builder, params) -> {
                        builder.field("index", indexId);
                        builder.field("shard", shardId);
                        builder.field("snapshot", snapshot);
                        builder.field("file", fileInfo.physicalName());
                        builder.field("checksum", fileInfo.checksum());
                        builder.field("part", 1);
                        builder.field("blob", fileInfo.name());
                        builder.field("error", "mismatched virtual blob size");
                        builder.field("expected_length", fileInfo.length());
                        builder.field("actual_length", fileInfo.metadata().length());
                        return builder;
                    });
                }
            } else {
                for (int i = 0; i < fileInfo.numberOfParts(); i++) {
                    final var part = i;
                    final var blobName = fileInfo.partName(part);
                    final var blobInfo = shardBlobs.get(blobName);
                    if (blobInfo == null) {
                        addFailure((builder, params) -> {
                            builder.field("index", indexId);
                            builder.field("shard", shardId);
                            builder.field("snapshot", snapshot);
                            builder.field("file", fileInfo.physicalName());
                            builder.field("checksum", fileInfo.checksum());
                            builder.field("part", part);
                            builder.field("blob", blobName);
                            builder.field("error", "missing blob");
                            builder.field("expected_length", fileInfo.length());
                            return builder;
                        });
                    } else if (blobInfo.length() != fileInfo.partBytes(part)) {
                        addFailure((builder, params) -> {
                            builder.field("index", indexId);
                            builder.field("shard", shardId);
                            builder.field("snapshot", snapshot);
                            builder.field("file", fileInfo.physicalName());
                            builder.field("checksum", fileInfo.checksum());
                            builder.field("part", part);
                            builder.field("blob", blobName);
                            builder.field("error", "mismatched blob size");
                            builder.field("expected_length", fileInfo.length());
                            builder.field("actual_length", blobInfo.length());
                            return builder;
                        });
                    }
                }
            }
        }

        private void onIndexMetadataChecksComplete() {
            if (shardCountListenersByBlobId.isEmpty()) {
                throw new IllegalStateException(format("index [%s] has no metadata", indexId));
            }

            try (
                var shardGenerationChecksRef = wrap(makeVoidListener(indexRefs, () -> {}, "checking shard generations for [%s]", indexId))
            ) {
                for (final var shardEntry : shardBlobsListenersByShard.entrySet()) {
                    verifyShardGenerations(shardGenerationChecksRef, shardEntry);
                }
            }
        }

        private void verifyShardGenerations(
            RefCounted shardGenerationChecksRef,
            Map.Entry<Integer, ListenableActionFuture<Map<String, BlobMetadata>>> shardEntry
        ) {
            final int shardId = shardEntry.getKey();
            shardEntry.getValue()
                .addListener(
                    makeListener(
                        shardGenerationChecksRef,
                        shardBlobs -> forkSupply(
                            shardGenerationChecksRef,
                            () -> blobStoreRepository.getBlobStoreIndexShardSnapshots(
                                indexId,
                                shardId,
                                Objects.requireNonNull(
                                    repositoryData.shardGenerations().getShardGen(indexId, shardId),
                                    "shard generations for " + indexId + "/" + shardId
                                )
                            ),
                            blobStoreIndexShardSnapshots -> {
                                for (final var snapshotFiles : blobStoreIndexShardSnapshots.snapshots()) {
                                    snapshotFiles.snapshot(); // TODO validate
                                    snapshotFiles.shardStateIdentifier(); // TODO validate
                                    for (final var fileInfo : snapshotFiles.indexFiles()) {
                                        verifyFileInfo(snapshotFiles.snapshot(), shardId, shardBlobs, fileInfo);
                                    }
                                }
                            },
                            "check shard generations for %s/%d",
                            indexId,
                            shardId
                        ),
                        "await listing for %s/%d before checking shard generations",
                        indexId,
                        shardId
                    )
                );
        }
    }

    private final AtomicLong idGenerator = new AtomicLong();

    private <T> ActionListener<T> makeListener(
        RefCounted refCounted,
        CheckedConsumer<T, Exception> consumer,
        String format,
        Object... args
    ) {
        final var description = format("[%d] ", idGenerator.incrementAndGet()) + format(format, args);
        logger.trace("start {}", description);
        refCounted.incRef();
        return ActionListener.runAfter(ActionListener.wrap(consumer, e -> addFailure((builder, params) -> {
            ElasticsearchException.generateFailureXContent(builder, params, e, true);
            return builder;
        })), () -> {
            logger.trace("end {}", description);
            refCounted.decRef();
        });
    }

    private ActionListener<Void> makeVoidListener(
        RefCounted refCounted,
        CheckedRunnable<Exception> runnable,
        String format,
        Object... args
    ) {
        return makeListener(refCounted, ignored -> runnable.run(), format, args);
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

            // TODO add cancellation support

            blobStoreRepository.threadPool().executor(ThreadPool.Names.SNAPSHOT_META).execute(new AbstractRunnable() {
                @Override
                public void onRejection(Exception e) {
                    try {
                        runnable.onRejection(e);
                    } finally {
                        onCompletion();
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

    private <T> void forkSupply(
        RefCounted refCounted,
        CheckedSupplier<T, Exception> supplier,
        CheckedConsumer<T, Exception> consumer,
        String format,
        Object... args
    ) {
        forkSupply(supplier, makeListener(refCounted, consumer, format, args));
    }

    private void onCompletion() {
        if (isComplete.compareAndSet(false, true)) {
            finalListener.onResponse(failures.stream().toList());
        }
    }

    private interface RefCountedListenerWrapper extends Releasable, RefCounted {}

    private static RefCountedListenerWrapper wrap(ActionListener<Void> listener) {
        final var refCounted = AbstractRefCounted.of(() -> listener.onResponse(null));
        return new RefCountedListenerWrapper() {
            @Override
            public void incRef() {
                refCounted.incRef();
            }

            @Override
            public boolean tryIncRef() {
                return refCounted.tryIncRef();
            }

            @Override
            public boolean decRef() {
                return refCounted.decRef();
            }

            @Override
            public boolean hasReferences() {
                return refCounted.hasReferences();
            }

            @Override
            public void close() {
                decRef();
            }
        };
    }

    private static <T> void runThrottled(
        ActionListener<Void> completionListener,
        Iterator<T> iterator,
        BiConsumer<RefCounted, T> consumer,
        int maxConcurrency
    ) {
        try (var throttledIterator = new ThrottledIterator<>(completionListener, iterator, consumer, maxConcurrency)) {
            throttledIterator.run();
        }
    }

    private static class ThrottledIterator<T> implements Releasable {
        private final RefCountedListenerWrapper refCounted;
        private final Iterator<T> iterator;
        private final BiConsumer<RefCounted, T> consumer;
        private final Semaphore permits;

        ThrottledIterator(
            ActionListener<Void> completionListener,
            Iterator<T> iterator,
            BiConsumer<RefCounted, T> consumer,
            int maxConcurrency
        ) {
            this.refCounted = wrap(completionListener);
            this.iterator = iterator;
            this.consumer = consumer;
            this.permits = new Semaphore(maxConcurrency);
        }

        void run() {
            while (permits.tryAcquire()) {
                final T item;
                synchronized (iterator) {
                    if (iterator.hasNext()) {
                        item = iterator.next();
                    } else {
                        permits.release();
                        return;
                    }
                }
                refCounted.incRef();
                final var itemRefCount = AbstractRefCounted.of(() -> {
                    permits.release();
                    try {
                        run();
                    } finally {
                        refCounted.decRef();
                    }
                });
                try {
                    consumer.accept(itemRefCount, item);
                } finally {
                    itemRefCount.decRef();
                }
            }
        }

        @Override
        public void close() {
            refCounted.close();
        }
    }
}
