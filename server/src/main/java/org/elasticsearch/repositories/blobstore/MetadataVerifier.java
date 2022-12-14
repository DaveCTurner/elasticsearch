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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

class MetadataVerifier implements Releasable {
    private static final Logger logger = LogManager.getLogger(MetadataVerifier.class);

    private final BlobStoreRepository blobStoreRepository;
    private final ActionListener<Void> finalListener;
    private final RefCounted finalRefs = AbstractRefCounted.of(this::onCompletion);
    private final String repositoryName;
    private final RepositoryData repositoryData;
    private final Set<String> failures = Collections.synchronizedSet(new TreeSet<>());
    private final AtomicBoolean isComplete = new AtomicBoolean();
    private final Map<String, Set<SnapshotId>> snapshotsByIndex;

    MetadataVerifier(BlobStoreRepository blobStoreRepository, RepositoryData repositoryData, ActionListener<Void> finalListener) {
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

    private void addFailure(String format, Object... args) {
        final var failure = format(format, args);
        logger.info("[{}] found metadata verification failure: {}", repositoryName, failure);
        failures.add(format("[%s] %s", repositoryName, failure));
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

        final var perSnapshotVerificationRef = AbstractRefCounted.of(this::verifyIndices);

        try {
            for (final var snapshotId : repositoryData.getSnapshotIds()) {
                verifySnapshot(perSnapshotVerificationRef, snapshotId);
            }
        } finally {
            perSnapshotVerificationRef.decRef();
        }
    }

    private void verifySnapshot(RefCounted refCounted, SnapshotId snapshotId) {
        if (repositoryData.hasMissingDetails(snapshotId)) {
            // may not always be true for repositories that haven't been touched by newer versions; TODO make this check optional
            addFailure("snapshot [%s] has missing snapshot details", snapshotId);
        }

        blobStoreRepository.getSnapshotInfo(snapshotId, makeListener(refCounted, snapshotInfo -> {
            if (snapshotInfo.snapshotId().equals(snapshotId) == false) {
                addFailure("snapshot [%s] has unexpected ID in info blob: [%s]", snapshotId, snapshotInfo.snapshotId());
            }
            for (final var index : snapshotInfo.indices()) {
                if (snapshotsByIndex.get(index).contains(snapshotId) == false) {
                    addFailure("snapshot [%s] contains unexpected index [%s]", snapshotId, index);
                }
            }
        }, "verify snapshot info for %s", snapshotId));

        forkSupply(refCounted, () -> blobStoreRepository.getSnapshotGlobalMetadata(snapshotId), metadata -> {
            if (metadata.indices().isEmpty() == false) {
                addFailure("snapshot [%s] contains unexpected index metadata within global metadata", snapshotId);
            }
        }, "verify global metadata for %s", snapshotId);
    }

    private void verifyIndices() {
        for (final var indicesEntry : repositoryData.getIndices().entrySet()) {
            final var name = indicesEntry.getKey();
            final var indexId = indicesEntry.getValue();
            if (name.equals(indexId.getName()) == false) {
                addFailure("index name [%s] has mismatched name in [%s]", name, indexId);
                continue;
            }

            final var expectedSnapshots = snapshotsByIndex.get(name);

            // TODO must limit the number of indices currently being processed to avoid loading all the IndexMetadata at once

            // TODO consider distributing the workload, giving each node a subset of indices to process

            finalRefs.incRef(); // released in onIndexMetadataChecksComplete
            final var shardBlobsListenersByShard = ConcurrentCollections.<
                Integer,
                ListenableActionFuture<Map<String, BlobMetadata>>>newConcurrentMap();
            final var indexMetadataChecksRef = AbstractRefCounted.of(
                () -> onIndexMetadataChecksComplete(indexId, shardBlobsListenersByShard)
            );
            try {
                final var shardCountListenersByBlobId = new HashMap<String, ListenableActionFuture<Integer>>();
                for (final var snapshotId : repositoryData.getSnapshots(indexId)) {
                    // TODO must limit the number of snapshots currently being processed to avoid loading all the metadata at once

                    if (expectedSnapshots.contains(snapshotId) == false) {
                        addFailure("index [%s] has mismatched snapshot [%s]", name, indexId, snapshotId);
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
                    }).addListener(makeListener(indexMetadataChecksRef, shardCount -> {
                        for (int i = 0; i < shardCount; i++) {
                            final var shardId = i;
                            shardBlobsListenersByShard.get(i)
                                .addListener(
                                    makeListener(
                                        indexMetadataChecksRef,
                                        shardBlobs -> forkSupply(
                                            indexMetadataChecksRef,
                                            () -> blobStoreRepository.loadShardSnapshot(
                                                blobStoreRepository.shardContainer(indexId, shardId),
                                                snapshotId
                                            ),
                                            shardSnapshot -> verifyShardSnapshot(snapshotId, indexId, shardId, shardBlobs, shardSnapshot),
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
                if (shardCountListenersByBlobId.isEmpty()) {
                    throw new IllegalStateException(format("index [%s] has no metadata", indexId));
                }
            } finally {
                indexMetadataChecksRef.decRef();
            }
        }
    }

    private void verifyShardSnapshot(
        SnapshotId snapshotId,
        IndexId indexId,
        int shardId,
        Map<String, BlobMetadata> shardBlobs,
        BlobStoreIndexShardSnapshot shardSnapshot
    ) {
        if (shardSnapshot.snapshot().equals(snapshotId.getName()) == false) {
            addFailure("snapshot [%s] for shard [%s/%d] has mismatched name [%s]", snapshotId, indexId, shardId, shardSnapshot.snapshot());
        }

        for (final var fileInfo : shardSnapshot.indexFiles()) {
            verifyFileInfo(snapshotId.toString(), indexId, shardId, shardBlobs, fileInfo);
        }
    }

    private void verifyFileInfo(
        String snapshot,
        IndexId indexId,
        int shardId,
        Map<String, BlobMetadata> shardBlobs,
        BlobStoreIndexShardSnapshot.FileInfo fileInfo
    ) {
        if (fileInfo.metadata().hashEqualsContents()) {
            if (fileInfo.length() != fileInfo.metadata().length()) {
                addFailure(
                    "snapshot [%s] for shard [%s/%d] has virtual blob for [%s] with length [%d] instead of [%d]",
                    snapshot,
                    indexId,
                    shardId,
                    fileInfo.physicalName(),
                    fileInfo.metadata().length(),
                    fileInfo.length()
                );
            }
        } else {
            for (int part = 0; part < fileInfo.numberOfParts(); part++) {
                final var blobName = fileInfo.partName(part);
                final var blobInfo = shardBlobs.get(blobName);
                if (blobInfo == null) {
                    addFailure(
                        "snapshot [%s] for shard [%s/%d] has missing blob [%s] for [%s]",
                        snapshot,
                        indexId,
                        shardId,
                        blobName,
                        fileInfo.physicalName()
                    );
                } else if (blobInfo.length() != fileInfo.partBytes(part)) {
                    addFailure(
                        "snapshot [%s] for shard [%s/%d] has blob [%s] for [%s] with length [%d] instead of [%d]",
                        snapshot,
                        indexId,
                        shardId,
                        blobName,
                        fileInfo.physicalName(),
                        blobInfo.length(),
                        fileInfo.partBytes(part)
                    );
                }
            }
        }
    }

    private void onIndexMetadataChecksComplete(
        IndexId indexId,
        Map<Integer, ListenableActionFuture<Map<String, BlobMetadata>>> shardBlobsListeners
    ) {
        final var shardGenerationChecksRef = AbstractRefCounted.of(finalRefs::decRef);
        try {
            for (final var shardEntry : shardBlobsListeners.entrySet()) {
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
                                            verifyFileInfo(snapshotFiles.snapshot(), indexId, shardId, shardBlobs, fileInfo);
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
        } finally {
            shardGenerationChecksRef.decRef();
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
        return ActionListener.runAfter(ActionListener.wrap(consumer, e -> addFailure("%s", e.getMessage())), () -> {
            logger.trace("end {}", description);
            refCounted.decRef();
        });
    }

    private <T> void forkSupply(CheckedSupplier<T, Exception> supplier, ActionListener<T> listener) {
        // TODO limit concurrency here, don't max out the SNAPSHOT_META threadpool
        blobStoreRepository.threadPool().executor(ThreadPool.Names.SNAPSHOT_META).execute(ActionRunnable.supply(listener, supplier));
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
            if (failures.isEmpty()) {
                finalListener.onResponse(null);
            } else {
                finalListener.onFailure(new IllegalStateException(String.join("\n", failures)));
            }
        }
    }
}
