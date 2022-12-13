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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

class MetadataVerifier {
    private static final Logger logger = LogManager.getLogger(MetadataVerifier.class);

    private final BlobStoreRepository blobStoreRepository;
    private final LongFunction<RepositoryData> repositoryDataLoader;
    private final ActionListener<Void> finalListener;
    private final RefCounted finalRefs = AbstractRefCounted.of(this::onCompletion);
    private final String repositoryName;
    private final Set<String> failures = Collections.synchronizedSet(new TreeSet<>());
    private final AtomicBoolean isComplete = new AtomicBoolean();

    MetadataVerifier(
        BlobStoreRepository blobStoreRepository,
        LongFunction<RepositoryData> repositoryDataLoader,
        ActionListener<Void> finalListener
    ) {
        this.blobStoreRepository = blobStoreRepository;
        this.repositoryName = blobStoreRepository.metadata.name();
        this.repositoryDataLoader = repositoryDataLoader;
        this.finalListener = finalListener;
    }

    void run() {
        try {
            blobStoreRepository.getRepositoryData(makeListener(finalRefs, this::onRepositoryDataReceived));
        } finally {
            finalRefs.decRef();
        }
    }

    private void addFailure(String format, Object... args) {
        final var failure = format(format, args);
        logger.info("addFailure: {}", failure);
        failures.add(failure);
    }

    private void onRepositoryDataReceived(RepositoryData repositoryData) {
        logger.info(
            "[{}] verifying metadata integrity for index generation [{}]: repo UUID [{}], cluster UUID [{}]",
            repositoryName,
            repositoryData.getGenId(),
            repositoryData.getUuid(),
            repositoryData.getClusterUUID()
        );

        forkSupply(finalRefs, () -> repositoryDataLoader.apply(repositoryData.getGenId()), loadedRepoData -> {
            if (loadedRepoData.getGenId() != repositoryData.getGenId()) {
                addFailure(
                    "[%s] has repository data generation [{}], expected [{}]",
                    repositoryName,
                    loadedRepoData.getGenId(),
                    repositoryData.getGenId()
                );
            }
        });

        final var snapshotsByIndex = repositoryData.getIndices()
            .values()
            .stream()
            .collect(Collectors.toMap(IndexId::getName, indexId -> new HashSet<>(repositoryData.getSnapshots(indexId))));

        for (final var snapshotId : repositoryData.getSnapshotIds()) {
            if (repositoryData.hasMissingDetails(snapshotId)) {
                // may not always be true for repositories that haven't been touched by newer versions; TODO make this check optional
                addFailure("[%s] snapshot [%s] has missing snapshot details", repositoryName, snapshotId);
            }

            blobStoreRepository.getSnapshotInfo(snapshotId, makeListener(finalRefs, snapshotInfo -> {
                if (snapshotInfo.snapshotId().equals(snapshotId) == false) {
                    addFailure(
                        "[%s] snapshot [%s] has unexpected ID in info blob: [%s]",
                        repositoryName,
                        snapshotId,
                        snapshotInfo.snapshotId()
                    );
                }
                for (final var index : snapshotInfo.indices()) {
                    if (snapshotsByIndex.get(index).contains(snapshotId) == false) {
                        addFailure("[%s] snapshot [%s] contains unexpected index [%s]", repositoryName, snapshotId, index);
                    }
                }
            }));

            forkSupply(finalRefs, () -> blobStoreRepository.getSnapshotGlobalMetadata(snapshotId), metadata -> {
                if (metadata.indices().isEmpty() == false) {
                    addFailure("[%s] snapshot [%s] contains unexpected index metadata within global metadata", repositoryName, snapshotId);
                }
            });
        }

        for (final var indicesEntry : repositoryData.getIndices().entrySet()) {
            final var name = indicesEntry.getKey();
            final var indexId = indicesEntry.getValue();
            if (name.equals(indexId.getName()) == false) {
                addFailure("[%s] index name [%s] has mismatched name in [%s]", repositoryName, name, indexId);
                continue;
            }

            // TODO must limit the number of indices currently being processed to avoid loading all the IndexMetadata at once

            finalRefs.incRef(); // released in onIndexMetadataChecksComplete
            final var shardBlobsListenersByShard = ConcurrentCollections.<
                Integer,
                ListenableActionFuture<Map<String, BlobMetadata>>>newConcurrentMap();
            final var indexMetadataChecksRef = AbstractRefCounted.of(
                () -> onIndexMetadataChecksComplete(repositoryData, indexId, shardBlobsListenersByShard)
            );
            try {
                final var indexMetadataListenersByBlobId = new HashMap<String, ListenableActionFuture<IndexMetadata>>();
                for (final var snapshotId : repositoryData.getSnapshots(indexId)) {
                    // TODO must limit the number of snapshots currently being processed to avoid loading all the metadata at once
                    final var indexMetaBlobId = repositoryData.indexMetaDataGenerations().indexMetaBlobId(snapshotId, indexId);
                    indexMetadataListenersByBlobId.computeIfAbsent(indexMetaBlobId, ignored -> {
                        final var indexMetadataFuture = new ListenableActionFuture<IndexMetadata>();
                        forkSupply(() -> {
                            final var indexMetadata = blobStoreRepository.getSnapshotIndexMetaData(repositoryData, snapshotId, indexId);
                            for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
                                shardBlobsListenersByShard.computeIfAbsent(i, shardId -> {
                                    final var shardBlobsFuture = new ListenableActionFuture<Map<String, BlobMetadata>>();
                                    forkSupply(() -> blobStoreRepository.shardContainer(indexId, shardId).listBlobs(), shardBlobsFuture);
                                    return shardBlobsFuture;
                                });
                            }
                            return indexMetadata;
                        }, indexMetadataFuture);
                        return indexMetadataFuture;
                    }).addListener(makeListener(indexMetadataChecksRef, indexMetadata -> {
                        for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
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
                                            shardSnapshot -> verifyShardSnapshot(snapshotId, indexId, shardId, shardBlobs, shardSnapshot)
                                        )
                                    )
                                );
                        }
                    }));
                }
                if (indexMetadataListenersByBlobId.isEmpty()) {
                    throw new IllegalStateException(format("[%s] index [%s] has no metadata", repositoryName, indexId));
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
            addFailure(
                "[%s] snapshot [%s] for shard [{}/{}] has mismatched name [{}]",
                repositoryName,
                snapshotId,
                shardId,
                shardSnapshot.snapshot()
            );
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
                    "[%s] snapshot [%s] for shard [%s/%d] has virtual blob for [%s] with length [%d] instead of [%d]",
                    repositoryName,
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
                        "[%s] snapshot [%s] for shard [%s/%d] has missing blob [%s] for [%s]",
                        repositoryName,
                        snapshot,
                        indexId,
                        shardId,
                        blobName,
                        fileInfo.physicalName()
                    );
                } else if (blobInfo.length() != fileInfo.partBytes(part)) {
                    addFailure(
                        "[%s] snapshot [%s] for shard [%s/%d] has blob [%s] for [%s] with length [%d] instead of [%d]",
                        repositoryName,
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
        RepositoryData repositoryData,
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
                                }
                            )
                        )
                    );
            }
        } finally {
            shardGenerationChecksRef.decRef();
        }
    }

    private <T> ActionListener<T> makeListener(RefCounted refCounted, CheckedConsumer<T, Exception> consumer) {
        refCounted.incRef();
        return ActionListener.runAfter(ActionListener.wrap(consumer, e -> failures.add(e.getMessage())), refCounted::decRef);
    }

    private <T> void forkSupply(CheckedSupplier<T, Exception> supplier, ActionListener<T> listener) {
        blobStoreRepository.threadPool().executor(ThreadPool.Names.SNAPSHOT_META).execute(ActionRunnable.supply(listener, supplier));
    }

    private <T> void forkSupply(RefCounted refCounted, CheckedSupplier<T, Exception> supplier, CheckedConsumer<T, Exception> consumer) {
        forkSupply(supplier, makeListener(refCounted, consumer));
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
