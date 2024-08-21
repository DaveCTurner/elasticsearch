/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.snapshots.SnapshotId;

public interface ResponseWriter {

    record ResponseChunk(
        String anomaly,
        SnapshotDescription snapshotDescription,
        IndexDescription indexDescription,
        int shardId,
        String physicalFileName,
        Exception exception
    ) {}

    void writeResponseChunk(ResponseChunk responseChunk, Releasable releasable);

    default void onFailedToLoadGlobalMetadata(SnapshotDescription snapshotDescription, Exception e, Releasable releasable) {
        writeResponseChunk(new ResponseChunk("failed to load global metadata", snapshotDescription, null, -1, null, e), releasable);
    }

    default void onFailedToLoadSnapshotInfo(SnapshotId snapshotId, Exception e, Releasable releasable) {
        writeResponseChunk(
            new ResponseChunk("failed to load snapshot info", new SnapshotDescription(snapshotId, 0, 0), null, -1, null, e),
            releasable
        );
    }

    default void onUnknownSnapshotForIndex(IndexId indexId, SnapshotId snapshotId, Releasable releasable) {
        writeResponseChunk(
            new ResponseChunk(
                "unknown snapshot for index",
                new SnapshotDescription(snapshotId, 0, 0),
                new IndexDescription(indexId, null, 0),
                -1,
                null,
                null
            ),
            releasable
        );
    }

    default void onFailedToLoadShardSnapshot(
        SnapshotDescription snapshotDescription,
        IndexDescription indexDescription,
        int shardId,
        Exception e,
        Releasable releasable
    ) {
        writeResponseChunk(
            new ResponseChunk("failed to load shard snapshot", snapshotDescription, indexDescription, shardId, null, e),
            releasable
        );
    }

    default void onFileInShardGenerationNotSnapshot(
        SnapshotDescription snapshotDescription,
        IndexDescription indexDescription,
        int shardId,
        String physicalFileName,
        Releasable releasable
    ) {
        writeResponseChunk(
            new ResponseChunk(
                "blob in shard generation but not snapshot",
                snapshotDescription,
                indexDescription,
                shardId,
                physicalFileName,
                null
            ),
            releasable
        );
    }

    default void onSnapshotShardGenerationMismatch(
        SnapshotDescription snapshotDescription,
        IndexDescription indexDescription,
        int shardId,
        String physicalFileName,
        Releasable releasable
    ) {
        writeResponseChunk(
            new ResponseChunk("snapshot shard generation mismatch", snapshotDescription, indexDescription, shardId, physicalFileName, null),
            releasable
        );
    }

    default void onFileInSnapshotNotShardGeneration(
        SnapshotDescription snapshotDescription,
        IndexDescription indexDescription,
        int shardId,
        String physicalFileName,
        Releasable releasable
    ) {
        writeResponseChunk(
            new ResponseChunk(
                "blob in snapshot but not shard generation",
                snapshotDescription,
                indexDescription,
                shardId,
                physicalFileName,
                null
            ),
            releasable
        );
    }

    default void onSnapshotNotInShardGeneration(
        SnapshotDescription snapshotDescription,
        IndexDescription indexDescription,
        int shardId,
        Releasable releasable
    ) {
        writeResponseChunk(
            new ResponseChunk("snapshot not in shard generation", snapshotDescription, indexDescription, shardId, null, null),
            releasable
        );
    }

    void onMissingBlob(
        SnapshotDescription snapshotDescription,
        IndexDescription indexDescription,
        int shardId,
        String blobName,
        String physicalName,
        int partIndex,
        int partCount,
        ByteSizeValue fileLength,
        ByteSizeValue partLength,
        Releasable releasable
    );

    void onMismatchedBlobLength(
        SnapshotDescription snapshotDescription,
        IndexDescription indexDescription,
        int shardId,
        String blobName,
        String physicalName,
        int partIndex,
        int partCount,
        ByteSizeValue fileLength,
        ByteSizeValue partLength,
        ByteSizeValue blobLength,
        Releasable releasable
    );

    void onCorruptDataBlob(
        SnapshotDescription snapshotDescription,
        IndexDescription indexDescription,
        int shardId,
        String blobName,
        String physicalName,
        int partCount,
        ByteSizeValue fileLength,
        Exception e,
        Releasable releasable
    );

    void onFailedToLoadIndexMetadata(IndexId indexId, String indexMetaBlobId, Exception e, Releasable releasable);

    void onFailedToListShardContainer(IndexDescription indexDescription, int shardId, Exception e, Releasable releasable);

    void onUndefinedShardGeneration(IndexDescription indexDescription, int shardId, Releasable releasable);

    void onFailedToLoadShardGeneration(
        IndexDescription indexDescription,
        int shardId,
        ShardGeneration shardGeneration,
        Exception e,
        Releasable releasable
    );

    void onUnexpectedException(Exception exception, Releasable releasable);

    void recordIndexRestorability(IndexId indexId, int totalSnapshotCount, int restorableSnapshotCount, Releasable releasable);
}
