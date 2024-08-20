/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;

public interface ResponseWriter {

    void onFailedToLoadGlobalMetadata(SnapshotDescription snapshotDescription, Exception e, Releasable releasable);

    void onFailedToLoadSnapshotInfo(SnapshotId snapshotId, Exception e, Releasable releasable);

    void onUnknownSnapshotForIndex(IndexId indexId, SnapshotId snapshotId, Releasable releasable);

    void onFailedToLoadShardSnapshot(
        SnapshotDescription snapshotDescription,
        IndexDescription indexDescription,
        int shardId,
        Exception e,
        Releasable releasable
    );

    void onFileInShardGenerationNotSnapshot(
        SnapshotDescription snapshotDescription,
        IndexDescription indexDescription,
        int shardId,
        String physicalFileName,
        Releasable releasable
    );

    void onSnapshotShardGenerationMismatch(
        SnapshotDescription snapshotDescription,
        IndexDescription indexDescription,
        int shardId,
        String physicalFileName,
        Releasable releasable
    );

    void onFileInSnapshotNotShardGeneration(
        SnapshotDescription snapshotDescription,
        IndexDescription indexDescription,
        int shardId,
        String physicalFileName,
        Releasable releasable
    );

    void onSnapshotNotInShardGeneration(
        SnapshotDescription snapshotDescription,
        IndexDescription indexDescription,
        int shardId,
        Releasable releasable
    );
}
