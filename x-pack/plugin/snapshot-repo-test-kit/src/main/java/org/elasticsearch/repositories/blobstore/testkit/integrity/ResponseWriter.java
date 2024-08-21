/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.repositories.ShardGeneration;

public interface ResponseWriter {

    record ResponseChunk(
        String anomaly,
        SnapshotDescription snapshotDescription,
        IndexDescription indexDescription,
        int shardId,
        ShardGeneration shardGeneration,
        String blobName,
        String physicalFileName,
        int partIndex,
        int partCount,
        ByteSizeValue fileLength,
        ByteSizeValue partLength,
        ByteSizeValue blobLength,
        int totalSnapshotCount,
        int restorableSnapshotCount,
        Exception exception
    ) {}

    void writeResponseChunk(ResponseChunk responseChunk, Releasable releasable);
}
