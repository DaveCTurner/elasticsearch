/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.snapshots;

/**
 * Returns an immutable state of {@link RunningIndexShardSnapshot} at a given point in time.
 */
public class IndexShardSnapshotStatus {

    private final RunningIndexShardSnapshot.Stage stage;
    private final long startTime;
    private final long totalTime;
    private final int incrementalFileCount;
    private final int totalFileCount;
    private final int processedFileCount;
    private final long totalSize;
    private final long processedSize;
    private final long incrementalSize;
    private final String failure;

    public IndexShardSnapshotStatus(
        final RunningIndexShardSnapshot.Stage stage,
        final long startTime,
        final long totalTime,
        final int incrementalFileCount,
        final int totalFileCount,
        final int processedFileCount,
        final long incrementalSize,
        final long totalSize,
        final long processedSize,
        final String failure
    ) {
        this.stage = stage;
        this.startTime = startTime;
        this.totalTime = totalTime;
        this.incrementalFileCount = incrementalFileCount;
        this.totalFileCount = totalFileCount;
        this.processedFileCount = processedFileCount;
        this.totalSize = totalSize;
        this.processedSize = processedSize;
        this.incrementalSize = incrementalSize;
        this.failure = failure;
    }

    public RunningIndexShardSnapshot.Stage getStage() {
        return stage;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getTotalTime() {
        return totalTime;
    }

    public int getIncrementalFileCount() {
        return incrementalFileCount;
    }

    public int getTotalFileCount() {
        return totalFileCount;
    }

    public int getProcessedFileCount() {
        return processedFileCount;
    }

    public long getIncrementalSize() {
        return incrementalSize;
    }

    public long getTotalSize() {
        return totalSize;
    }

    public long getProcessedSize() {
        return processedSize;
    }

    public String getFailure() {
        return failure;
    }

    @Override
    public String toString() {
        return "index shard snapshot status ("
            + "stage="
            + stage
            + ", startTime="
            + startTime
            + ", totalTime="
            + totalTime
            + ", incrementalFileCount="
            + incrementalFileCount
            + ", totalFileCount="
            + totalFileCount
            + ", processedFileCount="
            + processedFileCount
            + ", incrementalSize="
            + incrementalSize
            + ", totalSize="
            + totalSize
            + ", processedSize="
            + processedSize
            + ", failure='"
            + failure
            + '\''
            + ')';
    }
}
