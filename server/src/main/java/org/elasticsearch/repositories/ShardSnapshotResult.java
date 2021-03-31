/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.Objects;

/**
 * The details of a successful shard-level snapshot that are used to build the overall snapshot during finalization.
 */
public class ShardSnapshotResult {

    private final String generation;

    private final ByteSizeValue size;

    private final int segmentCount;

    /**
     * @param generation   the shard generation UUID, which uniquely identifies the specific snapshot of the shard
     * @param size         the total size of all the blobs that make up the shard snapshot, or equivalently, the size of the shard when
     *                     restored
     * @param segmentCount the number of segments in this shard snapshot
     */
    public ShardSnapshotResult(String generation, ByteSizeValue size, int segmentCount) {
        this.generation = Objects.requireNonNull(generation);
        this.size = Objects.requireNonNull(size);
        assert segmentCount >= -1;
        this.segmentCount = segmentCount;
    }

    /**
     * @return the shard generation UUID, which uniquely identifies the specific snapshot of the shard
     */
    public String getGeneration() {
        return generation;
    }

    /**
     * @return the total size of all the blobs that make up the shard snapshot, or equivalently, the size of the shard when restored
     */
    public ByteSizeValue getSize() {
        return size;
    }

    /**
     * @return the number of segments in this shard snapshot
     */
    public int getSegmentCount() {
        return segmentCount;
    }
}
