/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import org.elasticsearch.cluster.coordination.PersistedStateStats;
import org.elasticsearch.core.Releasable;

import java.util.function.LongSupplier;

public class PersistedStateStatsTracker {

    private final LongSupplier relativeTimeMillisSupplier;

    private static final long NOT_SET = -1L;

    // cumulative stats for past commits - access is synchronized on 'this'
    private long cumulativeDocCount = 0L;
    private long cumulativeDocSize = 0L;
    private long cumulativeWriteMillis = 0L;
    private long prepareCommitCount = 0;
    private long cumulativePrepareCommitMillis = 0L;
    private long commitCount = 0;
    private long cumulativeCommitMillis = 0L;
    private int documentBufferSize = 0;

    // stats for ongoing commit, updated under Coordinator#mutex on master-eligible nodes and on the async persisted state thread otherwise
    private long docCount = 0;
    private long totalDocSize = 0;
    private long writeTimeStartMillis = NOT_SET;
    private long writeTimeEndMillis  = NOT_SET;
    private long prepareCommitTimeStartMillis = NOT_SET;
    private long prepareCommitTimeEndMillis  = NOT_SET;
    private long commitTimeStartMillis = NOT_SET;
    private long commitTimeEndMillis  = NOT_SET;

    public PersistedStateStatsTracker(LongSupplier relativeTimeMillisSupplier) {
        this.relativeTimeMillisSupplier = relativeTimeMillisSupplier;
    }

    public synchronized PersistedStateStats getStats() {
        return new PersistedStateStats(
            cumulativeDocCount,
            cumulativeDocSize,
            cumulativeWriteMillis,
            prepareCommitCount,
            cumulativePrepareCommitMillis,
            commitCount,
            cumulativeCommitMillis,
            documentBufferSize);
    }

    public void onWriteCompleted(int documentBufferUsed) {
        assert (writeTimeStartMillis == NOT_SET) == (writeTimeEndMillis == NOT_SET);
        assert (prepareCommitTimeStartMillis == NOT_SET) == (prepareCommitTimeEndMillis == NOT_SET);
        assert (commitTimeStartMillis == NOT_SET) == (commitTimeEndMillis == NOT_SET);

        synchronized (this) {
            documentBufferSize = documentBufferUsed;
            cumulativeWriteMillis += writeTimeEndMillis - writeTimeStartMillis;
            cumulativePrepareCommitMillis += prepareCommitTimeEndMillis - prepareCommitTimeStartMillis;
            cumulativeCommitMillis += commitTimeEndMillis - commitTimeStartMillis;
            cumulativeDocCount += docCount;
            cumulativeDocSize += totalDocSize;
            if (prepareCommitTimeStartMillis != NOT_SET) {
                prepareCommitCount += 1;
            }
            if (commitTimeStartMillis != NOT_SET) {
                commitCount += 1;
            }
        }

        writeTimeStartMillis = NOT_SET;
        writeTimeEndMillis = NOT_SET;
        prepareCommitTimeStartMillis = NOT_SET;
        prepareCommitTimeEndMillis = NOT_SET;
        commitTimeStartMillis = NOT_SET;
        commitTimeEndMillis = NOT_SET;
        docCount = 0L;
        totalDocSize = 0L;
    }

    public void addDocument(int length) {
        assert writeTimeStartMillis != NOT_SET && writeTimeEndMillis == NOT_SET;
        docCount += 1;
        totalDocSize += length;
    }

    public Releasable getWriteTimer() {
        assert docCount == 0L;
        assert writeTimeStartMillis == NOT_SET;
        assert prepareCommitTimeStartMillis == NOT_SET;
        assert commitTimeStartMillis == NOT_SET;
        writeTimeStartMillis = relativeTimeMillisSupplier.getAsLong();
        return () -> writeTimeEndMillis = relativeTimeMillisSupplier.getAsLong();
    }

    public Releasable getPrepareCommitTimer() {
        assert prepareCommitTimeStartMillis == NOT_SET;
        assert commitTimeStartMillis == NOT_SET;
        prepareCommitTimeStartMillis = relativeTimeMillisSupplier.getAsLong();
        return () -> prepareCommitTimeEndMillis = relativeTimeMillisSupplier.getAsLong();
    }

    public Releasable getCommitTimer() {
        assert prepareCommitTimeStartMillis != NOT_SET;
        assert commitTimeStartMillis == NOT_SET;
        commitTimeStartMillis = relativeTimeMillisSupplier.getAsLong();
        return () -> commitTimeEndMillis = relativeTimeMillisSupplier.getAsLong();
    }
}
