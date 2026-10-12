/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.action.support.replication;

/**
 * TEMPORARY elasticsearch-serverless#7911 repro.
 * <p>
 * Delays stay off until a test sets {@link #ready} after the cluster is up, and the test clears it before shutdown.
 */
public final class RefreshRaceDelays {

    public static volatile boolean ready;

    private RefreshRaceDelays() {}
}
