/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.stats;

import org.elasticsearch.action.UnnecessaryActionTypeSubclass;

/**
 * This ActionType gets the stats for the watcher plugin
 */
public class WatcherStatsAction extends UnnecessaryActionTypeSubclass<WatcherStatsResponse> {

    public static final WatcherStatsAction INSTANCE = new WatcherStatsAction();
    public static final String NAME = "cluster:monitor/xpack/watcher/stats/dist";

    private WatcherStatsAction() {
        super(NAME);
    }
}
