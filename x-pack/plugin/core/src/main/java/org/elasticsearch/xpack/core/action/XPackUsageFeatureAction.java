/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.UnnecessaryActionTypeSubclass;
import org.elasticsearch.xpack.core.XPackField;

import java.util.List;

/**
 * A base action for usage of a feature plugin.
 *
 * This action is implemented by each feature plugin, bound to the public constants here. The
 * {@link XPackUsageAction} implementation iterates over the {@link #ALL} list of actions to form
 * the complete usage result.
 */
public final class XPackUsageFeatureAction {

    private XPackUsageFeatureAction() {/* no instances */}

    private static final String BASE_NAME = "cluster:monitor/xpack/usage/";

    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> SECURITY = xpackUsageFeatureAction(XPackField.SECURITY);
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> MONITORING = xpackUsageFeatureAction(XPackField.MONITORING);
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> WATCHER = xpackUsageFeatureAction(XPackField.WATCHER);
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> GRAPH = xpackUsageFeatureAction(XPackField.GRAPH);
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> MACHINE_LEARNING = xpackUsageFeatureAction(XPackField.MACHINE_LEARNING);
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> INFERENCE = xpackUsageFeatureAction(XPackField.INFERENCE);
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> LOGSTASH = xpackUsageFeatureAction(XPackField.LOGSTASH);
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> EQL = xpackUsageFeatureAction(XPackField.EQL);
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> ESQL = xpackUsageFeatureAction(XPackField.ESQL);
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> SQL = xpackUsageFeatureAction(XPackField.SQL);
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> ROLLUP = xpackUsageFeatureAction(XPackField.ROLLUP);
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> INDEX_LIFECYCLE = xpackUsageFeatureAction(XPackField.INDEX_LIFECYCLE);
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> SNAPSHOT_LIFECYCLE = xpackUsageFeatureAction(XPackField.SNAPSHOT_LIFECYCLE);
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> CCR = xpackUsageFeatureAction(XPackField.CCR);
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> TRANSFORM = xpackUsageFeatureAction(XPackField.TRANSFORM);
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> VOTING_ONLY = xpackUsageFeatureAction(XPackField.VOTING_ONLY);
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> FROZEN_INDICES = xpackUsageFeatureAction(XPackField.FROZEN_INDICES);
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> SPATIAL = xpackUsageFeatureAction(XPackField.SPATIAL);
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> ANALYTICS = xpackUsageFeatureAction(XPackField.ANALYTICS);
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> ENRICH = xpackUsageFeatureAction(XPackField.ENRICH);
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> SEARCHABLE_SNAPSHOTS = xpackUsageFeatureAction(
        XPackField.SEARCHABLE_SNAPSHOTS
    );
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> DATA_STREAMS = xpackUsageFeatureAction(XPackField.DATA_STREAMS);
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> DATA_STREAM_LIFECYCLE = xpackUsageFeatureAction(
        XPackField.DATA_STREAM_LIFECYCLE
    );
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> DATA_TIERS = xpackUsageFeatureAction(XPackField.DATA_TIERS);
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> AGGREGATE_METRIC = xpackUsageFeatureAction(XPackField.AGGREGATE_METRIC);
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> ARCHIVE = xpackUsageFeatureAction(XPackField.ARCHIVE);
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> HEALTH = xpackUsageFeatureAction(XPackField.HEALTH_API);
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> REMOTE_CLUSTERS = xpackUsageFeatureAction(XPackField.REMOTE_CLUSTERS);
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> ENTERPRISE_SEARCH = xpackUsageFeatureAction(XPackField.ENTERPRISE_SEARCH);
    public static final UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> UNIVERSAL_PROFILING = xpackUsageFeatureAction(XPackField.UNIVERSAL_PROFILING);

    static final List<UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse>> ALL = List.of(
        AGGREGATE_METRIC,
        ANALYTICS,
        CCR,
        DATA_STREAMS,
        DATA_STREAM_LIFECYCLE,
        DATA_TIERS,
        EQL,
        ESQL,
        FROZEN_INDICES,
        GRAPH,
        INDEX_LIFECYCLE,
        INFERENCE,
        LOGSTASH,
        MACHINE_LEARNING,
        MONITORING,
        ROLLUP,
        SEARCHABLE_SNAPSHOTS,
        SECURITY,
        SNAPSHOT_LIFECYCLE,
        SPATIAL,
        SQL,
        TRANSFORM,
        VOTING_ONLY,
        WATCHER,
        ARCHIVE,
        HEALTH,
        REMOTE_CLUSTERS,
        ENTERPRISE_SEARCH,
        UNIVERSAL_PROFILING
    );

    public static UnnecessaryActionTypeSubclass<XPackUsageFeatureResponse> xpackUsageFeatureAction(String suffix) {
        return new UnnecessaryActionTypeSubclass<>(BASE_NAME + suffix);
    }
}
