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
 * A base action for info about a feature plugin.
 *
 * This action is implemented by each feature plugin, bound to the public constants here. The
 * {@link XPackInfoAction} implementation iterates over the {@link #ALL} list of actions to form
 * the complete info result.
 */
public class XPackInfoFeatureAction {

    private static final String BASE_NAME = "cluster:monitor/xpack/info/";

    public static final UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse> SECURITY = xpackInfoFeatureAction(XPackField.SECURITY);
    public static final UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse> MONITORING = xpackInfoFeatureAction(XPackField.MONITORING);
    public static final UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse> WATCHER = xpackInfoFeatureAction(XPackField.WATCHER);
    public static final UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse> GRAPH = xpackInfoFeatureAction(XPackField.GRAPH);
    public static final UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse> MACHINE_LEARNING = xpackInfoFeatureAction(XPackField.MACHINE_LEARNING);
    public static final UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse> LOGSTASH = xpackInfoFeatureAction(XPackField.LOGSTASH);
    public static final UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse> EQL = xpackInfoFeatureAction(XPackField.EQL);
    public static final UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse> ESQL = xpackInfoFeatureAction(XPackField.ESQL);
    public static final UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse> SQL = xpackInfoFeatureAction(XPackField.SQL);
    public static final UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse> ROLLUP = xpackInfoFeatureAction(XPackField.ROLLUP);
    public static final UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse> INDEX_LIFECYCLE = xpackInfoFeatureAction(XPackField.INDEX_LIFECYCLE);
    public static final UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse> SNAPSHOT_LIFECYCLE = xpackInfoFeatureAction(XPackField.SNAPSHOT_LIFECYCLE);
    public static final UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse> CCR = xpackInfoFeatureAction(XPackField.CCR);
    public static final UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse> TRANSFORM = xpackInfoFeatureAction(XPackField.TRANSFORM);
    public static final UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse> VOTING_ONLY = xpackInfoFeatureAction(XPackField.VOTING_ONLY);
    public static final UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse> FROZEN_INDICES = xpackInfoFeatureAction(XPackField.FROZEN_INDICES);
    public static final UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse> SPATIAL = xpackInfoFeatureAction(XPackField.SPATIAL);
    public static final UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse> ANALYTICS = xpackInfoFeatureAction(XPackField.ANALYTICS);
    public static final UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse> ENRICH = xpackInfoFeatureAction(XPackField.ENRICH);
    public static final UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse> SEARCHABLE_SNAPSHOTS = xpackInfoFeatureAction(XPackField.SEARCHABLE_SNAPSHOTS);
    public static final UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse> DATA_STREAMS = xpackInfoFeatureAction(XPackField.DATA_STREAMS);
    public static final UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse> DATA_TIERS = xpackInfoFeatureAction(XPackField.DATA_TIERS);
    public static final UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse> AGGREGATE_METRIC = xpackInfoFeatureAction(XPackField.AGGREGATE_METRIC);
    public static final UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse> ARCHIVE = xpackInfoFeatureAction(XPackField.ARCHIVE);
    public static final UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse> ENTERPRISE_SEARCH = xpackInfoFeatureAction(XPackField.ENTERPRISE_SEARCH);
    public static final UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse> UNIVERSAL_PROFILING = xpackInfoFeatureAction(XPackField.UNIVERSAL_PROFILING);

    public static final List<UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse>> ALL = List.of(
        SECURITY,
        MONITORING,
        WATCHER,
        GRAPH,
        MACHINE_LEARNING,
        LOGSTASH,
        EQL,
        ESQL,
        SQL,
        ROLLUP,
        INDEX_LIFECYCLE,
        SNAPSHOT_LIFECYCLE,
        CCR,
        TRANSFORM,
        VOTING_ONLY,
        FROZEN_INDICES,
        SPATIAL,
        ANALYTICS,
        ENRICH,
        DATA_STREAMS,
        SEARCHABLE_SNAPSHOTS,
        DATA_TIERS,
        AGGREGATE_METRIC,
        ARCHIVE,
        ENTERPRISE_SEARCH,
        UNIVERSAL_PROFILING
    );

    public static UnnecessaryActionTypeSubclass<XPackInfoFeatureResponse> xpackInfoFeatureAction(String suffix) {
        return new UnnecessaryActionTypeSubclass<>(BASE_NAME + suffix);
    }

    private XPackInfoFeatureAction() {/* no instances */}
}
