/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.UnnecessaryActionTypeSubclass;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;

public class XPackInfoAction {

    public static final String NAME = "cluster:monitor/xpack/info";
    public static final UnnecessaryActionTypeSubclass<XPackInfoResponse> INSTANCE = new UnnecessaryActionTypeSubclass<>(NAME);
    public static final RemoteClusterActionType<XPackInfoResponse> REMOTE_TYPE = new RemoteClusterActionType<>(
        NAME,
        XPackInfoResponse::new
    );

    private XPackInfoAction() {/* no instances */}
}
