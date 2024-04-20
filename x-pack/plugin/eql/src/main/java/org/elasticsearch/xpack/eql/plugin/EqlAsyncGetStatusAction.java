/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.plugin;

import org.elasticsearch.action.UnnecessaryActionTypeSubclass;
import org.elasticsearch.xpack.ql.async.QlStatusResponse;

public class EqlAsyncGetStatusAction extends UnnecessaryActionTypeSubclass<QlStatusResponse> {
    public static final EqlAsyncGetStatusAction INSTANCE = new EqlAsyncGetStatusAction();
    public static final String NAME = "cluster:monitor/eql/async/status";

    private EqlAsyncGetStatusAction() {
        super(NAME);
    }
}
