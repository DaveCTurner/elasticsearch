/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm.action;

import org.elasticsearch.action.UnnecessaryActionTypeSubclass;
import org.elasticsearch.xpack.core.ilm.ExplainLifecycleResponse;

public class ExplainLifecycleAction extends UnnecessaryActionTypeSubclass<ExplainLifecycleResponse> {
    public static final ExplainLifecycleAction INSTANCE = new ExplainLifecycleAction();
    public static final String NAME = "indices:admin/ilm/explain";

    protected ExplainLifecycleAction() {
        super(NAME);
    }

}
