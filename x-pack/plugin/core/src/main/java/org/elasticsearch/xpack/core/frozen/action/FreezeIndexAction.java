/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.frozen.action;

import org.elasticsearch.action.UnnecessaryActionTypeSubclass;
import org.elasticsearch.protocol.xpack.frozen.FreezeResponse;

public class FreezeIndexAction extends UnnecessaryActionTypeSubclass<FreezeResponse> {

    public static final FreezeIndexAction INSTANCE = new FreezeIndexAction();
    public static final String NAME = "indices:admin/freeze";

    private FreezeIndexAction() {
        super(NAME);
    }
}
