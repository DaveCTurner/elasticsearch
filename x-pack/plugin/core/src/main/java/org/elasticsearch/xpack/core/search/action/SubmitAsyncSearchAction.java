/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.search.action;

import org.elasticsearch.action.UnnecessaryActionTypeSubclass;

public final class SubmitAsyncSearchAction extends UnnecessaryActionTypeSubclass<AsyncSearchResponse> {
    public static final SubmitAsyncSearchAction INSTANCE = new SubmitAsyncSearchAction();
    public static final String NAME = "indices:data/read/async_search/submit";

    private SubmitAsyncSearchAction() {
        super(NAME);
    }
}
