/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.search.action;

import org.elasticsearch.action.UnnecessaryActionTypeSubclass;

public class GetAsyncSearchAction extends UnnecessaryActionTypeSubclass<AsyncSearchResponse> {
    public static final GetAsyncSearchAction INSTANCE = new GetAsyncSearchAction();
    public static final String NAME = "indices:data/read/async_search/get";

    private GetAsyncSearchAction() {
        super(NAME);
    }
}
