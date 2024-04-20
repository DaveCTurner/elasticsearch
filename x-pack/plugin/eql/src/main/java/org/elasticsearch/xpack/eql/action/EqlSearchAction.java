/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.action.UnnecessaryActionTypeSubclass;

public class EqlSearchAction extends UnnecessaryActionTypeSubclass<EqlSearchResponse> {
    public static final EqlSearchAction INSTANCE = new EqlSearchAction();
    public static final String NAME = "indices:data/read/eql";

    private EqlSearchAction() {
        super(NAME);
    }
}
