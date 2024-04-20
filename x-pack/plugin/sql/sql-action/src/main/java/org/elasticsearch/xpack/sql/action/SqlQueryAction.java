/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.action.UnnecessaryActionTypeSubclass;

public class SqlQueryAction extends UnnecessaryActionTypeSubclass<SqlQueryResponse> {

    public static final SqlQueryAction INSTANCE = new SqlQueryAction();
    public static final String NAME = "indices:data/read/sql";

    private SqlQueryAction() {
        super(NAME);
    }
}
