/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.realm;

import org.elasticsearch.action.UnnecessaryActionTypeSubclass;

public class ClearRealmCacheAction extends UnnecessaryActionTypeSubclass<ClearRealmCacheResponse> {

    public static final ClearRealmCacheAction INSTANCE = new ClearRealmCacheAction();
    public static final String NAME = "cluster:admin/xpack/security/realm/cache/clear";

    protected ClearRealmCacheAction() {
        super(NAME);
    }
}
