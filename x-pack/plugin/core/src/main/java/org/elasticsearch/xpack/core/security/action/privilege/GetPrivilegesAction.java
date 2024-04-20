/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.action.UnnecessaryActionTypeSubclass;

/**
 * ActionType for retrieving one or more application privileges from the security index
 */
public final class GetPrivilegesAction extends UnnecessaryActionTypeSubclass<GetPrivilegesResponse> {

    public static final GetPrivilegesAction INSTANCE = new GetPrivilegesAction();
    public static final String NAME = "cluster:admin/xpack/security/privilege/get";

    private GetPrivilegesAction() {
        super(NAME);
    }
}
