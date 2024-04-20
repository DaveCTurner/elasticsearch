/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.oidc;

import org.elasticsearch.action.UnnecessaryActionTypeSubclass;

/**
 * ActionType for initiating an authentication process using OpenID Connect
 */
public final class OpenIdConnectAuthenticateAction extends UnnecessaryActionTypeSubclass<OpenIdConnectAuthenticateResponse> {

    public static final OpenIdConnectAuthenticateAction INSTANCE = new OpenIdConnectAuthenticateAction();
    public static final String NAME = "cluster:admin/xpack/security/oidc/authenticate";

    private OpenIdConnectAuthenticateAction() {
        super(NAME);
    }

}
