/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.profile;

import org.elasticsearch.action.UnnecessaryActionTypeSubclass;
import org.elasticsearch.action.support.master.AcknowledgedResponse;

public class UpdateProfileDataAction extends UnnecessaryActionTypeSubclass<AcknowledgedResponse> {

    public static final String NAME = "cluster:admin/xpack/security/profile/put/data";
    public static final UpdateProfileDataAction INSTANCE = new UpdateProfileDataAction();

    public UpdateProfileDataAction() {
        super(NAME);
    }
}
