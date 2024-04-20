/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.searchablesnapshots;

import org.elasticsearch.action.UnnecessaryActionTypeSubclass;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;

public class MountSearchableSnapshotAction extends UnnecessaryActionTypeSubclass<RestoreSnapshotResponse> {

    public static final MountSearchableSnapshotAction INSTANCE = new MountSearchableSnapshotAction();
    public static final String NAME = "cluster:admin/snapshot/mount";

    private MountSearchableSnapshotAction() {
        super(NAME);
    }
}
