/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.delete;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Transport action for delete snapshot operation
 */
public class TransportDeleteSnapshotAction extends AcknowledgedTransportMasterNodeAction<DeleteSnapshotRequest> {
    public static final ActionType<AcknowledgedResponse> TYPE = new ActionType<>("cluster:admin/snapshot/delete");
    private final SnapshotsService snapshotsService;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportDeleteSnapshotAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        SnapshotsService snapshotsService,
        ActionFilters actionFilters,
        ProjectResolver projectResolver
    ) {
        super(
            TYPE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteSnapshotRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.snapshotsService = snapshotsService;
        this.projectResolver = projectResolver;
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteSnapshotRequest request, ClusterState state) {
        // Cluster is not affected but we look up repositories in metadata
        return state.blocks().globalBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void doExecute(Task task, DeleteSnapshotRequest request, ActionListener<AcknowledgedResponse> listener) {
        if (clusterService.state().getMinTransportVersion().before(TransportVersions.V_8_15_0) && request.waitForCompletion() == false) {
            throw new UnsupportedOperationException("wait_for_completion parameter is not supported by all nodes in this cluster");
        }
        super.doExecute(task, request, listener);
    }

    @Override
    protected void masterOperation(
        Task task,
        final DeleteSnapshotRequest request,
        ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        snapshotsService.deleteSnapshots(projectResolver.getProjectId(), request, listener.map(v -> AcknowledgedResponse.TRUE));
    }
}
