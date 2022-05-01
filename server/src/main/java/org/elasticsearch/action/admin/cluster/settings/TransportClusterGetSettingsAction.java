/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportClusterGetSettingsAction extends TransportMasterNodeReadAction<
    ClusterGetSettingsRequest,
    ClusterGetSettingsResponse> {

    private final SettingsFilter settingsFilter;

    @Inject
    public TransportClusterGetSettingsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        SettingsFilter settingsFilter,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ClusterGetSettingsAction.NAME,
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterGetSettingsRequest::new,
            indexNameExpressionResolver,
            ClusterGetSettingsResponse::new,
            ThreadPool.Names.SAME
        );
        this.settingsFilter = settingsFilter;
    }

    @Override
    protected void masterOperation(
        Task task,
        ClusterGetSettingsRequest request,
        ClusterState state,
        ActionListener<ClusterGetSettingsResponse> listener
    ) throws Exception {
        final var metadata = state.metadata();
        listener.onResponse(
            new ClusterGetSettingsResponse(
                settingsFilter.filter(metadata.persistentSettings()),
                settingsFilter.filter(metadata.transientSettings()),
                settingsFilter.filter(metadata.settings())
            )
        );
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterGetSettingsRequest request, ClusterState state) {
        // We need to expose the cluster settings regardless of blocks, or else users won't be able to determine which settings to change to
        // remove the blocks.
        return null;
    }
}
