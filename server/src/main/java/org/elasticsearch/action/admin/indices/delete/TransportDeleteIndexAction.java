/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.delete;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataDeleteIndexService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.Index;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Delete index action.
 */
public class TransportDeleteIndexAction extends AcknowledgedTransportMasterNodeAction<DeleteIndexRequest> {

    public static final ActionType<AcknowledgedResponse> TYPE = new ActionType<>("indices:admin/delete");
    private static final Logger logger = LogManager.getLogger(TransportDeleteIndexAction.class);

    private final MetadataDeleteIndexService deleteIndexService;
    private final ProjectResolver projectResolver;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final DestructiveOperations destructiveOperations;

    @Inject
    public TransportDeleteIndexAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataDeleteIndexService deleteIndexService,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DestructiveOperations destructiveOperations
    ) {
        super(
            TYPE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteIndexRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.deleteIndexService = deleteIndexService;
        this.projectResolver = projectResolver;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.destructiveOperations = destructiveOperations;
    }

    @Override
    protected void doExecute(Task task, DeleteIndexRequest request, ActionListener<AcknowledgedResponse> listener) {
        destructiveOperations.failDestructive(request.indices());
        super.doExecute(task, request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteIndexRequest request, ClusterState state) {
        final ProjectId projectId = projectResolver.getProjectId();
        return state.blocks()
            .indicesAllowReleaseResources(
                projectId,
                indexNameExpressionResolver.concreteIndexNames(state.metadata().getProject(projectId), request)
            );
    }

    @Override
    protected void masterOperation(
        Task task,
        final DeleteIndexRequest request,
        final ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        final Set<Index> concreteIndices = new HashSet<>(Arrays.asList(indexNameExpressionResolver.concreteIndices(state, request)));
        if (concreteIndices.isEmpty()) {
            listener.onResponse(AcknowledgedResponse.TRUE);
            return;
        }

        deleteIndexService.deleteIndices(
            request.masterNodeTimeout(),
            request.ackTimeout(),
            concreteIndices,
            listener.delegateResponse((l, e) -> {
                logger.debug(() -> "failed to delete indices [" + concreteIndices + "]", e);
                listener.onFailure(e);
            })
        );
    }
}
