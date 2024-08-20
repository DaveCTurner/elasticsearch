/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.concurrent.Executor;

public class TransportRepositoryVerifyIntegrityCoordinationAction extends TransportAction<
    TransportRepositoryVerifyIntegrityCoordinationAction.Request,
    ActionResponse.Empty> {

    public static final ActionType<ActionResponse.Empty> INSTANCE = new ActionType<>("cluster:admin/repository/verify_integrity");

    private final OngoingRequests ongoingRequests = new OngoingRequests();

    private final TransportService transportService;
    private final Executor managementExecutor;

    public static class Request extends ActionRequest {
        private final TimeValue masterNodeTimeout;
        private final RepositoryVerifyIntegrityParams requestParams;
        private final RepositoryVerifyIntegrityResponseBuilder responseBuilder;

        public Request(
            TimeValue masterNodeTimeout,
            RepositoryVerifyIntegrityParams requestParams,
            RepositoryVerifyIntegrityResponseBuilder responseBuilder
        ) {
            this.masterNodeTimeout = masterNodeTimeout;
            this.requestParams = requestParams;
            this.responseBuilder = responseBuilder;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public TimeValue masterNodeTimeout() {
            return masterNodeTimeout;
        }

        public RepositoryVerifyIntegrityParams requestParams() {
            return requestParams;
        }

        public RepositoryVerifyIntegrityResponseBuilder responseBuilder() {
            return responseBuilder;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }
    }

    @Inject
    public TransportRepositoryVerifyIntegrityCoordinationAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            INSTANCE.name(),
            actionFilters,
            transportService.getTaskManager(),
            transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT)
        );

        this.transportService = transportService;
        this.managementExecutor = transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT);

        // register subsidiary actions
        new TransportRepositoryVerifyIntegrityMasterNodeAction(
            transportService,
            clusterService,
            actionFilters,
            indexNameExpressionResolver,
            managementExecutor
        );

        new TransportRepositoryVerifyIntegritySnapshotChunkAction(transportService, actionFilters, managementExecutor, ongoingRequests);
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<ActionResponse.Empty> listener) {
        final var responseBuilder = request.responseBuilder();
        ActionListener.run(
            ActionListener.releaseAfter(listener, ongoingRequests.registerResponseBuilder(task.getId(), responseBuilder)),
            l -> {

                // TODO explicit mode changes for response builder, triggered by master node, avoiding concurrency issues

                responseBuilder.writeFragment(
                    p0 -> ChunkedToXContentHelper.singleChunk((b, p) -> b.startObject().startArray("snapshots")),
                    () -> {}
                );

                transportService.sendChildRequest(
                    transportService.getLocalNodeConnection(),
                    TransportRepositoryVerifyIntegrityMasterNodeAction.MASTER_ACTION_NAME,
                    new TransportRepositoryVerifyIntegrityMasterNodeAction.Request(
                        request.masterNodeTimeout(),
                        transportService.getLocalNode(),
                        task.getId(),
                        request.requestParams()
                    ),
                    task,
                    TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<>(
                        // TODO if completed exceptionally, render the exception in the response
                        ActionListener.runBefore(
                            l,
                            () -> responseBuilder.writeFragment(
                                p0 -> ChunkedToXContentHelper.singleChunk((b, p) -> b.endArray().endObject()),
                                () -> {}
                            )
                        ),
                        in -> ActionResponse.Empty.INSTANCE,
                        managementExecutor
                    )
                );
            }
        );
    }
}
