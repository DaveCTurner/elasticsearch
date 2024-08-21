/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;

public class TransportRepositoryVerifyIntegrityMasterNodeAction extends TransportMasterNodeAction<
    TransportRepositoryVerifyIntegrityMasterNodeAction.Request,
    TransportRepositoryVerifyIntegrityMasterNodeAction.Response> {

    static final String MASTER_ACTION_NAME = TransportRepositoryVerifyIntegrityCoordinationAction.INSTANCE.name() + "[m]";
    private final RepositoriesService repositoriesService;

    TransportRepositoryVerifyIntegrityMasterNodeAction(
        TransportService transportService,
        ClusterService clusterService,
        RepositoriesService repositoriesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Executor executor
    ) {
        super(
            MASTER_ACTION_NAME,
            transportService,
            clusterService,
            transportService.getThreadPool(),
            actionFilters,
            TransportRepositoryVerifyIntegrityMasterNodeAction.Request::new,
            indexNameExpressionResolver,
            TransportRepositoryVerifyIntegrityMasterNodeAction.Response::new,
            executor
        );
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected ClusterBlockException checkBlock(TransportRepositoryVerifyIntegrityMasterNodeAction.Request request, ClusterState state) {
        return null;
    }

    @Override
    protected void masterOperation(
        Task rawTask,
        TransportRepositoryVerifyIntegrityMasterNodeAction.Request request,
        ClusterState state,
        ActionListener<TransportRepositoryVerifyIntegrityMasterNodeAction.Response> listener
    ) {
        final var repository = (BlobStoreRepository) repositoriesService.repository(request.requestParams.repository());
        final var responseWriter = new ResponseWriter() {
            @Override
            public void writeResponseChunk(ResponseChunk responseChunk, Releasable releasable) {
                transportService.sendChildRequest(
                    request.coordinatingNode,
                    TransportRepositoryVerifyIntegrityResponseChunkAction.SNAPSHOT_CHUNK_ACTION_NAME,
                    new TransportRepositoryVerifyIntegrityResponseChunkAction.Request(request.taskId, responseChunk),
                    rawTask,
                    TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<TransportResponse>(
                        // TODO add failure handling
                        ActionListener.releasing(releasable),
                        in -> ActionResponse.Empty.INSTANCE,
                        executor
                    )
                );
            }
        };

        final var task = (RepositoryVerifyIntegrityTask) rawTask;
        MetadataVerifier.run(
            repository,
            responseWriter,
            request.requestParams.withResolvedDefaults(repository.threadPool().info(ThreadPool.Names.SNAPSHOT_META)),
            new CancellableThreads(),
            task,
            listener.map(Response::new)
        );
    }

    public static class Request extends MasterNodeRequest<Request> {
        private final DiscoveryNode coordinatingNode;
        private final long taskId;
        private final RepositoryVerifyIntegrityParams requestParams;

        Request(TimeValue masterNodeTimeout, DiscoveryNode coordinatingNode, long taskId, RepositoryVerifyIntegrityParams requestParams) {
            super(masterNodeTimeout);
            this.coordinatingNode = coordinatingNode;
            this.taskId = taskId;
            this.requestParams = Objects.requireNonNull(requestParams);
        }

        Request(StreamInput in) throws IOException {
            super(in);
            coordinatingNode = new DiscoveryNode(in);
            taskId = in.readVLong();
            requestParams = new RepositoryVerifyIntegrityParams(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            coordinatingNode.writeTo(out);
            out.writeVLong(taskId);
            requestParams.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new RepositoryVerifyIntegrityTask(id, type, action, getDescription(), parentTaskId, headers);
        }
    }

    public static class Response extends ActionResponse {

        private final MetadataVerifier.VerificationResult verificationResult;

        Response(MetadataVerifier.VerificationResult verificationResult) {
            this.verificationResult = Objects.requireNonNull(verificationResult);
        }

        Response(StreamInput in) throws IOException {
            verificationResult = new MetadataVerifier.VerificationResult(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            verificationResult.writeTo(out);
        }
    }
}
