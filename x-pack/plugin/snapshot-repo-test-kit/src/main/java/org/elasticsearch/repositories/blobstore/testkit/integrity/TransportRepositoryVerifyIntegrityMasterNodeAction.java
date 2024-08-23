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
import org.elasticsearch.action.support.SubscribableListener;
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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
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
    RepositoryVerifyIntegrityResponse> {

    static final String ACTION_NAME = TransportRepositoryVerifyIntegrityCoordinationAction.INSTANCE.name() + "[m]";
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
            ACTION_NAME,
            transportService,
            clusterService,
            transportService.getThreadPool(),
            actionFilters,
            TransportRepositoryVerifyIntegrityMasterNodeAction.Request::new,
            indexNameExpressionResolver,
            RepositoryVerifyIntegrityResponse::new,
            executor
        );
        this.repositoriesService = repositoriesService;
    }

    public static class Request extends MasterNodeRequest<Request> {
        private final DiscoveryNode coordinatingNode;
        private final long coordinatingTaskId;
        private final RepositoryVerifyIntegrityParams requestParams;

        Request(
            TimeValue masterNodeTimeout,
            DiscoveryNode coordinatingNode,
            long coordinatingTaskId,
            RepositoryVerifyIntegrityParams requestParams
        ) {
            super(masterNodeTimeout);
            this.coordinatingNode = coordinatingNode;
            this.coordinatingTaskId = coordinatingTaskId;
            this.requestParams = Objects.requireNonNull(requestParams);
        }

        Request(StreamInput in) throws IOException {
            super(in);
            coordinatingNode = new DiscoveryNode(in);
            coordinatingTaskId = in.readVLong();
            requestParams = new RepositoryVerifyIntegrityParams(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            coordinatingNode.writeTo(out);
            out.writeVLong(coordinatingTaskId);
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

    @Override
    protected ClusterBlockException checkBlock(TransportRepositoryVerifyIntegrityMasterNodeAction.Request request, ClusterState state) {
        return null;
    }

    @Override
    protected void masterOperation(
        Task rawTask,
        TransportRepositoryVerifyIntegrityMasterNodeAction.Request request,
        ClusterState state,
        ActionListener<RepositoryVerifyIntegrityResponse> listener
    ) {
        final var responseWriter = new RepositoryVerifyIntegrityResponseChunk.Writer() {
            @Override
            public void writeResponseChunk(RepositoryVerifyIntegrityResponseChunk responseChunk, ActionListener<Void> listener) {
                transportService.sendChildRequest(
                    request.coordinatingNode,
                    TransportRepositoryVerifyIntegrityResponseChunkAction.ACTION_NAME,
                    new TransportRepositoryVerifyIntegrityResponseChunkAction.Request(request.coordinatingTaskId, responseChunk),
                    rawTask,
                    TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<TransportResponse>(
                        listener.map(ignored -> null),
                        in -> ActionResponse.Empty.INSTANCE,
                        executor
                    )
                );
            }
        };

        final var repository = (BlobStoreRepository) repositoriesService.repository(request.requestParams.repository());
        final var task = (RepositoryVerifyIntegrityTask) rawTask;

        SubscribableListener

            .<RepositoryData>newForked(l -> repository.getRepositoryData(executor, l))
            .andThenApply(repositoryData -> {
                final var cancellableThreads = new CancellableThreads();
                task.addListener(() -> cancellableThreads.cancel("task cancelled"));
                final var verifier = new RepositoryIntegrityVerifier(
                    repository,
                    responseWriter,
                    request.requestParams.withResolvedDefaults(repository.threadPool().info(ThreadPool.Names.SNAPSHOT_META)),
                    repositoryData,
                    cancellableThreads
                );
                task.setStatusSupplier(verifier::getStatus);
                return verifier;
            })
            .<RepositoryIntegrityVerifier>andThen((l, repositoryIntegrityVerifier) -> {
                ActionListener<Void> listener1 = l.map(ignored -> repositoryIntegrityVerifier);
                new RepositoryVerifyIntegrityResponseChunk.Builder(
                    responseWriter,
                    RepositoryVerifyIntegrityResponseChunk.Type.START_RESPONSE
                ).write(listener1);
            })
            .<RepositoryVerifyIntegrityResponse>andThen((l, repositoryIntegrityVerifier) -> repositoryIntegrityVerifier.start(l))
            .addListener(listener);
    }

}
