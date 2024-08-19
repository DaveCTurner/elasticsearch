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
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.ThrottledIterator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;

public class TransportRepositoryVerifyIntegrityMasterNodeAction extends TransportMasterNodeAction<
    TransportRepositoryVerifyIntegrityMasterNodeAction.Request,
    ActionResponse.Empty> {

    static final String MASTER_ACTION_NAME = TransportRepositoryVerifyIntegrityCoordinationAction.INSTANCE.name() + "[m]";

    TransportRepositoryVerifyIntegrityMasterNodeAction(
        TransportService transportService,
        ClusterService clusterService,
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
            in -> ActionResponse.Empty.INSTANCE,
            executor
        );
    }

    @Override
    protected ClusterBlockException checkBlock(TransportRepositoryVerifyIntegrityMasterNodeAction.Request request, ClusterState state) {
        return null;
    }

    @Override
    protected void masterOperation(
        Task task,
        TransportRepositoryVerifyIntegrityMasterNodeAction.Request request,
        ClusterState state,
        ActionListener<ActionResponse.Empty> listener
    ) {
        final var cancellableTask = (CancellableTask) task;
        try (var listeners = new RefCountingListener(listener.map(v -> {
            cancellableTask.ensureNotCancelled();
            return ActionResponse.Empty.INSTANCE;
        }))) {
            final var completionListener = listeners.acquire();
            ThrottledIterator.run(
                Iterators.failFast(
                    Iterators.forRange(0, 20, id -> new TransportRepositoryVerifyIntegritySnapshotChunkAction.Request(request.taskId, id)),
                    () -> cancellableTask.isCancelled() || listeners.isFailing()
                ),
                (ref, req) -> transportService.sendChildRequest(
                    request.coordinatingNode,
                    TransportRepositoryVerifyIntegritySnapshotChunkAction.SNAPSHOT_CHUNK_ACTION_NAME,
                    req,
                    task,
                    TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<>(
                        ActionListener.releaseAfter(listeners.acquire(response -> {}), ref),
                        in -> ActionResponse.Empty.INSTANCE,
                        executor
                    )
                ),
                5,
                () -> {},
                () -> completionListener.onResponse(null)
            );
        }
    }

    public static class Request extends MasterNodeRequest<Request> {
        private final DiscoveryNode coordinatingNode;
        private final long taskId;
        private final String repositoryName;

        Request(TimeValue masterNodeTimeout, DiscoveryNode coordinatingNode, long taskId, String repositoryName) {
            super(masterNodeTimeout);
            this.coordinatingNode = coordinatingNode;
            this.taskId = taskId;
            this.repositoryName = Objects.requireNonNull(repositoryName);
        }

        Request(StreamInput in) throws IOException {
            super(in);
            coordinatingNode = new DiscoveryNode(in);
            taskId = in.readVLong();
            repositoryName = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            coordinatingNode.writeTo(out);
            out.writeVLong(taskId);
            out.writeString(repositoryName);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }
    }
}
