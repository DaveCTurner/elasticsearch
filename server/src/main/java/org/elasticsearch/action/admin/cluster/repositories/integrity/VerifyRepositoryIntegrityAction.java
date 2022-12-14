/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.repositories.integrity;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class VerifyRepositoryIntegrityAction extends ActionType<VerifyRepositoryIntegrityAction.Response> {

    public static final VerifyRepositoryIntegrityAction INSTANCE = new VerifyRepositoryIntegrityAction();
    public static final String NAME = "cluster:admin/repository/verify_integrity";

    private VerifyRepositoryIntegrityAction() {
        super(NAME, VerifyRepositoryIntegrityAction.Response::new);
    }

    public static class Request extends MasterNodeReadRequest<Request> {

        private final String repository;

        public Request(String repository) {
            this.repository = repository;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.repository = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(repository);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }

        public int getThreadpoolConcurrency() {
            return 5;
        }

        public int getSnapshotVerificationConcurrency() {
            return 5;
        }

        public int getIndexVerificationConcurrency() {
            return 5;
        }

        public int getIndexSnapshotVerificationConcurrency() {
            return 5;
        }

        public int getMaxFailures() {
            return 10000;
        }
    }

    public static class Response extends ActionResponse implements ChunkedToXContent {

        private final List<RepositoryVerificationException> exceptions;

        public Response(List<RepositoryVerificationException> exceptions) {
            this.exceptions = exceptions;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.exceptions = in.readList(RepositoryVerificationException::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(exceptions);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params outerParams) {
            return Iterators.concat(
                Iterators.single((builder, params) -> builder.startObject().startArray("errors")),
                exceptions.stream().<ToXContent>map(e -> (builder, params) -> {
                    builder.startObject();
                    ElasticsearchException.generateFailureXContent(builder, params, e, true);
                    return builder.endObject();
                }).iterator(),
                Iterators.single((builder, params) -> builder.endArray().endObject())
            );
        }

        @Override
        public RestStatus getRestStatus() {
            if (exceptions.isEmpty()) {
                return RestStatus.OK;
            } else {
                return RestStatus.INTERNAL_SERVER_ERROR;
            }
        }

        public List<RepositoryVerificationException> getExceptions() {
            return exceptions;
        }
    }

    public static class TransportAction extends TransportMasterNodeReadAction<Request, Response> {

        private final RepositoriesService repositoriesService;

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            RepositoriesService repositoriesService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver
        ) {
            super(
                NAME,
                true,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                Request::new,
                indexNameExpressionResolver,
                Response::new,
                ThreadPool.Names.SNAPSHOT_META
            );
            this.repositoriesService = repositoriesService;
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
        }

        @Override
        protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
            // TODO add mechanism to block blob deletions while this is running
            final var cancellableTask = (CancellableTask) task;
            repositoriesService.repository(request.repository)
                .verifyMetadataIntegrity(request, listener.map(Response::new), cancellableTask::isCancelled);
        }
    }
}
