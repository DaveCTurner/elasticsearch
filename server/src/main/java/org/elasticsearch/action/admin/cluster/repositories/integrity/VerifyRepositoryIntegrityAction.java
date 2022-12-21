/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.repositories.integrity;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

public class VerifyRepositoryIntegrityAction extends ActionType<ActionResponse.Empty> {

    public static final VerifyRepositoryIntegrityAction INSTANCE = new VerifyRepositoryIntegrityAction();
    public static final String NAME = "cluster:admin/repository/verify_integrity";

    private VerifyRepositoryIntegrityAction() {
        super(NAME, in -> ActionResponse.Empty.INSTANCE);
    }

    public static class Request extends MasterNodeReadRequest<Request> {

        private final String repository;
        private final String resultsIndex;
        private final int metaThreadPoolConcurrency;
        private final int blobThreadPoolConcurrency;
        private final int snapshotVerificationConcurrency;
        private final int indexVerificationConcurrency;
        private final int indexSnapshotVerificationConcurrency;
        private final boolean verifyBlobContents;
        private final ByteSizeValue maxBytesPerSec;

        public Request(
            String repository,
            String resultsIndex,
            int metaThreadPoolConcurrency,
            int blobThreadPoolConcurrency,
            int snapshotVerificationConcurrency,
            int indexVerificationConcurrency,
            int indexSnapshotVerificationConcurrency,
            boolean verifyBlobContents,
            ByteSizeValue maxBytesPerSec
        ) {
            this.repository = Objects.requireNonNull(repository, "repository");
            this.resultsIndex = Objects.requireNonNull(resultsIndex, "resultsIndex");
            this.metaThreadPoolConcurrency = requireNonNegative("metaThreadPoolConcurrency", metaThreadPoolConcurrency);
            this.blobThreadPoolConcurrency = requireNonNegative("blobThreadPoolConcurrency", blobThreadPoolConcurrency);
            this.snapshotVerificationConcurrency = requireNonNegative("snapshotVerificationConcurrency", snapshotVerificationConcurrency);
            this.indexVerificationConcurrency = requireNonNegative("indexVerificationConcurrency", indexVerificationConcurrency);
            this.indexSnapshotVerificationConcurrency = requireNonNegative(
                "indexSnapshotVerificationConcurrency",
                indexSnapshotVerificationConcurrency
            );
            this.verifyBlobContents = verifyBlobContents;
            if (maxBytesPerSec.getBytes() < 1) {
                throw new IllegalArgumentException("invalid rate limit");
            }
            this.maxBytesPerSec = maxBytesPerSec;
        }

        private static int requireNonNegative(String name, int value) {
            if (value < 0) {
                throw new IllegalArgumentException("argument [" + name + "] must be at least [0]");
            }
            return value;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.repository = in.readString();
            this.resultsIndex = in.readString();
            this.metaThreadPoolConcurrency = in.readVInt();
            this.blobThreadPoolConcurrency = in.readVInt();
            this.snapshotVerificationConcurrency = in.readVInt();
            this.indexVerificationConcurrency = in.readVInt();
            this.indexSnapshotVerificationConcurrency = in.readVInt();
            this.verifyBlobContents = in.readBoolean();
            this.maxBytesPerSec = ByteSizeValue.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(repository);
            out.writeString(resultsIndex);
            out.writeVInt(metaThreadPoolConcurrency);
            out.writeVInt(blobThreadPoolConcurrency);
            out.writeVInt(snapshotVerificationConcurrency);
            out.writeVInt(indexVerificationConcurrency);
            out.writeVInt(indexSnapshotVerificationConcurrency);
            out.writeBoolean(verifyBlobContents);
            maxBytesPerSec.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new VerifyRepositoryIntegrityAction.Task(id, type, action, getDescription(), parentTaskId, headers);
        }

        @Override
        public String getDescription() {
            return Strings.format("verify metadata integrity of repository [%s]", repository);
        }

        public String getRepository() {
            return repository;
        }

        public String getResultsIndex() {
            return resultsIndex;
        }

        public int getMetaThreadPoolConcurrency() {
            return metaThreadPoolConcurrency;
        }

        public int getBlobThreadPoolConcurrency() {
            return blobThreadPoolConcurrency;
        }

        public int getSnapshotVerificationConcurrency() {
            return snapshotVerificationConcurrency;
        }

        public int getIndexVerificationConcurrency() {
            return indexVerificationConcurrency;
        }

        public int getIndexSnapshotVerificationConcurrency() {
            return indexSnapshotVerificationConcurrency;
        }

        public boolean getVerifyBlobContents() {
            return verifyBlobContents;
        }

        public ByteSizeValue getMaxBytesPerSec() {
            return maxBytesPerSec;
        }

        public Request withResolvedDefaults(long currentTimeMillis, ThreadPool.Info threadPoolInfo) {
            if (org.elasticsearch.common.Strings.isNullOrBlank(resultsIndex) == false
                && metaThreadPoolConcurrency > 0
                && blobThreadPoolConcurrency > 0
                && snapshotVerificationConcurrency > 0
                && indexVerificationConcurrency > 0
                && indexSnapshotVerificationConcurrency > 0) {
                return this;
            }

            final var maxThreads = Math.max(1, threadPoolInfo.getMax());
            final var halfMaxThreads = Math.max(1, maxThreads / 2);
            final var request = new Request(
                repository,
                org.elasticsearch.common.Strings.isNullOrBlank(resultsIndex)
                    ? ("repository-metadata-verification-" + repository + "-" + currentTimeMillis)
                    : resultsIndex,
                metaThreadPoolConcurrency > 0 ? metaThreadPoolConcurrency : halfMaxThreads,
                blobThreadPoolConcurrency > 0 ? blobThreadPoolConcurrency : 1,
                snapshotVerificationConcurrency > 0 ? snapshotVerificationConcurrency : halfMaxThreads,
                indexVerificationConcurrency > 0 ? indexVerificationConcurrency : maxThreads,
                indexSnapshotVerificationConcurrency > 0 ? indexSnapshotVerificationConcurrency : 1,
                verifyBlobContents,
                maxBytesPerSec
            );
            request.masterNodeTimeout(masterNodeTimeout());
            return request;
        }
    }

    public record Status(
        String repositoryName,
        long repositoryGeneration,
        String repositoryUUID,
        long snapshotCount,
        long snapshotsVerified,
        long indexCount,
        long indicesVerified,
        long indexSnapshotCount,
        long indexSnapshotsVerified,
        long blobsVerified,
        long blobBytesVerified,
        long throttledNanos,
        long anomalyCount,
        String resultsIndex
    ) implements org.elasticsearch.tasks.Task.Status {

        public static String NAME = "verify_repository_status";

        public Status(StreamInput in) throws IOException {
            this(
                in.readString(),
                in.readVLong(),
                in.readString(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readString()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(repositoryName);
            out.writeVLong(repositoryGeneration);
            out.writeString(repositoryUUID);
            out.writeVLong(snapshotCount);
            out.writeVLong(snapshotsVerified);
            out.writeVLong(indexCount);
            out.writeVLong(indicesVerified);
            out.writeVLong(indexSnapshotCount);
            out.writeVLong(indexSnapshotsVerified);
            out.writeVLong(blobsVerified);
            out.writeVLong(blobBytesVerified);
            out.writeVLong(throttledNanos);
            out.writeVLong(anomalyCount);
            out.writeString(resultsIndex);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject("repository");
            builder.field("name", repositoryName);
            builder.field("uuid", repositoryUUID);
            builder.field("generation", repositoryGeneration);
            builder.endObject();
            builder.startObject("snapshots");
            builder.field("verified", snapshotsVerified);
            builder.field("total", snapshotCount);
            builder.endObject();
            builder.startObject("indices");
            builder.field("verified", indicesVerified);
            builder.field("total", indexCount);
            builder.endObject();
            builder.startObject("index_snapshots");
            builder.field("verified", indexSnapshotsVerified);
            builder.field("total", indexSnapshotCount);
            builder.endObject();
            builder.startObject("blobs");
            builder.field("verified", blobsVerified);
            if (blobBytesVerified > 0) {
                builder.humanReadableField("verified_size_in_bytes", "verified_size", ByteSizeValue.ofBytes(blobBytesVerified));
                builder.humanReadableField("throttled_time_in_millis", "throttled_time", TimeValue.timeValueNanos(throttledNanos));
            }
            builder.endObject();
            builder.field("anomalies", anomalyCount);
            builder.field("results_index", resultsIndex);
            builder.endObject();
            return builder;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }

    public static class Task extends CancellableTask {

        private volatile Supplier<VerifyRepositoryIntegrityAction.Status> statusSupplier;

        public Task(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
            super(id, type, action, description, parentTaskId, headers);
        }

        public void setStatusSupplier(Supplier<VerifyRepositoryIntegrityAction.Status> statusSupplier) {
            this.statusSupplier = statusSupplier;
        }

        @Override
        public Status getStatus() {
            return Optional.ofNullable(statusSupplier).map(Supplier::get).orElse(null);
        }
    }

    public static class TransportAction extends TransportMasterNodeReadAction<Request, ActionResponse.Empty> {

        private final RepositoriesService repositoriesService;
        private final NodeClient client;

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            RepositoriesService repositoriesService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            NodeClient client
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
                in -> ActionResponse.Empty.INSTANCE,
                ThreadPool.Names.SNAPSHOT_META
            );
            this.repositoriesService = repositoriesService;
            this.client = client;
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
        }

        @Override
        protected void masterOperation(
            org.elasticsearch.tasks.Task task,
            Request request,
            ClusterState state,
            ActionListener<ActionResponse.Empty> listener
        ) throws Exception {
            // TODO add mechanism to block blob deletions while this is running
            final var verifyTask = (Task) task;

            final ClusterStateListener noLongerMasterListener = event -> {
                if (event.localNodeMaster() == false) {
                    transportService.getTaskManager().cancel(verifyTask, "no longer master", () -> {});
                }
            };
            clusterService.addListener(noLongerMasterListener);

            repositoriesService.repository(request.getRepository())
                .verifyMetadataIntegrity(
                    client,
                    transportService::newNetworkBytesStream,
                    request.withResolvedDefaults(
                        clusterService.threadPool().absoluteTimeInMillis(),
                        clusterService.threadPool().info(ThreadPool.Names.SNAPSHOT_META)
                    ),
                    ActionListener.runAfter(
                        listener.map(ignored -> ActionResponse.Empty.INSTANCE),
                        () -> clusterService.removeListener(noLongerMasterListener)
                    ),
                    verifyTask::isCancelled,
                    verifyTask::setStatusSupplier
                );
        }
    }
}
