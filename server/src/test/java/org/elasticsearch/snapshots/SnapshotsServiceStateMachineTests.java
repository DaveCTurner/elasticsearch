/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.FinalizeSnapshotContext;
import org.elasticsearch.repositories.GetSnapshotInfoContext;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryShardId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.transport.CloseableConnection;
import org.elasticsearch.transport.ClusterConnectionManager;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportMessageListener;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;

@TestLogging(reason = "nocommit", value = "org.elasticsearch.common.util.concurrent.DeterministicTaskQueue:TRACE")
public class SnapshotsServiceStateMachineTests extends ESTestCase {

    public void testFoo() {

        final var settings = Settings.EMPTY;
        final var clusterSettings = ClusterSettings.createBuiltInClusterSettings(settings);

        final var deterministicTaskQueue = new DeterministicTaskQueue();
        deterministicTaskQueue.setExecutionDelayVariabilityMillis(1000);
        final var threadPool = deterministicTaskQueue.getThreadPool();
        final var threadContext = threadPool.getThreadContext();

        final var localNode = DiscoveryNodeUtils.create("local");

        final var transport = new StubbableTransport(new MockTransport());
        final var transportService = new TransportService(
            settings,
            transport,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            ignored -> localNode,
            clusterSettings,
            new ClusterConnectionManager(settings, transport, threadContext),
            new TaskManager(settings, threadPool, Set.of()),
            Tracer.NOOP
        );

        transport.setDefaultConnectBehavior((t, discoveryNode, profile, listener) -> listener.onResponse(new CloseableConnection() {
            @Override
            public DiscoveryNode getNode() {
                return discoveryNode;
            }

            @Override
            public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                throws TransportException {

                SubscribableListener
                    // send request
                    .<TransportResponse>newForked(l -> threadPool.generic().execute(ActionRunnable.wrap(l, new CheckedConsumer<>() {
                        @Override
                        public void accept(ActionListener<TransportResponse> l2) throws Exception {
                            transport.getRequestHandlers().getHandler(action).processMessageReceived(request, new TestTransportChannel(l2));
                        }

                        @Override
                        public String toString() {
                            return "handle request [" + requestId + "][" + action + "]";
                        }
                    })))
                    // handle response
                    .addListener(new ThreadedActionListener<>(threadPool.generic(), new ActionListener<>() {
                        private TransportResponseHandler<TransportResponse> getResponseHandler() {
                            return lookupResponseHandler(requestId, action);
                        }

                        @Override
                        public void onResponse(TransportResponse transportResponse) {
                            getResponseHandler().handleResponse(transportResponse);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            getResponseHandler().handleException(new TransportException(e));
                        }
                    }));
            }

            @SuppressWarnings("unchecked")
            private TransportResponseHandler<TransportResponse> lookupResponseHandler(long requestId, String action) {
                return Objects.requireNonNull(
                    (TransportResponseHandler<TransportResponse>) transport.getResponseHandlers()
                        .onResponseReceived(requestId, TransportMessageListener.NOOP_LISTENER),
                    action
                );
            }

            @Override
            public TransportVersion getTransportVersion() {
                return TransportVersion.current();
            }
        }));

        final var clusterApplierService = new ClusterApplierService("test", settings, clusterSettings, threadPool) {
            @Override
            protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
                return deterministicTaskQueue.getPrioritizedEsThreadPoolExecutor();
            }
        };
        clusterApplierService.setNodeConnectionsService(new NodeConnectionsService(settings, threadPool, transportService));
        clusterApplierService.setInitialState(ClusterStateCreationUtils.state(localNode, localNode));

        final var masterService = new MasterService(settings, clusterSettings, threadPool, transportService.getTaskManager()) {
            @Override
            protected ExecutorService createThreadPoolExecutor() {
                return deterministicTaskQueue.getPrioritizedEsThreadPoolExecutor(threadContext::preserveContext);
            }
        };
        masterService.setClusterStatePublisher((clusterStatePublicationEvent, publishListener, ackListener) -> {
            ClusterServiceUtils.setAllElapsedMillis(clusterStatePublicationEvent);
            ackListener.onCommit(TimeValue.ZERO);
            clusterApplierService.onNewClusterState(
                clusterStatePublicationEvent.getSummary().toString(),
                clusterStatePublicationEvent::getNewState,
                publishListener.delegateFailureAndWrap((l, v) -> {
                    l.onResponse(v);
                    for (final var discoveryNode : clusterStatePublicationEvent.getNewState().nodes()) {
                        ackListener.onNodeAck(discoveryNode, null);
                    }
                })
            );
        });
        masterService.setClusterStateSupplier(clusterApplierService::state);

        final var clusterService = new ClusterService(settings, clusterSettings, masterService, clusterApplierService);

        final var systemIndices = new SystemIndices(List.of());

        class FakeRepository extends AbstractLifecycleComponent implements Repository {

            private final RepositoryMetadata repositoryMetadata = new RepositoryMetadata(
                "repo",
                "fake-uuid",
                "fake",
                Settings.EMPTY,
                1L,
                1L
            );

            @Override
            protected void doStart() {}

            @Override
            protected void doStop() {}

            @Override
            protected void doClose() {}

            @Override
            public RepositoryMetadata getMetadata() {
                return repositoryMetadata;
            }

            @Override
            public void getSnapshotInfo(GetSnapshotInfoContext context) {
                context.onFailure(new UnsupportedOperationException());
            }

            @Override
            public Metadata getSnapshotGlobalMetadata(SnapshotId snapshotId) {
                throw new UnsupportedOperationException();
            }

            @Override
            public IndexMetadata getSnapshotIndexMetaData(RepositoryData repositoryData, SnapshotId snapshotId, IndexId index) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void getRepositoryData(ActionListener<RepositoryData> listener) {
                threadPool.generic().execute(ActionRunnable.supply(listener, () -> RepositoryData.EMPTY));
            }

            @Override
            public void finalizeSnapshot(FinalizeSnapshotContext finalizeSnapshotContext) {
                finalizeSnapshotContext.onFailure(new UnsupportedOperationException());
            }

            @Override
            public void deleteSnapshots(
                Collection<SnapshotId> snapshotIds,
                long repositoryStateId,
                IndexVersion repositoryMetaVersion,
                SnapshotDeleteListener listener
            ) {
                listener.onFailure(new UnsupportedOperationException());
            }

            @Override
            public long getSnapshotThrottleTimeInNanos() {
                return 0;
            }

            @Override
            public long getRestoreThrottleTimeInNanos() {
                return 0;
            }

            @Override
            public String startVerification() {
                return randomAlphaOfLength(10);
            }

            @Override
            public void endVerification(String verificationToken) {}

            @Override
            public void verify(String verificationToken, DiscoveryNode localNode) {}

            @Override
            public boolean isReadOnly() {
                return false;
            }

            @Override
            public void snapshotShard(SnapshotShardContext snapshotShardContext) {
                snapshotShardContext.onFailure(new UnsupportedOperationException());
            }

            @Override
            public void restoreShard(
                Store store,
                SnapshotId snapshotId,
                IndexId indexId,
                ShardId snapshotShardId,
                RecoveryState recoveryState,
                ActionListener<Void> listener
            ) {
                listener.onFailure(new UnsupportedOperationException());
            }

            @Override
            public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void updateState(ClusterState state) {}

            @Override
            public void cloneShardSnapshot(
                SnapshotId source,
                SnapshotId target,
                RepositoryShardId shardId,
                ShardGeneration shardGeneration,
                ActionListener<ShardSnapshotResult> listener
            ) {
                listener.onFailure(new UnsupportedOperationException());
            }

            @Override
            public void awaitIdle() {}
        }

        final var repositoriesService = new RepositoriesService(
            settings,
            clusterService,
            transportService,
            Map.of("fake", ignored -> new FakeRepository()),
            Map.of(),
            threadPool,
            List.of()
        );

        final var snapshotsService = new SnapshotsService(
            settings,
            clusterService,
            new IndexNameExpressionResolver(threadPool.getThreadContext(), systemIndices),
            repositoriesService,
            transportService,
            new ActionFilters(Set.of()),
            systemIndices
        );

        clusterService.addStateApplier(repositoriesService);
        clusterService.addStateApplier(snapshotsService);

        transportService.start();
        clusterService.start();
        repositoriesService.start();
        snapshotsService.start();
        transportService.acceptIncomingRequests();

        final var future = new PlainActionFuture<Void>();

        SubscribableListener

            .<AcknowledgedResponse>newForked(l -> repositoriesService.registerRepository(new PutRepositoryRequest("repo").type("fake"), l))

            .addListener(future.map(ignored -> null));

        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue(future.isDone());
        future.actionGet();
    }

}
