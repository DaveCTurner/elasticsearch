/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
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
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.repositories.VerifyNodeRepositoryAction;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.FakeTransport;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.transport.CloseableConnection;
import org.elasticsearch.transport.ClusterConnectionManager;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportMessageListener;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.snapshots.SnapshotsService.UPDATE_SNAPSHOT_STATUS_ACTION_NAME;

@TestLogging(
    reason = "nocommit",
    value = "org.elasticsearch.common.util.concurrent.DeterministicTaskQueue:TRACE"
        + ",org.elasticsearch.cluster.service.MasterService:TRACE"
        + ",org.elasticsearch.repositories.RepositoriesService:TRACE"
        + ",org.elasticsearch.snapshots.SnapshotsService:TRACE"
        + ",org.elasticsearch.cluster.service.ClusterApplierService:TRACE"
)
public class SnapshotsServiceStateMachineTests extends ESTestCase {

    public void testFoo() {

        final var settings = Settings.EMPTY;
        final var clusterSettings = ClusterSettings.createBuiltInClusterSettings(settings);

        final var deterministicTaskQueue = new DeterministicTaskQueue();
        deterministicTaskQueue.setExecutionDelayVariabilityMillis(1000);
        final var threadPool = deterministicTaskQueue.getThreadPool();
        final var threadContext = threadPool.getThreadContext();

        final var localNode = DiscoveryNodeUtils.create("local");

        final var transport = new FakeTransport() {
            @Override
            public void openConnection(DiscoveryNode discoveryNode, ConnectionProfile profile, ActionListener<Connection> listener) {
                listener.onResponse(new CloseableConnection() {
                    @Override
                    public DiscoveryNode getNode() {
                        return discoveryNode;
                    }

                    @Override
                    public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                        throws TransportException {

                        switch (action) {
                            case TransportService.HANDSHAKE_ACTION_NAME -> lookupResponseHandler(requestId, action).handleResponse(
                                new TransportService.HandshakeResponse(
                                    Version.CURRENT,
                                    Build.current().hash(),
                                    discoveryNode,
                                    ClusterName.DEFAULT
                                )
                            );
                            case VerifyNodeRepositoryAction.ACTION_NAME -> lookupResponseHandler(requestId, action).handleResponse(
                                TransportResponse.Empty.INSTANCE
                            );
                            default -> throw new UnsupportedOperationException("unexpected action [" + action + "]");
                        }
                    }

                    @SuppressWarnings("unchecked")
                    private TransportResponseHandler<TransportResponse> lookupResponseHandler(long requestId, String action) {
                        return Objects.requireNonNull(
                            (TransportResponseHandler<TransportResponse>) getResponseHandlers().onResponseReceived(
                                requestId,
                                TransportMessageListener.NOOP_LISTENER
                            ),
                            action
                        );
                    }

                    @Override
                    public TransportVersion getTransportVersion() {
                        return TransportVersion.current();
                    }
                });
            }
        };
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

        final var clusterApplierService = new ClusterApplierService("test", settings, clusterSettings, threadPool) {
            @Override
            protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
                return deterministicTaskQueue.getPrioritizedEsThreadPoolExecutor();
            }
        };
        clusterApplierService.setNodeConnectionsService(new NodeConnectionsService(settings, threadPool, transportService));

        final int dataNodeCount = between(1, 10);
        final var discoveryNodes = DiscoveryNodes.builder();
        discoveryNodes.add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId());
        for (int i = 0; i < dataNodeCount; i++) {
            discoveryNodes.add(DiscoveryNodeUtils.builder("node-" + i).roles(Set.of(DiscoveryNodeRole.DATA_ROLE)).build());
        }

        final var indexName = "test-index";
        final var indexMetadata = IndexMetadata.builder(indexName)
            .settings(indexSettings(IndexVersion.current(), between(1, 10), 0).put(SETTING_CREATION_DATE, System.currentTimeMillis()))
            .build();
        final var indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex());
        for (int i = 0; i < indexMetadata.getRoutingNumShards(); i++) {
            indexRoutingTable.addShard(
                TestShardRouting.newShardRouting(
                    new ShardId(indexMetadata.getIndex(), i),
                    "node-" + between(0, dataNodeCount - 1),
                    true,
                    ShardRoutingState.STARTED
                )
            );
        }

        clusterApplierService.setInitialState(
            ClusterState.builder(ClusterName.DEFAULT)
                .nodes(discoveryNodes)
                .metadata(Metadata.builder().put(indexMetadata, false).generateClusterUuidIfNeeded())
                .routingTable(RoutingTable.builder().add(indexRoutingTable))
                .build()
        );

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

        record RepositoryShardState(Set<String> shardGenerations) {}

        class FakeRepository extends AbstractLifecycleComponent implements Repository {

            private final Map<ShardId, RepositoryShardState> repositoryShardStates = new HashMap<>();

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

            public RepositoryShardState getShardState(ShardId shardId) {
                return repositoryShardStates.computeIfAbsent(shardId, this::newRepositoryShardState);
            }

            private RepositoryShardState newRepositoryShardState(ShardId ignored) {
                final var shardGenerations = new HashSet<String>();
                shardGenerations.add(ShardGenerations.NEW_SHARD_GEN.toString());
                return new RepositoryShardState(shardGenerations);
            }
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

        final var systemIndices = new SystemIndices(List.of());

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
        clusterService.addStateApplier(new ClusterStateApplier() {

            private record OngoingShardSnapshot(Snapshot snapshot, ShardId shardId, String nodeId) {}

            private final Set<OngoingShardSnapshot> ongoingShardSnapshots = new HashSet<>();

            @Override
            public void applyClusterState(ClusterChangedEvent event) {
                SnapshotsInProgress.get(event.state()).asStream().forEach(snapshotInProgress -> {
                    for (final var shardSnapshotStatusEntry : snapshotInProgress.shards().entrySet()) {
                        switch (shardSnapshotStatusEntry.getValue().state()) {
                            case INIT -> {
                                final var ongoingShardSnapshot = new OngoingShardSnapshot(
                                    snapshotInProgress.snapshot(),
                                    shardSnapshotStatusEntry.getKey(),
                                    shardSnapshotStatusEntry.getValue().nodeId()
                                );
                                if (ongoingShardSnapshots.add(ongoingShardSnapshot)) {
                                    doShardSnapshot(
                                        snapshotInProgress,
                                        ongoingShardSnapshot,
                                        shardSnapshotStatusEntry.getValue().generation()
                                    );
                                }
                            }
                            case ABORTED -> {
                                // TODO
                            }
                        }
                    }
                });
            }

            private void doShardSnapshot(
                SnapshotsInProgress.Entry snapshotInProgress,
                OngoingShardSnapshot ongoingShardSnapshot,
                ShardGeneration originalShardGeneration
            ) {
                SubscribableListener

                    // fork
                    .<Void>newForked(l -> threadPool.generic().execute(ActionRunnable.run(l, () -> {})))

                    // perform shard snapshot
                    .<ShardSnapshotStatus>andThen(threadPool.generic(), null, (l, v) -> {
                        ActionListener.completeWith(l, () -> {
                            final var repository = (FakeRepository) repositoriesService.repository(snapshotInProgress.repository());
                            final var repositoryShardState = repository.getShardState(ongoingShardSnapshot.shardId());
                            assertThat(repositoryShardState.shardGenerations(), Matchers.hasItem(originalShardGeneration.toString()));

                            final var newShardGeneration = new ShardGeneration(randomAlphaOfLength(10));
                            repositoryShardState.shardGenerations().add(newShardGeneration.toString());

                            if (usually()) {
                                return ShardSnapshotStatus.success(
                                    ongoingShardSnapshot.nodeId(),
                                    new ShardSnapshotResult(newShardGeneration, ByteSizeValue.ZERO, 1)
                                );
                            }
                            return new ShardSnapshotStatus(
                                ongoingShardSnapshot.nodeId(),
                                SnapshotsInProgress.ShardState.FAILED,
                                "simulated",
                                randomBoolean() ? null : newShardGeneration
                            );
                        });
                    })

                    // clean up
                    .<ShardSnapshotStatus>andThen(threadPool.generic(), null, (l, v) -> {
                        assertTrue(ongoingShardSnapshots.remove(ongoingShardSnapshot));
                        l.onResponse(v);
                    })

                    // respond to master
                    .<Void>andThen(
                        threadPool.generic(),
                        null,
                        (l, shardSnapshotStatus) -> transportService.sendRequest(
                            transportService.getLocalNodeConnection(),
                            UPDATE_SNAPSHOT_STATUS_ACTION_NAME,
                            new UpdateIndexShardSnapshotStatusRequest(
                                ongoingShardSnapshot.snapshot,
                                ongoingShardSnapshot.shardId(),
                                shardSnapshotStatus
                            ),
                            TransportRequestOptions.EMPTY,
                            new ActionListenerResponseHandler<>(
                                l.map(ignored -> null),
                                in -> ActionResponse.Empty.INSTANCE,
                                threadPool.generic()
                            )
                        )
                    )

                    // check for no errors
                    .addListener(new ActionListener<>() {
                        @Override
                        public void onResponse(Void unused) {}

                        @Override
                        public void onFailure(Exception e) {
                            throw new AssertionError("unexpected", e);
                        }
                    });
            }
        });

        transportService.start();
        clusterService.start();
        repositoriesService.start();
        snapshotsService.start();
        transportService.acceptIncomingRequests();

        try {
            final var future = new PlainActionFuture<Void>();

            SubscribableListener

                .<AcknowledgedResponse>newForked(
                    l -> repositoriesService.registerRepository(new PutRepositoryRequest("repo").type("fake"), l)
                )

                .<SnapshotInfo>andThen((l, r) -> snapshotsService.executeSnapshot(new CreateSnapshotRequest("repo", "snap-1"), l))

                .addListener(future.map(ignored -> null));

            deterministicTaskQueue.runAllTasksInTimeOrder();
            assertTrue(future.isDone());
            future.actionGet();
        } finally {
            snapshotsService.stop();
            repositoriesService.stop();
            clusterService.stop();
            transportService.stop();
            snapshotsService.close();
            repositoriesService.close();
            clusterService.close();
            transportService.close();
        }
    }

}
