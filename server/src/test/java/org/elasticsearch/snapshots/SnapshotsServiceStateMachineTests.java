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
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.RepositoryCleanupInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
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
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.FakeTransport;
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
import org.elasticsearch.xcontent.ToXContentObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.snapshots.SnapshotsService.UPDATE_SNAPSHOT_STATUS_ACTION_NAME;
import static org.hamcrest.Matchers.hasItem;

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

        final var initialRepositoryData = RepositoryData.EMPTY.withClusterUuid(UUIDs.randomBase64UUID(random()));
        final var repositoryName = "repo";
        final var repositoryType = "fake";

        final int dataNodeCount = between(1, 10);
        final var discoveryNodes = DiscoveryNodes.builder();
        discoveryNodes.add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId());
        for (int i = 0; i < dataNodeCount; i++) {
            discoveryNodes.add(DiscoveryNodeUtils.builder("node-" + i).roles(Set.of(DiscoveryNodeRole.DATA_ROLE)).build());
        }

        final var indexName = "test-index";
        final var indexMetadata = IndexMetadata.builder(indexName)
            .settings(
                indexSettings(IndexVersion.current(), between(1, 10), 0).put(SETTING_CREATION_DATE, System.currentTimeMillis())
                    .put(SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random()))
            )
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
            final var newState = clusterStatePublicationEvent.getNewState();
            logger.info(
                """
                    --> cluster state version {} in term {}
                    SnapshotsInProgress
                    {}
                    SnapshotDeletionsInProgress
                    {}
                    RepositoryCleanupInProgress
                    {}
                    """,
                newState.version(),
                newState.term(),
                Strings.toString(SnapshotsInProgress.get(newState), true, true),
                Strings.toString(SnapshotDeletionsInProgress.get(newState), true, true),
                Strings.toString(RepositoryCleanupInProgress.get(newState), true, true)
            );
            clusterApplierService.onNewClusterState(
                clusterStatePublicationEvent.getSummary().toString(),
                clusterStatePublicationEvent::getNewState,
                publishListener.delegateFailureAndWrap((l, v) -> {
                    l.onResponse(v);
                    for (final var discoveryNode : newState.nodes()) {
                        ackListener.onNodeAck(discoveryNode, null);
                    }
                })
            );
        });
        masterService.setClusterStateSupplier(clusterApplierService::state);

        final var clusterService = new ClusterService(settings, clusterSettings, masterService, clusterApplierService);

        record RepositoryShardState(Set<ShardGeneration> shardGenerations) {}

        class FakeRepository extends AbstractLifecycleComponent implements Repository {

            private final Map<RepositoryShardId, RepositoryShardState> repositoryShardStates = new HashMap<>();
            private RepositoryData repositoryData = initialRepositoryData;
            private RepositoryMetadata repositoryMetadata;

            FakeRepository(RepositoryMetadata repositoryMetadata) {
                this.repositoryMetadata = repositoryMetadata;
            }

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
                threadPool.generic().execute(ActionRunnable.supply(listener, () -> repositoryData));
            }

            @Override
            public void finalizeSnapshot(FinalizeSnapshotContext finalizeSnapshotContext) {

                SubscribableListener
                    // get current repo data
                    .newForked(this::getRepositoryData)

                    // compute new repo data
                    .<RepositoryData>andThen(
                        threadPool.generic(),
                        null,
                        (l, currentRepositoryData) -> l.onResponse(
                            currentRepositoryData.addSnapshot(
                                finalizeSnapshotContext.snapshotInfo().snapshotId(),
                                new RepositoryData.SnapshotDetails(
                                    finalizeSnapshotContext.snapshotInfo().state(),
                                    IndexVersion.current(),
                                    finalizeSnapshotContext.snapshotInfo().startTime(),
                                    finalizeSnapshotContext.snapshotInfo().endTime(),
                                    null
                                ),
                                finalizeSnapshotContext.updatedShardGenerations(),
                                null /*TODO*/
                                ,
                                null/*TODO*/
                            )
                        )
                    )

                    // store new repo data
                    .<RepositoryData>andThen(
                        threadPool.generic(),
                        null,
                        (l, updatedRepositoryData) -> updateRepositoryData(
                            "finalize snapshot " + finalizeSnapshotContext.snapshotInfo().snapshotId(),
                            updatedRepositoryData,
                            finalizeSnapshotContext::updatedClusterState,
                            l.map(ignored -> updatedRepositoryData)
                        )
                    )

                    // fork background cleanup
                    .<RepositoryData>andThen((l, updatedRepositoryData) -> {
                        l.onResponse(updatedRepositoryData);
                        try (
                            var refs = new RefCountingRunnable(() -> finalizeSnapshotContext.onDone(finalizeSnapshotContext.snapshotInfo()))
                        ) {
                            for (final var obsoleteShardGeneration : finalizeSnapshotContext.obsoleteShardGenerations().entrySet()) {
                                final var repositoryShardState = getShardState(obsoleteShardGeneration.getKey());
                                for (final var shardGeneration : obsoleteShardGeneration.getValue()) {
                                    threadPool.generic().execute(ActionRunnable.run(refs.acquireListener(), () -> {
                                        logger.info(
                                            "removing shard gen [{}] from [{}] in create",
                                            shardGeneration,
                                            obsoleteShardGeneration.getKey()
                                        );
                                        assertTrue(repositoryShardState.shardGenerations().remove(shardGeneration));
                                    }));
                                }
                            }
                        }
                    })

                    // complete the context
                    .addListener(finalizeSnapshotContext, threadPool.generic(), null);
            }

            @Override
            public void deleteSnapshots(
                Collection<SnapshotId> snapshotIds,
                long repositoryStateId,
                IndexVersion repositoryIndexVersion,
                SnapshotDeleteListener snapshotDeleteListener
            ) {
                record DeleteResult(RepositoryData updatedRepositoryData, List<Runnable> cleanups) {}

                SubscribableListener
                    // get current repo data
                    .newForked(this::getRepositoryData)

                    // compute new repository data
                    .<DeleteResult>andThen(threadPool.generic(), null, (l, currentRepositoryData) -> {
                        assertEquals(repositoryStateId, currentRepositoryData.getGenId());
                        final var updatedShardGenerations = ShardGenerations.builder();
                        final var currentShardGenerations = currentRepositoryData.shardGenerations();
                        final var cleanups = new ArrayList<Runnable>();
                        for (final var repositoryShardStateEntry : repositoryShardStates.entrySet()) {
                            final var shardId = repositoryShardStateEntry.getKey();
                            final var currentShardGeneration = currentShardGenerations.getShardGen(shardId.index(), shardId.shardId());
                            final ShardGeneration updatedShardGeneration;
                            final Set<ShardGeneration> shardGenerationsToRemove;
                            if (currentShardGeneration == null) {
                                // no successful snapshots of this shard
                                updatedShardGeneration = null;
                                shardGenerationsToRemove = Set.copyOf(repositoryShardStateEntry.getValue().shardGenerations());
                            } else if (randomBoolean()) {
                                // shard generation is unchanged
                                assertThat(
                                    shardId + " should have gen " + currentShardGeneration,
                                    repositoryShardStateEntry.getValue().shardGenerations(),
                                    hasItem(currentShardGeneration)
                                );
                                updatedShardGeneration = currentShardGeneration;
                                shardGenerationsToRemove = Set.of();
                            } else {
                                // new shard generation created
                                updatedShardGeneration = new ShardGeneration(randomAlphaOfLength(10));
                                shardGenerationsToRemove = Set.copyOf(repositoryShardStateEntry.getValue().shardGenerations());
                                assertTrue(repositoryShardStateEntry.getValue().shardGenerations().add(updatedShardGeneration));
                            }
                            updatedShardGenerations.put(shardId.index(), shardId.shardId(), updatedShardGeneration);
                            cleanups.add(() -> repositoryShardStateEntry.getValue().shardGenerations().removeAll(shardGenerationsToRemove));
                        }

                        l.onResponse(
                            new DeleteResult(repositoryData.removeSnapshots(snapshotIds, updatedShardGenerations.build()), cleanups)
                        );
                    })

                    // update repository data
                    .<DeleteResult>andThen(
                        threadPool.generic(),
                        null,
                        (l, deleteResult) -> updateRepositoryData(
                            "deleting snapshots " + snapshotIds,
                            deleteResult.updatedRepositoryData(),
                            cs -> cs,
                            l.map(v -> deleteResult)
                        )
                    )

                    // fork background cleanup
                    .<RepositoryData>andThen((l, deleteResult) -> {
                        l.onResponse(deleteResult.updatedRepositoryData());
                        try (var refs = new RefCountingRunnable(snapshotDeleteListener::onDone)) {
                            for (final var cleanup : deleteResult.cleanups()) {
                                threadPool.generic().execute(ActionRunnable.run(refs.acquireListener(), cleanup::run));
                            }
                        }
                    })

                    // complete the context
                    .addListener(new ActionListener<>() {
                        @Override
                        public void onResponse(RepositoryData repositoryData) {
                            snapshotDeleteListener.onRepositoryDataWritten(repositoryData);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            snapshotDeleteListener.onFailure(e);
                        }
                    }, threadPool.generic(), null);
            }

            private void updateRepositoryData(
                String description,
                RepositoryData updatedRepositoryData,
                UnaryOperator<ClusterState> clusterStateOperator,
                ActionListener<Void> listener
            ) {
                masterService.submitUnbatchedStateUpdateTask(description, new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        final var newRepositoriesMetadata = RepositoriesMetadata.get(currentState)
                            .withUpdatedGeneration(repositoryName, updatedRepositoryData.getGenId(), updatedRepositoryData.getGenId());

                        return clusterStateOperator.apply(
                            currentState.copyAndUpdateMetadata(mdb -> mdb.putCustom(RepositoriesMetadata.TYPE, newRepositoriesMetadata))
                        );
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }

                    @Override
                    public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                        logger.info(
                            "--> update repositoryData [{}]\n{}",
                            description,
                            Strings.toString(
                                (ToXContentObject) (builder, p) -> updatedRepositoryData.snapshotsToXContent(
                                    builder,
                                    IndexVersion.current()
                                ),
                                true,
                                true
                            )
                        );
                        repositoryData = updatedRepositoryData;
                        listener.onResponse(null);
                    }
                });
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
            public void updateState(ClusterState state) {
                repositoryMetadata = RepositoriesMetadata.get(state).repository(repositoryName);
            }

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

            public RepositoryShardState getShardState(RepositoryShardId repositoryShardId) {
                return repositoryShardStates.computeIfAbsent(repositoryShardId, this::newRepositoryShardState);
            }

            private RepositoryShardState newRepositoryShardState(RepositoryShardId ignored) {
                final var shardGenerations = new HashSet<ShardGeneration>();
                shardGenerations.add(ShardGenerations.NEW_SHARD_GEN);
                return new RepositoryShardState(shardGenerations);
            }

            @Override
            public String toString() {
                return FakeRepository.class.getSimpleName();
            }
        }

        final var repositoriesService = new RepositoriesService(
            settings,
            clusterService,
            transportService,
            Map.of(repositoryType, FakeRepository::new),
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

        clusterService.addStateApplier(new ClusterStateApplier() {

            private record OngoingShardSnapshot(Snapshot snapshot, ShardId shardId, RepositoryShardId repositoryShardId, String nodeId) {}

            private final Set<OngoingShardSnapshot> ongoingShardSnapshots = new HashSet<>();

            @Override
            public void applyClusterState(ClusterChangedEvent event) {
                final var activeSnapshotIds = SnapshotsInProgress.get(event.state())
                    .asStream()
                    .map(SnapshotsInProgress.Entry::snapshot)
                    .collect(Collectors.toSet());
                ongoingShardSnapshots.removeIf(ongoingShardSnapshot -> activeSnapshotIds.contains(ongoingShardSnapshot.snapshot) == false);

                SnapshotsInProgress.get(event.state()).asStream().forEach(snapshotInProgress -> {
                    for (final var shardSnapshotStatusEntry : snapshotInProgress.shards().entrySet()) {
                        switch (shardSnapshotStatusEntry.getValue().state()) {
                            case INIT -> {
                                final var shardId = shardSnapshotStatusEntry.getKey();
                                final var ongoingShardSnapshot = new OngoingShardSnapshot(
                                    snapshotInProgress.snapshot(),
                                    shardId,
                                    new RepositoryShardId(snapshotInProgress.indices().get(shardId.getIndexName()), shardId.id()),
                                    shardSnapshotStatusEntry.getValue().nodeId()
                                );
                                if (ongoingShardSnapshots.add(ongoingShardSnapshot)) {
                                    threadPool.generic().execute(ActionRunnable.<Void>wrap(new ActionListener<>() {
                                        @Override
                                        public void onResponse(Void unused) {}

                                        @Override
                                        public void onFailure(Exception e) {
                                            throw new AssertionError("unexpected", e);
                                        }
                                    },
                                        l -> doShardSnapshot(
                                            snapshotInProgress,
                                            ongoingShardSnapshot,
                                            shardSnapshotStatusEntry.getValue().generation(),
                                            l
                                        )
                                    ));
                                }
                            }
                            case ABORTED -> {
                                // fail("TODO");
                                // TODO
                            }
                        }
                    }
                });
            }

            private void doShardSnapshot(
                SnapshotsInProgress.Entry snapshotInProgress,
                OngoingShardSnapshot ongoingShardSnapshot,
                ShardGeneration originalShardGeneration,
                ActionListener<Void> listener
            ) {
                SubscribableListener

                    // perform shard snapshot
                    .<ShardSnapshotStatus>newForked(l -> ActionListener.completeWith(l, () -> {
                        final var repository = (FakeRepository) repositoriesService.repository(snapshotInProgress.repository());
                        final var repositoryShardState = repository.getShardState(ongoingShardSnapshot.repositoryShardId());
                        logger.info(
                            "--> doShardSnapshot[{}]: {}",
                            ongoingShardSnapshot.repositoryShardId(),
                            repositoryShardState.shardGenerations()
                        );
                        assertThat(
                            ongoingShardSnapshot.repositoryShardId().toString(),
                            repositoryShardState.shardGenerations(),
                            hasItem(originalShardGeneration)
                        );
                        repositoryShardState.shardGenerations().remove(ShardGenerations.NEW_SHARD_GEN);

                        final var newShardGeneration = new ShardGeneration(randomAlphaOfLength(10));
                        repositoryShardState.shardGenerations().add(newShardGeneration);

                        if (rarely()) {
                            return new ShardSnapshotStatus(
                                ongoingShardSnapshot.nodeId(),
                                SnapshotsInProgress.ShardState.FAILED,
                                "simulated",
                                randomBoolean() ? originalShardGeneration : newShardGeneration
                            );
                        }

                        return ShardSnapshotStatus.success(
                            ongoingShardSnapshot.nodeId(),
                            new ShardSnapshotResult(newShardGeneration, ByteSizeValue.ZERO, 1)
                        );
                    }))

                    // respond to master - TODO might not even be able to notify master, then what?
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
                    .addListener(listener);
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
                    l -> repositoriesService.registerRepository(new PutRepositoryRequest(repositoryName).type(repositoryType), l)
                )

                .<SnapshotInfo>andThen((l, r) -> snapshotsService.executeSnapshot(new CreateSnapshotRequest(repositoryName, "snap-1"), l))

                .<Void>andThen((l, r) -> snapshotsService.deleteSnapshots(new DeleteSnapshotRequest(repositoryName, "snap-1"), l))

                .<Void>andThen((l, r) -> {
                    try (var listeners = new RefCountingListener(l)) {
                        class DeleteAfterStartListener implements ClusterStateListener {
                            @Override
                            public void clusterChanged(ClusterChangedEvent event) {
                                if (SnapshotsInProgress.get(event.state())
                                    .asStream()
                                    .anyMatch(e -> e.snapshot().getSnapshotId().getName().equals("snap-2"))) {
                                    clusterApplierService.removeListener(DeleteAfterStartListener.this);
                                    threadPool.generic().execute(ActionRunnable.wrap(listeners.acquire().delegateResponse((dl, e) -> {
                                        if (e instanceof SnapshotMissingException) {
                                            // TODO should be impossible, we know the snapshot started
                                            dl.onResponse(null);
                                        } else {
                                            dl.onFailure(e);
                                        }
                                    }), dl -> snapshotsService.deleteSnapshots(new DeleteSnapshotRequest(repositoryName, "snap-2"), dl)));
                                }
                            }
                        }

                        clusterService.addListener(new DeleteAfterStartListener());
                        snapshotsService.executeSnapshot(
                            new CreateSnapshotRequest(repositoryName, "snap-2").partial(true),
                            listeners.acquire(i -> {})
                        );
                    }
                })

                .addListener(future.map(ignored -> null));

            deterministicTaskQueue.runAllTasksInTimeOrder();
            assertTrue(future.isDone());
            future.actionGet();

            final var repository = (FakeRepository) repositoriesService.repository("repo");
            logger.info("--> final states: {}", repository.repositoryShardStates);
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
