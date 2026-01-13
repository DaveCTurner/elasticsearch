/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.ProjectScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.FinalizeSnapshotContext;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoriesStats;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.repositories.RepositoryShardId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.repositories.SnapshotMetrics;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.UnaryOperator;

import static org.elasticsearch.cluster.SnapshotDeletionsInProgress.State.WAITING;
import static org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED;
import static org.elasticsearch.cluster.SnapshotsInProgress.State.ABORTED;
import static org.elasticsearch.cluster.SnapshotsInProgress.State.SUCCESS;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@TestLogging(
    reason = "nocommit",
    value = "org.elasticsearch.cluster.coordination:TRACE"
        + ",org.elasticsearch.discovery:TRACE"
        + ",org.elasticsearch.snapshots:TRACE"
        + ",org.elasticsearch.common.util.concurrent.DeterministicTaskQueue:TRACE"
        + ",org.elasticsearch.cluster.service.MasterService:TRACE"
)
public class SnapshotDeletionStartBatcherTests extends ESTestCase {

    private DeterministicTaskQueue deterministicTaskQueue;
    private ClusterService clusterService;
    private RepositoriesService repositoriesService;
    private SnapshotDeletionStartBatcher batcher;
    private String repoName;

    private Set<SnapshotId> snapshotAbortNotifications;
    private Set<SnapshotId> snapshotEndNotifications;
    private Map<String, List<ActionListener<Void>>> completionHandlers;
    private List<SnapshotDeletionsInProgress.Entry> startedDeletions;
    private DiscoveryNode localNode;

    @Before
    public void createServices() {
        deterministicTaskQueue = new DeterministicTaskQueue();
        final var threadPool = deterministicTaskQueue.getThreadPool();

        final var settings = Settings.builder()
            // disable infinitely-retrying watchdog so we can completely drain the task queue
            .put(ClusterApplierService.CLUSTER_APPLIER_THREAD_WATCHDOG_INTERVAL.getKey(), TimeValue.ZERO)
            .build();
        final var clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final var projectScopedSettings = new ProjectScopedSettings(settings, Set.of());

        localNode = DiscoveryNodeUtils.create(randomIdentifier("node-id-"));
        final var initialState = ClusterState.builder(new ClusterName(randomIdentifier("cluster-")))
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()).build())
            .build();

        final var masterService = new MasterService(
            settings,
            clusterSettings,
            threadPool,
            new TaskManager(settings, threadPool, Set.of()),
            MeterRegistry.NOOP
        ) {
            @Override
            protected ExecutorService createThreadPoolExecutor() {
                return deterministicTaskQueue.getPrioritizedEsThreadPoolExecutor();
            }
        };

        final var clusterApplierService = new ClusterApplierService(randomIdentifier("node-"), settings, clusterSettings, threadPool) {
            @Override
            protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
                return deterministicTaskQueue.getPrioritizedEsThreadPoolExecutor();
            }
        };
        clusterApplierService.setInitialState(initialState);
        clusterApplierService.setNodeConnectionsService(ClusterServiceUtils.createNoOpNodeConnectionsService());
        masterService.setClusterStateSupplier(clusterApplierService::state);
        masterService.setClusterStatePublisher((clusterStatePublicationEvent, publishListener, ackListener) -> {
            ClusterServiceUtils.setAllElapsedMillis(clusterStatePublicationEvent);
            ackListener.onCommit(TimeValue.ZERO);
            clusterApplierService.onNewClusterState(
                "mock_publish_to_self[" + clusterStatePublicationEvent.getSummary() + "]",
                clusterStatePublicationEvent::getNewState,
                ActionTestUtils.assertNoFailureListener(ignored -> {
                    logger.info("--> ack and complete [{}]", clusterStatePublicationEvent.getNewState().version());
                    ackListener.onNodeAck(localNode, null);
                    publishListener.onResponse(null);
                })
            );
        });

        clusterService = new ClusterService(settings, clusterSettings, projectScopedSettings, masterService, clusterApplierService);
        clusterService.start();

        final var repoType = randomIdentifier("repo-type-");
        repositoriesService = new RepositoriesService(
            settings,
            clusterService,
            Map.of(repoType, TestRepository::new),
            Map.of(),
            threadPool,
            new NodeClient(settings, threadPool, DefaultProjectResolver.INSTANCE),
            List.of(),
            SnapshotMetrics.NOOP
        );
        repositoriesService.start();

        threadPool.getThreadContext().markAsSystemContext();

        repoName = randomIdentifier("repo-");
        safeAwait(l -> {
            repositoriesService.registerRepository(
                ProjectId.DEFAULT,
                new PutRepositoryRequest(
                    MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
                    MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
                    repoName
                ).type(repoType).verify(false),
                l.map(ignored -> null)
            );
            deterministicTaskQueue.runAllTasksInTimeOrder();
        });

        snapshotAbortNotifications = new HashSet<>();
        snapshotEndNotifications = new HashSet<>();
        completionHandlers = new HashMap<>();
        startedDeletions = new ArrayList<>();

        batcher = new SnapshotDeletionStartBatcher(
            repositoriesService,
            clusterService,
            threadPool,
            ProjectId.DEFAULT,
            repoName,
            snapshot -> {
                assertEquals(ProjectId.DEFAULT, snapshot.getProjectId());
                assertEquals(repoName, snapshot.getRepository());
                snapshotAbortNotifications.add(snapshot.getSnapshotId());
            },
            (entry, ignoredMetadata, ignoredRepositoryData) -> {
                assertEquals(ProjectId.DEFAULT, entry.projectId());
                assertEquals(repoName, entry.repository());
                snapshotEndNotifications.add(entry.snapshot().getSnapshotId());
            },
            (deletionUuid, itemListener) -> {
                if (deletionUuid == null) {
                    itemListener.onResponse(null);
                } else {
                    completionHandlers.computeIfAbsent(deletionUuid, ignored -> new ArrayList<>()).add(itemListener);
                }
            },
            (projectId, repositoryName, deleteEntry, ignoredRepositoryData, ignoredMinNodeVersion) -> {
                assertEquals(ProjectId.DEFAULT, projectId);
                assertEquals(repoName, repositoryName);
                startedDeletions.add(deleteEntry);
            }
        );

        logger.info("----> services all created");
    }

    private static class TestRepository extends AbstractLifecycleComponent implements Repository {
        private final RepositoryMetadata metadata;

        TestRepository(ProjectId projectId, RepositoryMetadata metadata) {
            assertEquals(ProjectId.DEFAULT, projectId);
            this.metadata = metadata;
        }

        @Override
        public ProjectId getProjectId() {
            return ProjectId.DEFAULT;
        }

        @Override
        public RepositoryMetadata getMetadata() {
            return metadata;
        }

        @Override
        public void getSnapshotInfo(
            Collection<SnapshotId> snapshotIds,
            boolean abortOnFailure,
            BooleanSupplier isCancelled,
            CheckedConsumer<SnapshotInfo, Exception> consumer,
            ActionListener<Void> listener
        ) {
            throw new AssertionError("should not be called");
        }

        @Override
        public Metadata getSnapshotGlobalMetadata(SnapshotId snapshotId, boolean fromProjectMetadata) {
            throw new AssertionError("should not be called");
        }

        @Override
        public IndexMetadata getSnapshotIndexMetaData(RepositoryData repositoryData, SnapshotId snapshotId, IndexId index) {
            throw new AssertionError("should not be called");
        }

        @Override
        public void getRepositoryData(Executor responseExecutor, ActionListener<RepositoryData> listener) {
            listener.onResponse(RepositoryData.EMPTY);
        }

        @Override
        public void finalizeSnapshot(FinalizeSnapshotContext finalizeSnapshotContext) {
            throw new AssertionError("should not be called");
        }

        @Override
        public void deleteSnapshots(
            Collection<SnapshotId> snapshotIds,
            long repositoryDataGeneration,
            IndexVersion minimumNodeVersion,
            ActionListener<RepositoryData> repositoryDataUpdateListener,
            Runnable onCompletion
        ) {
            throw new AssertionError("should not be called");
        }

        @Override
        public String startVerification() {
            throw new AssertionError("should not be called");
        }

        @Override
        public void endVerification(String verificationToken) {
            throw new AssertionError("should not be called");
        }

        @Override
        public void verify(String verificationToken, DiscoveryNode localNode) {
            throw new AssertionError("should not be called");
        }

        @Override
        public boolean isReadOnly() {
            throw new AssertionError("should not be called");
        }

        @Override
        public void snapshotShard(SnapshotShardContext snapshotShardContext) {
            throw new AssertionError("should not be called");
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
            throw new AssertionError("should not be called");
        }

        @Override
        public IndexShardSnapshotStatus.Copy getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
            throw new AssertionError("should not be called");
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
            throw new AssertionError("should not be called");
        }

        @Override
        public void awaitIdle() {
            throw new AssertionError("should not be called");
        }

        @Override
        public LongWithAttributes getShardSnapshotsInProgress() {
            throw new AssertionError("should not be called");
        }

        @Override
        public RepositoriesStats.SnapshotStats getSnapshotStats() {
            throw new AssertionError("should not be called");
        }

        @Override
        public void doStart() {}

        @Override
        public void doStop() {}

        @Override
        public void doClose() {}
    }

    private void updateClusterState(UnaryOperator<ClusterState> clusterStateOperator) {
        final var completed = new AtomicBoolean();
        // noinspection deprecation
        clusterService.submitUnbatchedStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return clusterStateOperator.apply(currentState);
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                assertTrue(completed.compareAndSet(false, true));
            }
        });
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue(completed.get());
    }

    public void testUnknownRepository() {
        final var unregisterFuture = SubscribableListener.<AcknowledgedResponse>newForked(
            l -> repositoriesService.unregisterRepository(
                ProjectId.DEFAULT,
                new DeleteRepositoryRequest(
                    MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
                    MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
                    repoName
                ),
                l
            )
        );
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue(unregisterFuture.isDone());
        safeAwait(unregisterFuture);

        final var deletionFuture = SubscribableListener.<Void>newForked(
            l -> batcher.startDeletion(new String[] { randomSnapshotName() }, true, MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT, l)
        );
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue(deletionFuture.isDone());
        assertThat(
            asInstanceOf(RepositoryMissingException.class, safeAwaitFailure(deletionFuture)).getMessage(),
            equalTo(Strings.format("[%s] missing", repoName))
        );
    }

    public void testDeleteMissingSnapshots() {
        final var snapshotName = randomSnapshotName();
        final var deletionFuture = SubscribableListener.<Void>newForked(
            l -> batcher.startDeletion(new String[] { snapshotName }, true, MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT, l)
        );
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue(deletionFuture.isDone());
        assertThat(
            asInstanceOf(SnapshotMissingException.class, safeAwaitFailure(deletionFuture)).getMessage(),
            allOf(containsString(repoName), containsString(snapshotName))
        );
    }

    public void testTimeoutWhileWaiting() {
        final var keepGoing = new AtomicBoolean(true);
        class LoopingTask extends ClusterStateUpdateTask {
            LoopingTask() {
                super(Priority.HIGH);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                if (deterministicTaskQueue.hasDeferredTasks()) {
                    deterministicTaskQueue.advanceTime();
                }
                return currentState;
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                maybeEnqueueTask();
            }

            void maybeEnqueueTask() {
                if (keepGoing.get()) {
                    logger.info("--> submit looping task");
                    // noinspection deprecation
                    clusterService.submitUnbatchedStateUpdateTask("looping task", LoopingTask.this);
                }
            }
        }
        new LoopingTask().maybeEnqueueTask();

        final var snapshotName = randomSnapshotName();
        final var deletionFuture = SubscribableListener.<Void>newForked(
            l -> batcher.startDeletion(new String[] { snapshotName }, true, TimeValue.THIRTY_SECONDS, l)
        );
        deletionFuture.addListener(ActionTestUtils.assertNoSuccessListener(e -> {
            assertThat(
                asInstanceOf(ElasticsearchTimeoutException.class, e).getMessage(),
                allOf(containsString(repoName), containsString("did not start snapshot deletion within [30s]"))
            );
            keepGoing.set(false);
        }));
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue(deletionFuture.isDone());
        assertFalse(keepGoing.get());
    }

    public void testDeleteNotStartedSnapshot() {
        final var snapshot = new Snapshot(ProjectId.DEFAULT, repoName, new SnapshotId(randomSnapshotName(), randomUUID()));
        final var repoIndex = new IndexId(randomIndexName(), randomUUID());
        final var shardId = new ShardId(new Index(repoIndex.getName(), randomUUID()), 0);
        updateClusterState(
            currentState -> ClusterState.builder(currentState)
                .putCustom(
                    SnapshotsInProgress.TYPE,
                    SnapshotsInProgress.EMPTY.createCopyWithUpdatedEntriesForRepo(
                        ProjectId.DEFAULT,
                        repoName,
                        List.of(
                            SnapshotsInProgress.startedEntry(
                                snapshot,
                                randomBoolean(),
                                randomBoolean(),
                                Map.of(repoIndex.getName(), repoIndex),
                                List.of(),
                                randomNonNegativeLong(),
                                randomNonNegativeLong(),
                                Map.of(shardId, UNASSIGNED_QUEUED),
                                Map.of(),
                                IndexVersionUtils.randomVersion(),
                                List.of()
                            )
                        )
                    )
                )
                .build()
        );

        final var deletionFuture = SubscribableListener.<Void>newForked(
            l -> batcher.startDeletion(
                new String[] { snapshot.getSnapshotId().getName() },
                true,
                MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
                l
            )
        );
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue(deletionFuture.isDone());
        safeAwait(deletionFuture);

        assertTrue(startedDeletions.isEmpty());
        assertTrue(snapshotEndNotifications.isEmpty());
        assertThat(snapshotAbortNotifications, equalTo(Set.of(snapshot.getSnapshotId())));
        assertTrue(completionHandlers.isEmpty());

        assertEquals(0, SnapshotsInProgress.get(clusterService.state()).count());
        assertThat(SnapshotDeletionsInProgress.get(clusterService.state()).getEntries(), hasSize(0));
    }

    public void testDeleteInProgressSnapshot() {
        final var snapshot = new Snapshot(ProjectId.DEFAULT, repoName, new SnapshotId(randomSnapshotName(), randomUUID()));
        final var repoIndex = new IndexId(randomIndexName(), randomUUID());
        final var shardId = new ShardId(new Index(repoIndex.getName(), randomUUID()), 0);
        final var shardGeneration = ShardGeneration.newGeneration();
        updateClusterState(
            currentState -> ClusterState.builder(currentState)
                .putCustom(
                    SnapshotsInProgress.TYPE,
                    SnapshotsInProgress.EMPTY.createCopyWithUpdatedEntriesForRepo(
                        ProjectId.DEFAULT,
                        repoName,
                        List.of(
                            SnapshotsInProgress.startedEntry(
                                snapshot,
                                randomBoolean(),
                                randomBoolean(),
                                Map.of(repoIndex.getName(), repoIndex),
                                List.of(),
                                randomNonNegativeLong(),
                                randomNonNegativeLong(),
                                Map.of(shardId, new SnapshotsInProgress.ShardSnapshotStatus(localNode.getId(), shardGeneration)),
                                Map.of(),
                                IndexVersionUtils.randomVersion(),
                                List.of()
                            )
                        )
                    )
                )
                .build()
        );

        final var deletionFuture = SubscribableListener.<Void>newForked(
            l -> batcher.startDeletion(
                new String[] { snapshot.getSnapshotId().getName() },
                true,
                MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
                l
            )
        );
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertFalse(deletionFuture.isDone());

        assertTrue(startedDeletions.isEmpty());
        assertTrue(snapshotEndNotifications.isEmpty());
        assertTrue(snapshotAbortNotifications.isEmpty());

        final var snapshotsInProgress = SnapshotsInProgress.get(clusterService.state());
        assertEquals(1, snapshotsInProgress.count());
        final var snapshotEntry = Objects.requireNonNull(snapshotsInProgress.snapshot(snapshot));
        assertEquals(ABORTED, snapshotEntry.state());
        assertEquals(
            Map.of(
                shardId,
                new SnapshotsInProgress.ShardSnapshotStatus(
                    localNode.getId(),
                    SnapshotsInProgress.ShardState.ABORTED,
                    shardGeneration,
                    "aborted by snapshot deletion"
                )
            ),
            snapshotEntry.shards()
        );

        final var deletionsInProgress = SnapshotDeletionsInProgress.get(clusterService.state());
        assertThat(deletionsInProgress.getEntries(), hasSize(1));
        final var deletionEntry = deletionsInProgress.getEntries().getFirst();
        assertEquals(WAITING, deletionEntry.state());

        assertThat(completionHandlers.keySet(), equalTo(Set.of(deletionEntry.uuid())));
        final var listeners = Objects.requireNonNull(completionHandlers.get(deletionEntry.uuid()));
        assertThat(listeners, hasSize(1));
        listeners.getFirst().onResponse(null);

        assertTrue(deletionFuture.isDone());
        safeAwait(deletionFuture);
    }

    public void testDeleteCompletesInProgressSnapshot() {
        final var snapshot = new Snapshot(ProjectId.DEFAULT, repoName, new SnapshotId(randomSnapshotName(), randomUUID()));
        final var repoIndex = new IndexId(randomIndexName(), randomUUID());
        final var index = new Index(repoIndex.getName(), randomUUID());

        final var shardId0 = new ShardId(index, 0);
        final var shard0Status = randomBoolean()
            ? SnapshotsInProgress.ShardSnapshotStatus.success(
                localNode.getId(),
                new ShardSnapshotResult(ShardGeneration.newGeneration(), ByteSizeValue.ZERO, 1)
            )
            : new SnapshotsInProgress.ShardSnapshotStatus(
                localNode.getId(),
                SnapshotsInProgress.ShardState.FAILED,
                ShardGeneration.newGeneration(),
                "test"
            );

        final var shardId1 = new ShardId(index, 1);

        updateClusterState(
            currentState -> ClusterState.builder(currentState)
                .putCustom(
                    SnapshotsInProgress.TYPE,
                    SnapshotsInProgress.EMPTY.createCopyWithUpdatedEntriesForRepo(
                        ProjectId.DEFAULT,
                        repoName,
                        List.of(
                            SnapshotsInProgress.startedEntry(
                                snapshot,
                                randomBoolean(),
                                randomBoolean(),
                                Map.of(repoIndex.getName(), repoIndex),
                                List.of(),
                                randomNonNegativeLong(),
                                randomNonNegativeLong(),
                                Map.of(shardId0, shard0Status, shardId1, UNASSIGNED_QUEUED),
                                Map.of(),
                                IndexVersionUtils.randomVersion(),
                                List.of()
                            )
                        )
                    )
                )
                .build()
        );

        final var deletionFuture = SubscribableListener.<Void>newForked(
            l -> batcher.startDeletion(
                new String[] { snapshot.getSnapshotId().getName() },
                true,
                MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
                l
            )
        );
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertFalse(deletionFuture.isDone());

        assertTrue(startedDeletions.isEmpty());
        assertThat(snapshotEndNotifications, equalTo(Set.of(snapshot.getSnapshotId())));
        assertTrue(snapshotAbortNotifications.isEmpty());

        final var snapshotsInProgress = SnapshotsInProgress.get(clusterService.state());
        assertEquals(1, snapshotsInProgress.count());
        final var snapshotEntry = Objects.requireNonNull(snapshotsInProgress.snapshot(snapshot));
        assertEquals(SUCCESS, snapshotEntry.state());
        assertEquals(
            Map.of(
                shardId0,
                shard0Status,
                shardId1,
                new SnapshotsInProgress.ShardSnapshotStatus(
                    null,
                    SnapshotsInProgress.ShardState.FAILED,
                    null,
                    "aborted by snapshot deletion"
                )
            ),
            snapshotEntry.shards()
        );

        final var deletionsInProgress = SnapshotDeletionsInProgress.get(clusterService.state());
        assertThat(deletionsInProgress.getEntries(), hasSize(1));
        final var deletionEntry = deletionsInProgress.getEntries().getFirst();
        assertEquals(WAITING, deletionEntry.state());

        assertThat(completionHandlers.keySet(), equalTo(Set.of(deletionEntry.uuid())));
        final var listeners = Objects.requireNonNull(completionHandlers.get(deletionEntry.uuid()));
        assertThat(listeners, hasSize(1));
        listeners.getFirst().onResponse(null);

        assertTrue(deletionFuture.isDone());
        safeAwait(deletionFuture);
    }
}
