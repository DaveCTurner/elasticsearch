/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class SnapshotRestoreRandomlyIT extends AbstractSnapshotIntegTestCase {

    public void testRandomActivities() throws InterruptedException {
        final TrackedCluster trackedCluster = new TrackedCluster(internalCluster());

        trackedCluster.start();

        Thread.sleep(20000);

        trackedCluster.stop();

        disableRepoConsistencyCheck("have not necessarily written to all repositories");
    }

    static class TrackedCluster {

        static final Logger logger = LogManager.getLogger(TrackedCluster.class);
        static final String CLIENT = "client";

        private final ThreadPool threadPool = new TestThreadPool("TrackedCluster",
            // a single thread for "client" activities, to limit the number of activities all starting at once
            new ScalingExecutorBuilder(CLIENT, 1, 1, TimeValue.ZERO, CLIENT));

        private final InternalTestCluster cluster;
        private final Map<String, TrackedNode> nodes = ConcurrentCollections.newConcurrentMap();
        private final Map<String, TrackedRepository> repositories = ConcurrentCollections.newConcurrentMap();
        private final Map<String, TrackedIndex> indices = ConcurrentCollections.newConcurrentMap();
        private final Map<String, TrackedSnapshot> snapshots = ConcurrentCollections.newConcurrentMap();

        private final AtomicInteger snapshotCounter = new AtomicInteger();

        TrackedCluster(InternalTestCluster cluster) {
            this.cluster = cluster;
            for (String nodeName : cluster.getNodeNames()) {
                nodes.put(nodeName, new TrackedNode(nodeName));
            }

            final int repoCount = between(1, 3);
            for (int i = 0; i < repoCount; i++) {
                final String repositoryName = "repo-" + i;
                repositories.put(repositoryName, new TrackedRepository(repositoryName, randomRepoPath()));
            }

            final int indexCount = between(1, 10);
            for (int i = 0; i < indexCount; i++) {
                final String indexName = "index-" + i;
                indices.put(indexName, new TrackedIndex(indexName));
            }
        }

        void start() {
            for (TrackedNode trackedNode : nodes.values()) {
                trackedNode.start();
            }

            for (TrackedIndex trackedIndex : indices.values()) {
                trackedIndex.start();
            }

            for (TrackedRepository trackedRepository : repositories.values()) {
                trackedRepository.start();
            }

            final int snapshotterCount = between(1, 5);
            for (int i = 0; i < snapshotterCount; i++) {
                startSnapshotter();
            }

            final int clonerCount = between(0, 5);
            for (int i = 0; i < clonerCount; i++) {
                startCloner();
            }

            final int deleterCount = between(0, 3);
            for (int i = 0; i < deleterCount; i++) {
                startSnapshotDeleter();
            }
        }

        private void startCloner() {
            threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueMillis(between(1, 500)), CLIENT, mustSucceed(() -> {
                final List<Releasable> localReleasables = new ArrayList<>();

                try {
                    final List<TrackedSnapshot> trackedSnapshots = new ArrayList<>(snapshots.values());
                    if (trackedSnapshots.isEmpty()) {
                        startCloner();
                        return;
                    }

                    final Releasable nodeRestartBlock = blockNodeRestarts();
                    if (nodeRestartBlock == null) {
                        startCloner();
                        return;
                    }
                    localReleasables.add(nodeRestartBlock);

                    final TrackedSnapshot trackedSnapshot = randomFrom(trackedSnapshots);
                    final Semaphore repositoryPermits = trackedSnapshot.trackedRepository.permits;
                    if (repositoryPermits.tryAcquire() == false) {
                        startCloner();
                        return;
                    }
                    localReleasables.add(Releasables.releaseOnce(repositoryPermits::release));

                    final Semaphore snapshotPermits = trackedSnapshot.permits;
                    if (snapshotPermits.tryAcquire() == false) {
                        startCloner();
                        return;
                    }
                    localReleasables.add(Releasables.releaseOnce(snapshotPermits::release));

                    if (snapshots.get(trackedSnapshot.snapshotName) != trackedSnapshot) {
                        // concurrently removed
                        startCloner();
                        return;
                    }

                    final Releasable[] finalReleasables = localReleasables.toArray(new Releasable[0]);
                    final Releasable releaseAll = () -> Releasables.close(finalReleasables);

                    final String cloneName = "snapshot-clone-" + snapshotCounter.incrementAndGet();

                    logger.info(
                        "--> starting clone of [{}:{}] as [{}:{}]",
                        trackedSnapshot.trackedRepository.repositoryName,
                        trackedSnapshot.snapshotName,
                        trackedSnapshot.trackedRepository.repositoryName,
                        cloneName);

                    client().admin().cluster().prepareCloneSnapshot(
                        trackedSnapshot.trackedRepository.repositoryName,
                        trackedSnapshot.snapshotName,
                        cloneName).setIndices("*").execute(new ActionListener<>() {

                        @Override
                        public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                            assertTrue(acknowledgedResponse.isAcknowledged());
                            logger.info(
                                "--> completed clone of [{}:{}] as [{}:{}]",
                                trackedSnapshot.trackedRepository.repositoryName,
                                trackedSnapshot.snapshotName,
                                trackedSnapshot.trackedRepository.repositoryName,
                                cloneName);
                            startCloner();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            Releasables.close(releaseAll);
                            throw new AssertionError("unexpected", e);
                        }
                    });

                    localReleasables.clear();
                } finally {
                    Releasables.close(localReleasables);
                }
            }));
        }

        private void startSnapshotDeleter() {
            threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueMillis(between(1, 500)), CLIENT, mustSucceed(() -> {
                final List<Releasable> localReleasables = new ArrayList<>();

                try {
                    final List<TrackedSnapshot> trackedSnapshots = new ArrayList<>(snapshots.values());
                    if (trackedSnapshots.isEmpty()) {
                        startCloner();
                        return;
                    }

                    final Releasable nodeRestartBlock = blockNodeRestarts();
                    if (nodeRestartBlock == null) {
                        startSnapshotDeleter();
                        return;
                    }
                    localReleasables.add(nodeRestartBlock);

                    final TrackedSnapshot trackedSnapshot = randomFrom(trackedSnapshots);
                    final Semaphore repositoryPermits = trackedSnapshot.trackedRepository.permits;
                    if (repositoryPermits.tryAcquire() == false) {
                        startSnapshotDeleter();
                        return;
                    }
                    localReleasables.add(Releasables.releaseOnce(repositoryPermits::release));

                    final Semaphore snapshotPermits = trackedSnapshot.permits;
                    if (snapshotPermits.tryAcquire(Integer.MAX_VALUE) == false) {
                        startSnapshotDeleter();
                        return;
                    }
                    localReleasables.add(Releasables.releaseOnce(() -> snapshotPermits.release(Integer.MAX_VALUE)));

                    if (snapshots.get(trackedSnapshot.snapshotName) != trackedSnapshot) {
                        // concurrently removed
                        startSnapshotDeleter();
                        return;
                    }

                    final Releasable[] finalReleasables = localReleasables.toArray(new Releasable[0]);
                    final Releasable releaseAll = () -> Releasables.close(finalReleasables);

                    logger.info(
                        "--> starting deletion of [{}:{}]",
                        trackedSnapshot.trackedRepository.repositoryName,
                        trackedSnapshot.snapshotName);

                    client().admin().cluster().prepareDeleteSnapshot(
                        trackedSnapshot.trackedRepository.repositoryName,
                        trackedSnapshot.snapshotName).execute(new ActionListener<>() {

                        @Override
                        public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                            assertTrue(acknowledgedResponse.isAcknowledged());
                            assertThat(snapshots.remove(trackedSnapshot.snapshotName), sameInstance(trackedSnapshot));
                            logger.info(
                                "--> completed deletion of [{}:{}]",
                                trackedSnapshot.trackedRepository.repositoryName,
                                trackedSnapshot.snapshotName);
                            startSnapshotDeleter();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            Releasables.close(releaseAll);
                            throw new AssertionError("unexpected", e);
                        }
                    });

                    localReleasables.clear();
                } finally {
                    Releasables.close(localReleasables);
                }
            }));
        }

        private void startSnapshotter() {
            threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueMillis(between(1, 500)), CLIENT, mustSucceed(() -> {
                final List<Releasable> localReleasables = new ArrayList<>();

                try {
                    final Releasable nodeRestartBlock = blockNodeRestarts();
                    if (nodeRestartBlock == null) {
                        startSnapshotter();
                        return;
                    }
                    localReleasables.add(nodeRestartBlock);

                    final TrackedRepository trackedRepository = randomFrom(repositories.values());
                    final Semaphore repositoryPermits = trackedRepository.permits;
                    if (repositoryPermits.tryAcquire() == false) {
                        startSnapshotter();
                        return;
                    }
                    localReleasables.add(Releasables.releaseOnce(repositoryPermits::release));

                    boolean snapshotSpecificIndicesTmp = randomBoolean();
                    final List<String> targetIndexNames = new ArrayList<>(indices.size());
                    for (TrackedIndex trackedIndex : indices.values()) {
                        final Semaphore indexPermits = trackedIndex.permits;
                        if (usually() && indexPermits.tryAcquire()) {
                            targetIndexNames.add(trackedIndex.indexName);
                            localReleasables.add(Releasables.releaseOnce(indexPermits::release));
                        } else {
                            snapshotSpecificIndicesTmp = true;
                        }
                    }
                    final boolean snapshotSpecificIndices = snapshotSpecificIndicesTmp;

                    final Releasable[] finalReleasables = localReleasables.toArray(new Releasable[0]);
                    final Releasable releaseAll = () -> Releasables.close(finalReleasables);

                    final StepListener<ClusterHealthResponse> ensureYellowStep = new StepListener<>();

                    final String snapshotName = "snapshot-" + snapshotCounter.incrementAndGet();

                    logger.info(
                        "--> waiting for yellow health of [{}] before creating snapshot [{}:{}]",
                        targetIndexNames,
                        trackedRepository.repositoryName,
                        snapshotName);

                    client().admin().cluster().prepareHealth(targetIndexNames.toArray(new String[0]))
                        .setWaitForEvents(Priority.LANGUID)
                        .setWaitForYellowStatus()
                        .setWaitForNodes(Integer.toString(internalCluster().getNodeNames().length))
                        .execute(ensureYellowStep);

                    ensureYellowStep.whenComplete(clusterHealthResponse -> {
                        Releasable localReleasable = releaseAll;

                        try {
                            assertFalse("timed out waiting for yellow state of " + targetIndexNames, clusterHealthResponse.isTimedOut());

                            logger.info(
                                "--> take snapshot [{}:{}] with indices [{}{}]",
                                trackedRepository.repositoryName,
                                snapshotName,
                                snapshotSpecificIndices ? "*=" : "",
                                targetIndexNames);

                            final CreateSnapshotRequestBuilder createSnapshotRequestBuilder
                                = client().admin().cluster().prepareCreateSnapshot(trackedRepository.repositoryName, snapshotName);

                            if (snapshotSpecificIndices) {
                                createSnapshotRequestBuilder.setIndices(targetIndexNames.toArray(new String[0]));
                            }

                            if (randomBoolean()) {
                                createSnapshotRequestBuilder.setWaitForCompletion(true);
                                createSnapshotRequestBuilder.execute(new ActionListener<>() {
                                    @Override
                                    public void onResponse(CreateSnapshotResponse createSnapshotResponse) {
                                        logger.info("--> completed snapshot [{}:{}]", trackedRepository.repositoryName, snapshotName);
                                        assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));
                                        Releasables.close(releaseAll);
                                        startSnapshotter();
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        Releasables.close(releaseAll);
                                        throw new AssertionError("unexpected", e);
                                    }
                                });
                            } else {
                                createSnapshotRequestBuilder.execute(new ActionListener<>() {
                                    @Override
                                    public void onResponse(CreateSnapshotResponse createSnapshotResponse) {
                                        logger.info("--> started snapshot [{}:{}]", trackedRepository.repositoryName, snapshotName);
                                        pollForSnapshotCompletion(
                                            trackedRepository.repositoryName,
                                            snapshotName,
                                            releaseAll,
                                            () -> {
                                                snapshots.put(snapshotName, new TrackedSnapshot(trackedRepository, snapshotName));
                                                startSnapshotter();
                                            });
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        Releasables.close(releaseAll);
                                        throw new AssertionError("unexpected", e);
                                    }
                                });
                            }

                            localReleasable = null;
                        } finally {
                            Releasables.close(localReleasable);
                        }
                    }, e -> {
                        Releasables.close(releaseAll);
                        throw new AssertionError("unexpected", e);
                    });

                    localReleasables.clear();
                } finally {
                    Releasables.close(localReleasables);
                }
            }));
        }

        private void pollForSnapshotCompletion(String repositoryName, String snapshotName, Releasable onCompletion, Runnable onSuccess) {
            threadPool.executor(CLIENT).execute(mustSucceed(() -> {
                Releasable localReleasable = onCompletion;
                try {
                    client().admin().cluster().prepareGetSnapshots(repositoryName).setCurrentSnapshot().execute(new ActionListener<>() {
                        @Override
                        public void onResponse(GetSnapshotsResponse getSnapshotsResponse) {
                            if (getSnapshotsResponse.getSnapshots().stream()
                                .noneMatch(snapshotInfo -> snapshotInfo.snapshotId().getName().equals(snapshotName))) {

                                logger.info("--> snapshot [{}:{}] no longer running", repositoryName, snapshotName);
                                Releasables.close(onCompletion);
                                onSuccess.run();
                            } else {
                                pollForSnapshotCompletion(repositoryName, snapshotName, onCompletion, onSuccess);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            Releasables.close(onCompletion);
                            throw new AssertionError("unexpected", e);
                        }
                    });

                    localReleasable = null;
                } finally {
                    Releasables.close(localReleasable);
                }
            }));

        }

        void stop() {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }

        @Nullable // if we couldn't block node restarts
        private Releasable blockNodeRestarts() {
            final List<Releasable> nodeBlocks = new ArrayList<>(nodes.size());
            try {
                for (TrackedNode trackedNode : nodes.values()) {
                    final Semaphore permits = trackedNode.getPermits();
                    if (permits.tryAcquire(1)) {
                        nodeBlocks.add(permits::release);
                    } else {
                        return null;
                    }
                }

                final Releasable[] finalNodeBlocks = nodeBlocks.toArray(new Releasable[0]);
                nodeBlocks.clear();
                return Releasables.releaseOnce(() -> Releasables.close(finalNodeBlocks));
            } finally {
                Releasables.close(nodeBlocks);
            }
        }

        private AbstractRunnable mustSucceed(CheckedRunnable<Exception> runnable) {
            return new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError("unexpected", e);
                }

                @Override
                protected void doRun() throws Exception {
                    runnable.run();
                }

                @Override
                public void onRejection(Exception e) {
                    // ok, shutting down
                }
            };
        }

        /**
         * Tracks a node in the cluster, and occasionally restarts it if no other activity holds any of its permits.
         */
        private class TrackedNode {

            private final Semaphore permits = new Semaphore(Integer.MAX_VALUE);
            private final String nodeName;

            TrackedNode(String nodeName) {
                this.nodeName = nodeName;
            }

            void start() {
                threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueMillis(between(1, 500)), CLIENT, mustSucceed(() -> {
                    if (rarely() && permits.tryAcquire(Integer.MAX_VALUE)) {
                        threadPool.generic().execute(() -> {
                            try {
                                logger.info("--> restarting [{}]", nodeName);
                                cluster.restartNode(nodeName);
                            } catch (Exception e) {
                                throw new AssertionError("unexpected", e);
                            } finally {
                                logger.info("--> finished restarting [{}]", nodeName);
                                permits.release(Integer.MAX_VALUE);
                                start();
                            }
                        });
                    } else {
                        start();
                    }
                }));
            }

            Semaphore getPermits() {
                return permits;
            }

            @Override
            public String toString() {
                return "TrackedNode[" + nodeName + "]";
            }
        }

        /**
         * Tracks a repository in the cluster, and occasionally removes it and adds it back if no other activity holds any of its permits.
         */
        private class TrackedRepository {

            private final Semaphore permits = new Semaphore(Integer.MAX_VALUE);
            private final String repositoryName;
            private final Path location;

            private TrackedRepository(String repositoryName, Path location) {
                this.repositoryName = repositoryName;
                this.location = location;
            }

            @Override
            public String toString() {
                return "TrackedRepository[" + repositoryName + "]";
            }

            public void start() {
                final Releasable nodeRestartBlock = blockNodeRestarts();
                assertNotNull(nodeRestartBlock);
                assertTrue(permits.tryAcquire(Integer.MAX_VALUE));
                final Releasable releaseRepository = releaseAllPermits();
                putRepositoryAndContinue(() -> Releasables.close(releaseRepository, nodeRestartBlock));
            }

            private Releasable releaseAllPermits() {
                return Releasables.releaseOnce(() -> permits.release(Integer.MAX_VALUE));
            }

            private void putRepositoryAndContinue(Releasable releasable) {
                logger.info("--> put repo [{}]", repositoryName);
                client().admin().cluster().preparePutRepository(repositoryName).setType(FsRepository.TYPE)
                    .setSettings(Settings.builder().put(FsRepository.LOCATION_SETTING.getKey(), location))
                    .execute(new ActionListener<>() {
                        @Override
                        public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                            assertTrue(acknowledgedResponse.isAcknowledged());
                            logger.info("--> finished put repo [{}]", repositoryName);
                            Releasables.close(releasable);
                            scheduleRemoveAndAdd();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            Releasables.close(releasable);
                            throw new AssertionError("unexpected", e);
                        }
                    });
            }

            private void scheduleRemoveAndAdd() {
                threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueMillis(between(1, 500)), CLIENT, mustSucceed(() -> {
                    if (rarely() && permits.tryAcquire(Integer.MAX_VALUE)) {

                        final Releasable nodeRestartBlock = blockNodeRestarts();
                        if (nodeRestartBlock == null) {
                            // TODO maybe attempt to remove + add repo even if nodes are restarting?
                            // but what if client node is shut down after sending request to master, we'd get a failure but the delete
                            // might still go through later, perhaps after we tried to re-add it
                            permits.release(Integer.MAX_VALUE);
                            scheduleRemoveAndAdd();
                            return;
                        }
                        final Releasable releaseRepository = releaseAllPermits();
                        final Releasable releaseAll = () -> Releasables.close(nodeRestartBlock, releaseRepository);
                        Releasable localReleasable = releaseAll;
                        try {
                            logger.info("--> delete repo [{}]", repositoryName);
                            client().admin().cluster().prepareDeleteRepository(repositoryName).execute(new ActionListener<>() {
                                @Override
                                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                                    assertTrue(acknowledgedResponse.isAcknowledged());
                                    logger.info("--> finished delete repo [{}]", repositoryName);
                                    putRepositoryAndContinue(releaseAll);
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    Releasables.close(releaseAll);
                                    throw new AssertionError("unexpected", e);
                                }
                            });
                            localReleasable = null;
                        } finally {
                            Releasables.close(localReleasable);
                        }
                    } else {
                        scheduleRemoveAndAdd();
                    }
                }));
            }

        }

        private class TrackedIndex {

            private final Semaphore permits = new Semaphore(Integer.MAX_VALUE);
            private final String indexName;

            private TrackedIndex(String indexName) {
                this.indexName = indexName;
            }


            @Override
            public String toString() {
                return "TrackedIndex[" + indexName + "]";
            }

            public void start() {
                final Releasable nodeRestartBlock = blockNodeRestarts();
                assertNotNull(nodeRestartBlock);
                assertTrue(permits.tryAcquire(Integer.MAX_VALUE));
                final Releasable releaseRepository = releaseAllPermits();
                createIndexAndContinue(() -> Releasables.close(releaseRepository, nodeRestartBlock));
            }

            private Releasable releaseAllPermits() {
                return Releasables.releaseOnce(() -> permits.release(Integer.MAX_VALUE));
            }

            private void createIndexAndContinue(Releasable releasable) {
                logger.info("--> create index [{}]", indexName);
                client().admin().indices().prepareCreate(indexName)
                    .setSettings(Settings.builder()
                        .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), between(1, 5))
                        .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), between(0, cluster.numDataNodes() - 1)))
                    .execute(new ActionListener<>() {
                        @Override
                        public void onResponse(CreateIndexResponse response) {
                            assertTrue(response.isAcknowledged());
                            logger.info("--> finished create index [{}]", indexName);
                            Releasables.close(releasable);
                            scheduleIndexingAndPossibleDelete();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            Releasables.close(releasable);
                            throw new AssertionError("unexpected", e);
                        }
                    });
            }

            private void scheduleIndexingAndPossibleDelete() {
                threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueMillis(between(1, 500)), CLIENT, mustSucceed(() -> {
                    if (usually()) {
                        assertTrue(permits.tryAcquire()); // nobody else should be blocking this index
                        final Releasable nodeRestartBlock = blockNodeRestarts();
                        if (nodeRestartBlock == null) {
                            permits.release();
                            scheduleIndexingAndPossibleDelete();
                            return;
                        }

                        final Releasable releaseIndex = Releasables.releaseOnce(permits::release);
                        final Releasable releaseAll = () -> Releasables.close(nodeRestartBlock, releaseIndex);
                        Releasable localReleasable = releaseAll;
                        final Consumer<Exception> onFailure = e -> {
                            Releasables.close(releaseAll);
                            throw new AssertionError("unexpected", e);
                        };

                        try {

                            final StepListener<ClusterHealthResponse> ensureYellowStep = new StepListener<>();

                            logger.info("--> waiting for yellow health of [{}]", indexName);

                            client().admin().cluster().prepareHealth(indexName)
                                .setWaitForEvents(Priority.LANGUID)
                                .setWaitForYellowStatus()
                                .setWaitForNodes(Integer.toString(internalCluster().getNodeNames().length))
                                .execute(ensureYellowStep);

                            final StepListener<BulkResponse> bulkStep = new StepListener<>();

                            ensureYellowStep.whenComplete(clusterHealthResponse -> {

                                if (clusterHealthResponse.isTimedOut()) {
                                    Releasables.close(releaseAll);
                                    throw new AssertionError("timed out waiting for yellow state of [" + indexName + "]");
                                }

                                final int docCount = between(1, 1000);
                                final BulkRequestBuilder bulkRequestBuilder = client().prepareBulk(indexName);

                                logger.info("--> indexing [{}] docs into [{}]", docCount, indexName);

                                for (int i = 0; i < docCount; i++) {
                                    bulkRequestBuilder.add(new IndexRequest().source(jsonBuilder()
                                        .startObject().field("field-" + between(1, 5), randomAlphaOfLength(10)).endObject()));
                                }

                                bulkRequestBuilder.execute(bulkStep);
                            }, onFailure);

                            bulkStep.whenComplete(bulkItemResponses -> {
                                for (BulkItemResponse bulkItemResponse : bulkItemResponses.getItems()) {
                                    assertNull(bulkItemResponse.getFailure());
                                }

                                logger.info("--> indexing into [{}] finished", indexName);

                                Releasables.close(releaseAll);
                                scheduleIndexingAndPossibleDelete();

                            }, onFailure);

                            localReleasable = null;
                        } finally {
                            Releasables.close(localReleasable);
                        }

                    } else if (permits.tryAcquire(Integer.MAX_VALUE)) {
                        final Releasable nodeRestartBlock = blockNodeRestarts();
                        if (nodeRestartBlock == null) {
                            permits.release(Integer.MAX_VALUE);
                            scheduleIndexingAndPossibleDelete();
                            return;
                        }

                        final Releasable releaseIndex = releaseAllPermits();
                        final Releasable releaseAll = () -> Releasables.close(nodeRestartBlock, releaseIndex);
                        Releasable localReleasable = releaseAll;

                        try {
                            logger.info("--> deleting index [{}]", indexName);

                            client().admin().indices().prepareDelete(indexName).execute(new ActionListener<>() {
                                @Override
                                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                                    logger.info("--> deleting index [{}] finished", indexName);
                                    assertTrue(acknowledgedResponse.isAcknowledged());
                                    createIndexAndContinue(releaseAll);
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    Releasables.close(releaseAll);
                                    throw new AssertionError("unexpected", e);
                                }
                            });

                            localReleasable = null;
                        } finally {
                            Releasables.close(localReleasable);
                        }
                    }
                }));
            }

        }

        private class TrackedSnapshot {

            private final TrackedRepository trackedRepository;
            private final String snapshotName;
            private final Semaphore permits = new Semaphore(Integer.MAX_VALUE);

            public TrackedSnapshot(TrackedRepository trackedRepository, String snapshotName) {
                this.trackedRepository = trackedRepository;
                this.snapshotName = snapshotName;
            }
        }

    }

}
