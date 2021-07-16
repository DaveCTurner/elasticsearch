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
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.CheckedConsumer;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;

public class SnapshotRestoreRandomlyIT extends AbstractSnapshotIntegTestCase {

    public void testRandomActivities() throws InterruptedException {
        final DiscoveryNodes discoveryNodes = client().admin().cluster().prepareState().clear().setNodes(true).get().getState().nodes();
        final Set<String> masterNodeNames = StreamSupport.stream(discoveryNodes.getMasterNodes().keys().spliterator(), false)
            .map(c -> c.value)
            .collect(Collectors.toSet());
        final Set<String> dataNodeNames = StreamSupport.stream(discoveryNodes.getDataNodes().keys().spliterator(), false)
            .map(c -> c.value)
            .collect(Collectors.toSet());

        new TrackedCluster(internalCluster(), masterNodeNames, dataNodeNames).run();
        disableRepoConsistencyCheck("have not necessarily written to all repositories");
    }

    /**
     * Encapsulates a common pattern of trying to acquire a bunch of resources and then transferring ownership elsewhere on success,
     * but releasing them on failure
     */
    private static class TransferableReleasables implements Releasable {

        private boolean transferred = false;
        private final List<Releasable> releasables = new ArrayList<>();

        <T extends Releasable> T add(T releasable) {
            assert transferred == false : "already transferred";
            releasables.add(releasable);
            return releasable;
        }

        Releasable transfer() {
            assert transferred == false : "already transferred";
            transferred = true;
            return () -> Releasables.close(releasables);
        }

        @Override
        public void close() {
            if (transferred == false) {
                Releasables.close(releasables);
            }
        }
    }

    @Nullable // if no permit was acquired
    private static Releasable tryAcquirePermit(Semaphore permits) {
        if (permits.tryAcquire()) {
            return Releasables.releaseOnce(permits::release);
        } else {
            return null;
        }
    }

    @Nullable // if not all permits were acquired
    private static Releasable tryAcquireAllPermits(Semaphore permits) {
        if (permits.tryAcquire(Integer.MAX_VALUE)) {
            return Releasables.releaseOnce(() -> permits.release(Integer.MAX_VALUE));
        } else {
            return null;
        }
    }

    private static AbstractRunnable mustSucceed(CheckedRunnable<Exception> runnable) {
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

    private static <T> ActionListener<T> mustSucceed(CheckedConsumer<T, Exception> consumer) {
        return new ActionListener<>() {
            @Override
            public void onResponse(T t) {
                try {
                    consumer.accept(t);
                } catch (Exception e) {
                    throw new AssertionError("unexpected", e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("unexpected", e);
            }
        };
    }

    private static class TrackedCluster {

        static final Logger logger = LogManager.getLogger(TrackedCluster.class);
        static final String CLIENT = "client";

        private final ThreadPool threadPool = new TestThreadPool(
            "TrackedCluster",
            // a single thread for "client" activities, to limit the number of activities all starting at once
            new ScalingExecutorBuilder(CLIENT, 1, 1, TimeValue.ZERO, CLIENT)
        );

        private final InternalTestCluster cluster;
        private final Map<String, TrackedNode> nodes = ConcurrentCollections.newConcurrentMap();
        private final Map<String, TrackedRepository> repositories = ConcurrentCollections.newConcurrentMap();
        private final Map<String, TrackedIndex> indices = ConcurrentCollections.newConcurrentMap();
        private final Map<String, TrackedSnapshot> snapshots = ConcurrentCollections.newConcurrentMap();

        private final AtomicInteger snapshotCounter = new AtomicInteger();
        private final CountDownLatch completedSnapshotLatch = new CountDownLatch(30);

        TrackedCluster(InternalTestCluster cluster, Set<String> masterNodeNames, Set<String> dataNodeNames) {
            this.cluster = cluster;
            for (String nodeName : cluster.getNodeNames()) {
                nodes.put(nodeName, new TrackedNode(nodeName, masterNodeNames.contains(nodeName), dataNodeNames.contains(nodeName)));
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

        public void run() throws InterruptedException {
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

            if (completedSnapshotLatch.await(30, TimeUnit.SECONDS) == false) {
                logger.info("--> did not complete target snapshot count in 30s, giving up");
            }

            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }

        private void startCloner() {
            threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueMillis(between(1, 500)), CLIENT, mustSucceed(() -> {

                boolean startedClone = false;
                try (TransferableReleasables localReleasables = new TransferableReleasables()) {

                    final List<TrackedSnapshot> trackedSnapshots = new ArrayList<>(snapshots.values());
                    if (trackedSnapshots.isEmpty()) {
                        return;
                    }

                    if (localReleasables.add(blockNodeRestarts()) == null) {
                        return;
                    }

                    final Client client = localReleasables.add(acquireClient()).getClient();

                    final TrackedSnapshot trackedSnapshot = randomFrom(trackedSnapshots);
                    if (localReleasables.add(tryAcquirePermit(trackedSnapshot.trackedRepository.permits)) == null) {
                        return;
                    }

                    if (localReleasables.add(tryAcquirePermit(trackedSnapshot.permits)) == null) {
                        return;
                    }

                    if (snapshots.get(trackedSnapshot.snapshotName) != trackedSnapshot) {
                        // concurrently removed
                        return;
                    }

                    final Releasable releaseAll = localReleasables.transfer();

                    final StepListener<List<String>> getIndicesStep = new StepListener<>();

                    if (randomBoolean()) {
                        getIndicesStep.onResponse(Collections.singletonList("*"));
                    } else {

                        logger.info(
                            "--> listing indices in [{}:{}] in preparation for cloning",
                            trackedSnapshot.trackedRepository.repositoryName,
                            trackedSnapshot.snapshotName
                        );

                        client.admin()
                            .cluster()
                            .prepareGetSnapshots(trackedSnapshot.trackedRepository.repositoryName)
                            .setSnapshots(trackedSnapshot.snapshotName)
                            .execute(mustSucceed(getSnapshotsResponse -> {
                                assertThat(getSnapshotsResponse.getSnapshots(), hasSize(1));
                                final SnapshotInfo snapshotInfo = getSnapshotsResponse.getSnapshots().get(0);
                                assertThat(snapshotInfo.snapshotId().getName(), equalTo(trackedSnapshot.snapshotName));

                                final List<String> indexNames = new ArrayList<>();
                                for (String indexName : snapshotInfo.indices()) {
                                    if (randomBoolean()) {
                                        indexNames.add(indexName);
                                    }
                                }

                                if (indexNames.isEmpty()) {
                                    indexNames.add("*");
                                }

                                getIndicesStep.onResponse(indexNames);
                            }));
                    }

                    getIndicesStep.addListener(mustSucceed(indexNames -> {

                        final String cloneName = "snapshot-clone-" + snapshotCounter.incrementAndGet();

                        logger.info(
                            "--> starting clone of [{}:{}] as [{}:{}] with indices {}",
                            trackedSnapshot.trackedRepository.repositoryName,
                            trackedSnapshot.snapshotName,
                            trackedSnapshot.trackedRepository.repositoryName,
                            cloneName,
                            indexNames
                        );

                        client.admin()
                            .cluster()
                            .prepareCloneSnapshot(trackedSnapshot.trackedRepository.repositoryName, trackedSnapshot.snapshotName, cloneName)
                            .setIndices(indexNames.toArray(new String[0]))
                            .execute(mustSucceed(acknowledgedResponse -> {
                                Releasables.close(releaseAll);
                                assertTrue(acknowledgedResponse.isAcknowledged());
                                completedSnapshotLatch.countDown();
                                logger.info(
                                    "--> completed clone of [{}:{}] as [{}:{}]",
                                    trackedSnapshot.trackedRepository.repositoryName,
                                    trackedSnapshot.snapshotName,
                                    trackedSnapshot.trackedRepository.repositoryName,
                                    cloneName
                                );
                                startCloner();
                            }));
                    }));

                    startedClone = true;
                } finally {
                    if (startedClone == false) {
                        startCloner();
                    }
                }
            }));
        }

        private void startSnapshotDeleter() {
            threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueMillis(between(1, 500)), CLIENT, mustSucceed(() -> {

                boolean startedDeletion = false;
                try (TransferableReleasables localReleasables = new TransferableReleasables()) {

                    final List<TrackedSnapshot> trackedSnapshots = new ArrayList<>(snapshots.values());
                    if (trackedSnapshots.isEmpty()) {
                        return;
                    }

                    if (localReleasables.add(blockNodeRestarts()) == null) {
                        return;
                    }

                    final Client client = localReleasables.add(acquireClient()).getClient();

                    final TrackedSnapshot trackedSnapshot = randomFrom(trackedSnapshots);
                    if (localReleasables.add(tryAcquirePermit(trackedSnapshot.trackedRepository.permits)) == null) {
                        return;
                    }

                    if (localReleasables.add(tryAcquireAllPermits(trackedSnapshot.permits)) == null) {
                        return;
                    }

                    if (snapshots.get(trackedSnapshot.snapshotName) != trackedSnapshot) {
                        // concurrently removed
                        return;
                    }

                    logger.info(
                        "--> starting deletion of [{}:{}]",
                        trackedSnapshot.trackedRepository.repositoryName,
                        trackedSnapshot.snapshotName
                    );

                    final Releasable releaseAll = localReleasables.transfer();

                    client.admin()
                        .cluster()
                        .prepareDeleteSnapshot(trackedSnapshot.trackedRepository.repositoryName, trackedSnapshot.snapshotName)
                        .execute(mustSucceed(acknowledgedResponse -> {
                            Releasables.close(releaseAll);
                            assertTrue(acknowledgedResponse.isAcknowledged());
                            assertThat(snapshots.remove(trackedSnapshot.snapshotName), sameInstance(trackedSnapshot));
                            logger.info(
                                "--> completed deletion of [{}:{}]",
                                trackedSnapshot.trackedRepository.repositoryName,
                                trackedSnapshot.snapshotName
                            );
                            startSnapshotDeleter();
                        }));

                    startedDeletion = true;

                } finally {
                    if (startedDeletion == false) {
                        startSnapshotDeleter();
                    }
                }
            }));
        }

        private void startSnapshotter() {
            threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueMillis(between(1, 500)), CLIENT, mustSucceed(() -> {

                boolean startedSnapshot = false;
                try (TransferableReleasables localReleasables = new TransferableReleasables()) {

                    if (localReleasables.add(blockNodeRestarts()) == null) {
                        return;
                    }

                    final TrackedRepository trackedRepository = randomFrom(repositories.values());
                    if (localReleasables.add(tryAcquirePermit(trackedRepository.permits)) == null) {
                        return;
                    }

                    boolean snapshotSpecificIndicesTmp = randomBoolean();
                    final List<String> targetIndexNames = new ArrayList<>(indices.size());
                    for (TrackedIndex trackedIndex : indices.values()) {
                        if (usually() && localReleasables.add(tryAcquirePermit(trackedIndex.permits)) != null) {
                            targetIndexNames.add(trackedIndex.indexName);
                        } else {
                            snapshotSpecificIndicesTmp = true;
                        }
                    }
                    final boolean snapshotSpecificIndices = snapshotSpecificIndicesTmp;

                    if (snapshotSpecificIndices && targetIndexNames.isEmpty()) {
                        return;
                    }

                    final Releasable releaseAll = localReleasables.transfer();

                    final StepListener<ClusterHealthResponse> ensureYellowStep = new StepListener<>();

                    final String snapshotName = "snapshot-" + snapshotCounter.incrementAndGet();

                    logger.info(
                        "--> waiting for yellow health of [{}] before creating snapshot [{}:{}]",
                        targetIndexNames,
                        trackedRepository.repositoryName,
                        snapshotName
                    );

                    client().admin()
                        .cluster()
                        .prepareHealth(targetIndexNames.toArray(new String[0]))
                        .setWaitForEvents(Priority.LANGUID)
                        .setWaitForYellowStatus()
                        .setWaitForNodes(Integer.toString(internalCluster().getNodeNames().length))
                        .execute(ensureYellowStep);

                    ensureYellowStep.addListener(mustSucceed(clusterHealthResponse -> {
                        assertFalse("timed out waiting for yellow state of " + targetIndexNames, clusterHealthResponse.isTimedOut());

                        logger.info(
                            "--> take snapshot [{}:{}] with indices [{}{}]",
                            trackedRepository.repositoryName,
                            snapshotName,
                            snapshotSpecificIndices ? "" : "*=",
                            targetIndexNames
                        );

                        final CreateSnapshotRequestBuilder createSnapshotRequestBuilder = client().admin()
                            .cluster()
                            .prepareCreateSnapshot(trackedRepository.repositoryName, snapshotName);

                        if (snapshotSpecificIndices) {
                            createSnapshotRequestBuilder.setIndices(targetIndexNames.toArray(new String[0]));
                        }

                        if (randomBoolean()) {
                            createSnapshotRequestBuilder.setWaitForCompletion(true);
                            createSnapshotRequestBuilder.execute(mustSucceed(createSnapshotResponse -> {
                                logger.info("--> completed snapshot [{}:{}]", trackedRepository.repositoryName, snapshotName);
                                assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));
                                Releasables.close(releaseAll);
                                completedSnapshotLatch.countDown();
                                startSnapshotter();
                            }));
                        } else {
                            createSnapshotRequestBuilder.execute(mustSucceed(createSnapshotResponse -> {
                                logger.info("--> started snapshot [{}:{}]", trackedRepository.repositoryName, snapshotName);
                                pollForSnapshotCompletion(trackedRepository.repositoryName, snapshotName, releaseAll, () -> {
                                    snapshots.put(snapshotName, new TrackedSnapshot(trackedRepository, snapshotName));
                                    completedSnapshotLatch.countDown();
                                    startSnapshotter();
                                });
                            }));
                        }
                    }));

                    startedSnapshot = true;
                } finally {
                    if (startedSnapshot == false) {
                        startSnapshotter();
                    }
                }
            }));
        }

        private void pollForSnapshotCompletion(String repositoryName, String snapshotName, Releasable onCompletion, Runnable onSuccess) {
            threadPool.executor(CLIENT)
                .execute(
                    mustSucceed(
                        () -> client().admin()
                            .cluster()
                            .prepareGetSnapshots(repositoryName)
                            .setCurrentSnapshot()
                            .execute(mustSucceed(getSnapshotsResponse -> {
                                if (getSnapshotsResponse.getSnapshots()
                                    .stream()
                                    .noneMatch(snapshotInfo -> snapshotInfo.snapshotId().getName().equals(snapshotName))) {

                                    logger.info("--> snapshot [{}:{}] no longer running", repositoryName, snapshotName);
                                    Releasables.close(onCompletion);
                                    onSuccess.run();
                                } else {
                                    pollForSnapshotCompletion(repositoryName, snapshotName, onCompletion, onSuccess);
                                }
                            }))
                    )
                );
        }

        @Nullable // if we couldn't block node restarts
        private Releasable blockNodeRestarts() {
            try (TransferableReleasables localReleasables = new TransferableReleasables()) {
                for (TrackedNode trackedNode : nodes.values()) {
                    if (localReleasables.add(tryAcquirePermit(trackedNode.getPermits())) == null) {
                        return null;
                    }
                }
                return localReleasables.transfer();
            }
        }

        private ReleasableClient acquireClient() {
            final ReleasableClient client = tryAcquireClient();
            assertNotNull(client);
            return client;
        }

        @Nullable // if we couldn't block the restart of the client node
        private ReleasableClient tryAcquireClient() {
            final ArrayList<TrackedNode> trackedNodes = new ArrayList<>(nodes.values());
            Randomness.shuffle(trackedNodes);
            for (TrackedNode trackedNode : trackedNodes) {
                final Releasable permit = tryAcquirePermit(trackedNode.getPermits());
                if (permit != null) {
                    return new ReleasableClient(permit, client(trackedNode.nodeName));
                }
            }

            return null;
        }

        /**
         * Try and block the restart of a majority of the master nodes, which therefore prevents a full-cluster restart from occurring.
         */
        @Nullable // if we couldn't block enough master node restarts
        private Releasable blockFullClusterRestart() {
            final List<TrackedNode> masterNodes = nodes.values().stream().filter(TrackedNode::isMasterNode).collect(Collectors.toList());
            Randomness.shuffle(masterNodes);
            int permitsAcquired = 0;
            try (TransferableReleasables localReleasables = new TransferableReleasables()) {
                for (TrackedNode trackedNode : masterNodes) {
                    if (localReleasables.add(tryAcquirePermit(trackedNode.getPermits())) != null) {
                        permitsAcquired += 1;
                        if (masterNodes.size() < permitsAcquired * 2) {
                            return localReleasables.transfer();
                        }
                    }
                }
                return null;
            }
        }

        private static class ReleasableClient implements Releasable {
            private final Releasable releasable;
            private final Client client;

            ReleasableClient(Releasable releasable, Client client) {
                this.releasable = releasable;
                this.client = client;
            }

            @Override
            public void close() {
                releasable.close();
            }

            Client getClient() {
                return client;
            }
        }

        /**
         * Tracks a node in the cluster, and occasionally restarts it if no other activity holds any of its permits.
         */
        private class TrackedNode {

            private final Semaphore permits = new Semaphore(Integer.MAX_VALUE);
            private final String nodeName;
            private final boolean isMasterNode;
            private final boolean isDataNode;

            TrackedNode(String nodeName, boolean isMasterNode, boolean isDataNode) {
                this.nodeName = nodeName;
                this.isMasterNode = isMasterNode;
                this.isDataNode = isDataNode;
            }

            void start() {
                threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueMillis(between(1, 500)), CLIENT, mustSucceed(() -> {

                    boolean restarting = false;
                    try (TransferableReleasables localReleasables = new TransferableReleasables()) {

                        if (usually()) {
                            return;
                        }

                        if (localReleasables.add(tryAcquireAllPermits(permits)) == null) {
                            return;
                        }

                        final Releasable releaseAll = localReleasables.transfer();

                        threadPool.generic().execute(mustSucceed(() -> {
                            try {
                                logger.info("--> restarting [{}]", nodeName);
                                cluster.restartNode(nodeName);
                            } finally {
                                logger.info("--> finished restarting [{}]", nodeName);
                                Releasables.close(releaseAll);
                                start();
                            }
                        }));

                        restarting = true;
                    } finally {
                        if (restarting == false) {
                            start();
                        }
                    }
                }));
            }

            Semaphore getPermits() {
                return permits;
            }

            boolean isMasterNode() {
                return isMasterNode;
            }

            boolean isDataNode() {
                return isDataNode;
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
                client().admin()
                    .cluster()
                    .preparePutRepository(repositoryName)
                    .setType(FsRepository.TYPE)
                    .setSettings(Settings.builder().put(FsRepository.LOCATION_SETTING.getKey(), location))
                    .execute(mustSucceed(acknowledgedResponse -> {
                        assertTrue(acknowledgedResponse.isAcknowledged());
                        logger.info("--> finished put repo [{}]", repositoryName);
                        Releasables.close(releasable);
                        scheduleRemoveAndAdd();
                    }));
            }

            private void scheduleRemoveAndAdd() {
                threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueMillis(between(1, 500)), CLIENT, mustSucceed(() -> {

                    boolean replacingRepo = false;
                    try (TransferableReleasables localReleasables = new TransferableReleasables()) {

                        if (usually()) {
                            return;
                        }

                        if (localReleasables.add(tryAcquireAllPermits(permits)) == null) {
                            return;
                        }

                        if (localReleasables.add(blockNodeRestarts()) == null) {
                            // TODO maybe attempt to remove + add repo even if nodes are restarting?
                            // but what if client node is shut down after sending request to master, we'd get a failure but the delete
                            // might still go through later, perhaps after we tried to re-add it
                            return;
                        }

                        final Releasable releaseAll = localReleasables.transfer();

                        logger.info("--> delete repo [{}]", repositoryName);
                        client().admin().cluster().prepareDeleteRepository(repositoryName).execute(mustSucceed(acknowledgedResponse -> {
                            assertTrue(acknowledgedResponse.isAcknowledged());
                            logger.info("--> finished delete repo [{}]", repositoryName);
                            putRepositoryAndContinue(releaseAll);
                        }));

                        replacingRepo = true;
                    } finally {
                        if (replacingRepo == false) {
                            scheduleRemoveAndAdd();
                        }
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
                client().admin()
                    .indices()
                    .prepareCreate(indexName)
                    .setSettings(
                        Settings.builder()
                            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), between(1, 5))
                            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), between(0, cluster.numDataNodes() - 1))
                    )
                    .execute(mustSucceed(response -> {
                        assertTrue(response.isAcknowledged());
                        logger.info("--> finished create index [{}]", indexName);
                        Releasables.close(releasable);
                        scheduleIndexingAndPossibleDelete();
                    }));
            }

            private void scheduleIndexingAndPossibleDelete() {
                threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueMillis(between(1, 500)), CLIENT, mustSucceed(() -> {

                    boolean forked = false;
                    try (TransferableReleasables localReleasables = new TransferableReleasables()) {

                        if (localReleasables.add(blockNodeRestarts()) == null) {
                            return;
                        }

                        if (usually()) {
                            // index some more docs

                            assertNotNull(localReleasables.add(tryAcquirePermit(permits))); // nobody else should be blocking this index

                            final Releasable releaseAll = localReleasables.transfer();

                            final StepListener<ClusterHealthResponse> ensureYellowStep = new StepListener<>();

                            logger.info("--> waiting for yellow health of [{}]", indexName);

                            client().admin()
                                .cluster()
                                .prepareHealth(indexName)
                                .setWaitForEvents(Priority.LANGUID)
                                .setWaitForYellowStatus()
                                .setWaitForNodes(Integer.toString(internalCluster().getNodeNames().length))
                                .execute(ensureYellowStep);

                            final StepListener<BulkResponse> bulkStep = new StepListener<>();

                            ensureYellowStep.addListener(mustSucceed(clusterHealthResponse -> {

                                assertFalse(
                                    "timed out waiting for yellow state of [" + indexName + "]",
                                    clusterHealthResponse.isTimedOut()
                                );

                                final int docCount = between(1, 1000);
                                final BulkRequestBuilder bulkRequestBuilder = client().prepareBulk(indexName);

                                logger.info("--> indexing [{}] docs into [{}]", docCount, indexName);

                                for (int i = 0; i < docCount; i++) {
                                    bulkRequestBuilder.add(
                                        new IndexRequest().source(
                                            jsonBuilder().startObject().field("field-" + between(1, 5), randomAlphaOfLength(10)).endObject()
                                        )
                                    );
                                }

                                bulkRequestBuilder.execute(bulkStep);
                            }));

                            bulkStep.addListener(mustSucceed(bulkItemResponses -> {
                                for (BulkItemResponse bulkItemResponse : bulkItemResponses.getItems()) {
                                    assertNull(bulkItemResponse.getFailure());
                                }

                                logger.info("--> indexing into [{}] finished", indexName);

                                Releasables.close(releaseAll);
                                scheduleIndexingAndPossibleDelete();

                            }));

                            forked = true;

                        } else if (localReleasables.add(tryAcquireAllPermits(permits)) != null) {
                            // delete the index and create a new one

                            final Releasable releaseAll = localReleasables.transfer();

                            logger.info("--> deleting index [{}]", indexName);

                            client().admin().indices().prepareDelete(indexName).execute(mustSucceed(acknowledgedResponse -> {
                                logger.info("--> deleting index [{}] finished", indexName);
                                assertTrue(acknowledgedResponse.isAcknowledged());
                                createIndexAndContinue(releaseAll);
                            }));

                            forked = true;
                        }
                    } finally {
                        if (forked == false) {
                            scheduleIndexingAndPossibleDelete();
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
