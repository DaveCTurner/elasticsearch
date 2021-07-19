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
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequestBuilder;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;

@LuceneTestCase.SuppressFileSystems(value = "HandleLimitFS") // we sometimes have >2048 open files
public class SnapshotStressTestsIT extends AbstractSnapshotIntegTestCase {

    public void testRandomActivities() throws InterruptedException {
        final DiscoveryNodes discoveryNodes = client().admin().cluster().prepareState().clear().setNodes(true).get().getState().nodes();
        new TrackedCluster(internalCluster(), nodeNames(discoveryNodes.getMasterNodes()), nodeNames(discoveryNodes.getDataNodes())).run();
        disableRepoConsistencyCheck("have not necessarily written to all repositories");
    }

    private static Set<String> nodeNames(ImmutableOpenMap<String, DiscoveryNode> nodesMap) {
        return StreamSupport.stream(nodesMap.values().spliterator(), false).map(c -> c.value.getName()).collect(Collectors.toSet());
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

    /**
     * Test harness for snapshot stress tests.
     *
     * The test performs random operations on the cluster, as if from an external client:
     *
     * - indexing docs, deleting and re-creating the indices
     * - restarting nodes
     * - removing and adding repositores
     * - taking snapshots (sometimes partial), cloning them, and deleting them
     *
     * It ensures that these operations should succeed via a system of shared/exclusive locks implemented via permits: acquiring a single
     * permit is a shared lock, whereas acquiring all the permits is an exclusive lock. So for instance taking a snapshot acquires a shared
     * lock on the repository (permitting other concurrent snapshots/clones/deletes) whereas deleting and recreating the repository requires
     * an exclusive lock (ensuring that there are no ongoing operations on the repository, and preventing any new such operations from
     * starting).
     *
     * None of the operations block. If the necessary locks aren't all available then the operation just releases the ones it has acquired
     * and tries again later.
     *
     * The test completes after completing a certain number of snapshots (see {@link #completedSnapshotLatch}) or after a certain time has
     * elapsed.
     */
    private static class TrackedCluster {

        static final Logger logger = LogManager.getLogger(TrackedCluster.class);
        static final String CLIENT = "client";

        private final ThreadPool threadPool = new TestThreadPool(
            "TrackedCluster",
            // a single thread for "client" activities, to limit the number of activities all starting at once
            new ScalingExecutorBuilder(CLIENT, 1, 1, TimeValue.ZERO, CLIENT)
        );

        private final AtomicBoolean shouldStop = new AtomicBoolean();
        private final boolean blockAllNodes;
        private final InternalTestCluster cluster;
        private final Map<String, TrackedNode> nodes = ConcurrentCollections.newConcurrentMap();
        private final Map<String, TrackedRepository> repositories = ConcurrentCollections.newConcurrentMap();
        private final Map<String, TrackedIndex> indices = ConcurrentCollections.newConcurrentMap();
        private final Map<String, TrackedSnapshot> snapshots = ConcurrentCollections.newConcurrentMap();

        private final AtomicInteger snapshotCounter = new AtomicInteger();
        private final CountDownLatch completedSnapshotLatch = new CountDownLatch(30);

        TrackedCluster(InternalTestCluster cluster, Set<String> masterNodeNames, Set<String> dataNodeNames) {
            this.blockAllNodes = randomBoolean();
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

            final int partialSnapshotterCount = between(1, 5);
            for (int i = 0; i < partialSnapshotterCount; i++) {
                startPartialSnapshotter();
            }

            final int clonerCount = between(0, 5);
            for (int i = 0; i < clonerCount; i++) {
                startCloner();
            }

            final int deleterCount = between(0, 3);
            for (int i = 0; i < deleterCount; i++) {
                startSnapshotDeleter();
            }

            final int restorerCount = between(0, 3);
            for (int i = 0; i < restorerCount; i++) {
                startRestorer();
            }

            if (completedSnapshotLatch.await(30, TimeUnit.SECONDS)) {
                logger.info("--> completed target snapshot count, finishing test");
            } else {
                logger.info("--> did not complete target snapshot count in 30s, giving up");
            }

            assertTrue(shouldStop.compareAndSet(false, true));

            final List<Tuple<String, Semaphore>> labelledPermits = new ArrayList<>();
            nodes.values().forEach(n -> labelledPermits.add(Tuple.tuple(n.nodeName, n.permits)));
            repositories.values().forEach(n -> labelledPermits.add(Tuple.tuple(n.repositoryName, n.permits)));
            indices.values().forEach(n -> labelledPermits.add(Tuple.tuple(n.indexName, n.permits)));
            snapshots.values().forEach(n -> labelledPermits.add(Tuple.tuple(n.snapshotName, n.permits)));

            for (Tuple<String, Semaphore> labelledPermit : labelledPermits) {
                assertTrue(labelledPermit.v1(), labelledPermit.v2().tryAcquire(Integer.MAX_VALUE, 10, TimeUnit.SECONDS));
            }

            logger.info("--> acquired all permits");

            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }

        private void enqueueAction(final CheckedRunnable<Exception> action) {
            if (shouldStop.get()) {
                return;
            }

            threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueMillis(between(1, 500)), CLIENT, mustSucceed(action));
        }

        private void startRestorer() {
            enqueueAction(() -> {
                boolean startedRestore = false;
                try (TransferableReleasables localReleasables = new TransferableReleasables()) {

                    final List<TrackedSnapshot> trackedSnapshots = new ArrayList<>(snapshots.values());
                    if (trackedSnapshots.isEmpty()) {
                        return;
                    }

                    if (localReleasables.add(blockNodeRestarts()) == null) {
                        return;
                    }

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

                    logger.info(
                        "--> listing indices in [{}:{}] in preparation for restoring",
                        trackedSnapshot.trackedRepository.repositoryName,
                        trackedSnapshot.snapshotName
                    );

                    client().admin()
                        .cluster()
                        .prepareGetSnapshots(trackedSnapshot.trackedRepository.repositoryName)
                        .setSnapshots(trackedSnapshot.snapshotName)
                        .execute(mustSucceed(getSnapshotsResponse -> restoreSnapshot(getSnapshotsResponse, releaseAll)));

                    startedRestore = true;
                } finally {
                    if (startedRestore == false) {
                        startRestorer();
                    }
                }
            });
        }

        private void restoreSnapshot(GetSnapshotsResponse getSnapshotsResponse, Releasable releasePreviousStep) {
            boolean startedRestore = false;
            try (TransferableReleasables localReleasables = new TransferableReleasables()) {
                localReleasables.add(releasePreviousStep);

                if (shouldStop.get()) {
                    return;
                }

                assertThat(getSnapshotsResponse.getSnapshots(), hasSize(1));
                final SnapshotInfo snapshotInfo = getSnapshotsResponse.getSnapshots().get(0);

                boolean restoreSpecificIndicesTmp = randomBoolean();
                final List<String> indicesToRestoreList = new ArrayList<>(snapshotInfo.indices().size());
                final List<String> indicesToCloseList = new ArrayList<>(snapshotInfo.indices().size());
                final List<String> indicesToDeleteList = new ArrayList<>(snapshotInfo.indices().size());
                for (String indexName : snapshotInfo.indices()) {
                    if (snapshotInfo.shardFailures()
                        .stream()
                        .anyMatch(snapshotShardFailure -> snapshotShardFailure.getShardId().getIndexName().equals(indexName))) {

                        restoreSpecificIndicesTmp = true;
                        continue;
                    }
                    if (randomBoolean() && localReleasables.add(tryAcquireAllPermits(indices.get(indexName).permits)) != null) {

                        indicesToRestoreList.add(indexName);

                        final int snapshotShardCount = snapshotInfo.indexSnapshotDetails().get(indexName).getShardCount();
                        final int indexShardCount = indices.get(indexName).shardCount;
                        if (snapshotShardCount == indexShardCount && randomBoolean()) {
                            indicesToCloseList.add(indexName);
                        } else {
                            indicesToDeleteList.add(indexName);
                        }
                    } else {
                        restoreSpecificIndicesTmp = true;
                    }
                }
                final boolean restoreSpecificIndices = restoreSpecificIndicesTmp;

                if (indicesToRestoreList.isEmpty()) {
                    logger.info(
                        "--> could not obtain exclusive lock on any indices in [{}:{}] for restore",
                        snapshotInfo.repository(),
                        snapshotInfo.snapshotId().getName()
                    );

                    return;
                }

                final Releasable releaseAll = localReleasables.transfer();

                final String[] indicesToRestore = indicesToRestoreList.toArray(new String[0]);
                final String[] indicesToClose = indicesToCloseList.toArray(new String[0]);
                final String[] indicesToDelete = indicesToDeleteList.toArray(new String[0]);

                final StepListener<Void> closeIndicesStep = new StepListener<>();
                final StepListener<Void> deleteIndicesStep = new StepListener<>();

                if (indicesToClose.length > 0) {
                    logger.info(
                        "--> closing indices {} in preparation for restoring from [{}:{}]",
                        indicesToRestoreList,
                        snapshotInfo.repository(),
                        snapshotInfo.snapshotId().getName()
                    );
                    client().admin().indices().prepareClose(indicesToClose).execute(mustSucceed(closeIndexResponse -> {
                        logger.info(
                            "--> finished closing indices {} in preparation for restoring from [{}:{}]",
                            indicesToRestoreList,
                            snapshotInfo.repository(),
                            snapshotInfo.snapshotId().getName()
                        );
                        assertTrue(closeIndexResponse.isAcknowledged());
                        assertTrue(closeIndexResponse.isShardsAcknowledged());
                        closeIndicesStep.onResponse(null);
                    }));
                } else {
                    closeIndicesStep.onResponse(null);
                }

                if (indicesToDelete.length > 0) {
                    logger.info(
                        "--> deleting indices {} in preparation for restoring from [{}:{}]",
                        indicesToRestoreList,
                        snapshotInfo.repository(),
                        snapshotInfo.snapshotId().getName()
                    );
                    client().admin().indices().prepareDelete(indicesToDelete).execute(mustSucceed(deleteIndicesResponse -> {
                        logger.info(
                            "--> finished deleting indices {} in preparation for restoring from [{}:{}]",
                            indicesToRestoreList,
                            snapshotInfo.repository(),
                            snapshotInfo.snapshotId().getName()
                        );
                        assertTrue(deleteIndicesResponse.isAcknowledged());
                        deleteIndicesStep.onResponse(null);
                    }));
                } else {
                    deleteIndicesStep.onResponse(null);
                }

                closeIndicesStep.addListener(mustSucceed(ignored1 -> deleteIndicesStep.addListener(mustSucceed(ignored2 -> {

                    final RestoreSnapshotRequestBuilder restoreSnapshotRequestBuilder = client().admin()
                        .cluster()
                        .prepareRestoreSnapshot(snapshotInfo.repository(), snapshotInfo.snapshotId().getName());

                    if (restoreSpecificIndices) {
                        restoreSnapshotRequestBuilder.setIndices(indicesToRestore);
                    }

                    logger.info(
                        "--> restoring indices {}{} from [{}:{}]",
                        restoreSpecificIndices ? "" : "*=",
                        indicesToRestoreList,
                        snapshotInfo.repository(),
                        snapshotInfo.snapshotId().getName()
                    );

                    restoreSnapshotRequestBuilder.execute(mustSucceed(restoreSnapshotResponse -> {
                        logger.info(
                            "--> triggered restore of indices {} from [{}:{}], waiting for green health",
                            indicesToRestoreList,
                            snapshotInfo.repository(),
                            snapshotInfo.snapshotId().getName()
                        );

                        client().admin()
                            .cluster()
                            .prepareHealth(indicesToRestore)
                            .setWaitForEvents(Priority.LANGUID)
                            .setWaitForGreenStatus()
                            .setWaitForNoInitializingShards(true)
                            .setWaitForNodes(Integer.toString(internalCluster().getNodeNames().length))
                            .execute(mustSucceed(clusterHealthResponse -> {

                                logger.info(
                                    "--> indices {} successfully restored from [{}:{}]",
                                    indicesToRestoreList,
                                    snapshotInfo.repository(),
                                    snapshotInfo.snapshotId().getName()
                                );

                                Releasables.close(releaseAll);
                                assertFalse(clusterHealthResponse.isTimedOut());
                                startRestorer();
                            }));
                    }));
                }))));

                startedRestore = true;
            } finally {
                if (startedRestore == false) {
                    startRestorer();
                }
            }
        }

        private void startCloner() {
            enqueueAction(() -> {
                boolean startedClone = false;
                try (TransferableReleasables localReleasables = new TransferableReleasables()) {

                    final List<TrackedSnapshot> trackedSnapshots = new ArrayList<>(snapshots.values());
                    if (trackedSnapshots.isEmpty()) {
                        return;
                    }

                    if (localReleasables.add(blockFullClusterRestart()) == null) {
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
            });
        }

        private void startSnapshotDeleter() {
            enqueueAction(() -> {

                boolean startedDeletion = false;
                try (TransferableReleasables localReleasables = new TransferableReleasables()) {

                    final List<TrackedSnapshot> trackedSnapshots = new ArrayList<>(snapshots.values());
                    if (trackedSnapshots.isEmpty()) {
                        return;
                    }

                    if (localReleasables.add(blockFullClusterRestart()) == null) {
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
            });
        }

        private void startSnapshotter() {
            enqueueAction(() -> {

                boolean startedSnapshot = false;
                try (TransferableReleasables localReleasables = new TransferableReleasables()) {

                    // separate TransferableReleasables for blocking node restarts & index deletion so we can release these blocks and
                    // permit data node restarts and index deletions as soon as the snapshot starts
                    final TransferableReleasables releasableAfterStart = new TransferableReleasables();
                    localReleasables.add(releasableAfterStart);

                    if (releasableAfterStart.add(blockNodeRestarts()) == null) {
                        return;
                    }
                    assertNotNull(localReleasables.add(blockFullClusterRestart()));
                    final Client client = localReleasables.add(acquireClient()).getClient();

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

                    final String snapshotName = "snapshot-partial-" + snapshotCounter.incrementAndGet();

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
                                Releasables.close(releasableAfterStart.transfer());
                                pollForSnapshotCompletion(client, trackedRepository.repositoryName, snapshotName, releaseAll, () -> {
                                    snapshots.put(snapshotName, new TrackedSnapshot(trackedRepository, snapshotName));
                                    completedSnapshotLatch.countDown();
                                    startSnapshotter();
                                });
                            }));
                        }

                        // TODO sometimes concurrently abort the snapshot

                    }));

                    startedSnapshot = true;
                } finally {
                    if (startedSnapshot == false) {
                        startSnapshotter();
                    }
                }
            });
        }

        private void startPartialSnapshotter() {
            enqueueAction(() -> {

                boolean startedSnapshot = false;
                try (TransferableReleasables localReleasables = new TransferableReleasables()) {

                    // separate TransferableReleasables for blocking node restarts & index deletion so we can release these blocks and
                    // permit data node restarts and index deletions as soon as the snapshot starts
                    final TransferableReleasables releasableAfterStart = new TransferableReleasables();
                    localReleasables.add(releasableAfterStart);

                    if (releasableAfterStart.add(blockNodeRestarts()) == null) {
                        return;
                    }
                    assertNotNull(localReleasables.add(blockFullClusterRestart()));
                    final Client client = localReleasables.add(acquireClient()).getClient();

                    final TrackedRepository trackedRepository = randomFrom(repositories.values());
                    if (localReleasables.add(tryAcquirePermit(trackedRepository.permits)) == null) {
                        return;
                    }

                    boolean snapshotSpecificIndicesTmp = randomBoolean();
                    final List<String> targetIndexNames = new ArrayList<>(indices.size());
                    for (TrackedIndex trackedIndex : indices.values()) {
                        if (usually() && releasableAfterStart.add(tryAcquirePermit(trackedIndex.permits)) != null) {
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

                    final String snapshotName = "snapshot-" + snapshotCounter.incrementAndGet();

                    logger.info(
                        "--> take partial snapshot [{}:{}] with indices [{}{}]",
                        trackedRepository.repositoryName,
                        snapshotName,
                        snapshotSpecificIndices ? "" : "*=",
                        targetIndexNames
                    );

                    final CreateSnapshotRequestBuilder createSnapshotRequestBuilder = client().admin()
                        .cluster()
                        .prepareCreateSnapshot(trackedRepository.repositoryName, snapshotName)
                        .setPartial(true);

                    if (snapshotSpecificIndices) {
                        createSnapshotRequestBuilder.setIndices(targetIndexNames.toArray(new String[0]));
                    }

                    createSnapshotRequestBuilder.execute(mustSucceed(createSnapshotResponse -> {
                        logger.info("--> started partial snapshot [{}:{}]", trackedRepository.repositoryName, snapshotName);
                        Releasables.close(releasableAfterStart.transfer());
                        pollForSnapshotCompletion(client, trackedRepository.repositoryName, snapshotName, releaseAll, () -> {
                            snapshots.put(snapshotName, new TrackedSnapshot(trackedRepository, snapshotName));
                            completedSnapshotLatch.countDown();
                            startPartialSnapshotter();
                        });
                    }));

                    // TODO sometimes concurrently abort the snapshot

                    startedSnapshot = true;
                } finally {
                    if (startedSnapshot == false) {
                        startPartialSnapshotter();
                    }
                }
            });
        }

        private void pollForSnapshotCompletion(
            Client client,
            String repositoryName,
            String snapshotName,
            Releasable onCompletion,
            Runnable onSuccess
        ) {
            threadPool.executor(CLIENT)
                .execute(
                    mustSucceed(
                        () -> client.admin()
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
                                    pollForSnapshotCompletion(client, repositoryName, snapshotName, onCompletion, onSuccess);
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

        /**
         * Try and block the restart of a majority of the master nodes, which therefore prevents a full-cluster restart from occurring.
         */
        @Nullable // if we couldn't block enough master node restarts
        private Releasable blockFullClusterRestart() {
            if (blockAllNodes) {
                return blockNodeRestarts();
            }

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

        /**
         * Acquire a client (i.e. block the client node from restarting) in a situation where we know that such a block can be obtained,
         * since previous acquisitions mean that at least one node is already blocked from restarting.
         */
        private ReleasableClient acquireClient() {
            final ReleasableClient client = tryAcquireClient();
            assertNotNull(client);
            return client;
        }

        /**
         * Attempt to acquire a client (i.e. block the client node from restarting) but if no blockable node can be found then return {@code
         * null}.
         */
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
         * A client to a node that is blocked from restarting; close this {@link Releasable} to release the block.
         */
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
                enqueueAction(() -> {
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
                            logger.info("--> restarting [{}]", nodeName);
                            cluster.restartNode(nodeName);
                            logger.info("--> finished restarting [{}]", nodeName);
                            Releasables.close(releaseAll);
                            start();
                        }));

                        restarting = true;
                    } finally {
                        if (restarting == false) {
                            start();
                        }
                    }
                });
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
                return "TrackedNode{" + nodeName + "}{" + (isMasterNode ? "m" : "") + (isDataNode ? "d" : "") + "}";
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
                enqueueAction(() -> {

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
                });
            }

        }

        private class TrackedIndex {

            private final Semaphore permits = new Semaphore(Integer.MAX_VALUE);
            private final String indexName;
            private int shardCount; // only changed when all permits held by the delete/recreate process

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
                shardCount = between(1, 5);
                client().admin()
                    .indices()
                    .prepareCreate(indexName)
                    .setSettings(
                        Settings.builder()
                            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), shardCount)
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
                enqueueAction(() -> {

                    boolean forked = false;
                    try (TransferableReleasables localReleasables = new TransferableReleasables()) {

                        if (localReleasables.add(blockNodeRestarts()) == null) {
                            return;
                        }

                        if (usually()) {
                            // index some more docs

                            if (localReleasables.add(tryAcquirePermit(permits)) == null) {
                                return;
                            }

                            final Releasable releaseAll = localReleasables.transfer();

                            final StepListener<ClusterHealthResponse> ensureYellowStep = new StepListener<>();

                            logger.info("--> waiting for yellow health of [{}] prior to indexing", indexName);

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
                });
            }

        }

        private static class TrackedSnapshot {

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
