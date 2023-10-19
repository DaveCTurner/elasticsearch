/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class SnapshotsServiceIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    public void testDeletingSnapshotsIsLoggedAfterClusterStateIsProcessed() throws Exception {
        createRepository("test-repo", "fs");
        createIndexWithRandomDocs("test-index", randomIntBetween(1, 42));
        createSnapshot("test-repo", "test-snapshot", List.of("test-index"));

        final MockLogAppender mockLogAppender = new MockLogAppender();

        try {
            mockLogAppender.start();
            Loggers.addAppender(LogManager.getLogger(SnapshotsService.class), mockLogAppender);

            mockLogAppender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "[does-not-exist]",
                    SnapshotsService.class.getName(),
                    Level.INFO,
                    "deleting snapshots [does-not-exist] from repository [test-repo]"
                )
            );

            mockLogAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "[deleting test-snapshot]",
                    SnapshotsService.class.getName(),
                    Level.INFO,
                    "deleting snapshots [test-snapshot] from repository [test-repo]"
                )
            );

            mockLogAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "[test-snapshot deleted]",
                    SnapshotsService.class.getName(),
                    Level.INFO,
                    "snapshots [test-snapshot/*] deleted"
                )
            );

            final SnapshotMissingException e = expectThrows(
                SnapshotMissingException.class,
                () -> startDeleteSnapshot("test-repo", "does-not-exist").actionGet()
            );
            assertThat(e.getMessage(), containsString("[test-repo:does-not-exist] is missing"));
            assertThat(startDeleteSnapshot("test-repo", "test-snapshot").actionGet().isAcknowledged(), is(true));

            awaitNoMoreRunningOperations(); // ensure background file deletion is completed
            mockLogAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(LogManager.getLogger(SnapshotsService.class), mockLogAppender);
            mockLogAppender.stop();
            deleteRepository("test-repo");
        }
    }

    public void testSnapshotDeletionFailureShouldBeLogged() throws Exception {
        createRepository("test-repo", "mock");
        createIndexWithRandomDocs("test-index", randomIntBetween(1, 42));
        createSnapshot("test-repo", "test-snapshot", List.of("test-index"));

        final MockLogAppender mockLogAppender = new MockLogAppender();

        try {
            mockLogAppender.start();
            Loggers.addAppender(LogManager.getLogger(SnapshotsService.class), mockLogAppender);

            mockLogAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "[test-snapshot]",
                    SnapshotsService.class.getName(),
                    Level.WARN,
                    "failed to complete snapshot deletion for [test-snapshot] from repository [test-repo]"
                )
            );

            if (randomBoolean()) {
                // Failure when listing root blobs
                final MockRepository mockRepository = getRepositoryOnMaster("test-repo");
                mockRepository.setRandomControlIOExceptionRate(1.0);
                final Exception e = expectThrows(Exception.class, () -> startDeleteSnapshot("test-repo", "test-snapshot").actionGet());
                assertThat(e.getCause().getMessage(), containsString("Random IOException"));
            } else {
                // Failure when finalizing on index-N file
                final ActionFuture<AcknowledgedResponse> deleteFuture;
                blockMasterFromFinalizingSnapshotOnIndexFile("test-repo");
                deleteFuture = startDeleteSnapshot("test-repo", "test-snapshot");
                waitForBlock(internalCluster().getMasterName(), "test-repo");
                unblockNode("test-repo", internalCluster().getMasterName());
                final Exception e = expectThrows(Exception.class, deleteFuture::actionGet);
                assertThat(e.getCause().getMessage(), containsString("exception after block"));
            }

            mockLogAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(LogManager.getLogger(SnapshotsService.class), mockLogAppender);
            mockLogAppender.stop();
            deleteRepository("test-repo");
        }
    }

    public void testConcurrentOutOfOrderFinalization() throws Exception {
        createRepository("test-repo", "mock");
        createIndex("index-00", 1, 0);
        createIndex("index-11", 1, 0);
        createIndex("index-12", 1, 0);
        createIndex("index-13", 1, 0);
        createIndex("index-14", 1, 0);
        createIndex("index-15", 1, 0);
        clusterAdmin().prepareCreateSnapshot("test-repo", "snapshot-initial").setWaitForCompletion(true).get();

        final var masterTransportService = asInstanceOf(
            MockTransportService.class,
            internalCluster().getCurrentMasterNodeInstance(TransportService.class)
        );

        final var actions = new ConcurrentHashMap<String, CheckedRunnable<Exception>>();

        masterTransportService.addRequestHandlingBehavior(
            SnapshotsService.UPDATE_SNAPSHOT_STATUS_ACTION_NAME,
            (StubbableTransport.RequestHandlingBehavior<UpdateIndexShardSnapshotStatusRequest>) (handler, request, channel, task) -> {
                assertNull(
                    actions.putIfAbsent(
                        request.snapshot().getSnapshotId().getName() + "/" + request.shardId().getIndexName(),
                        () -> handler.messageReceived(request, channel, task)
                    )
                );
            }
        );

        final var barrier = new CyclicBarrier(2);
        final var blockingTask = new ClusterStateUpdateTask(Priority.LANGUID) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                safeAwait(barrier);
                safeAwait(barrier);
                return currentState;
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }
        };

        // start the snapshots running
        clusterAdmin().prepareCreateSnapshot("test-repo", "snapshot-11").setPartial(true).setIndices("index-00", "index-11").get();
        clusterAdmin().prepareCreateSnapshot("test-repo", "snapshot-12").setPartial(true).setIndices("index-00", "index-12").get();
        clusterAdmin().prepareCreateSnapshot("test-repo", "snapshot-13").setPartial(true).setIndices("index-00", "index-13").get();

        CheckedConsumer<String, Exception> x = name -> {
            Thread.sleep(500);
            logger.info("--> execute {}", name);
            Objects.requireNonNull(actions.remove(name), name).run();
        };

        x.accept("snapshot-11/index-00");
        x.accept("snapshot-12/index-00");

        final var abort13Future = clusterAdmin().prepareDeleteSnapshot("test-repo", "snapshot-13").execute();
        Thread.sleep(500);

        x.accept("snapshot-13/index-00");
        x.accept("snapshot-13/index-13");

        Thread.sleep(500);
        clusterAdmin().prepareCreateSnapshot("test-repo", "snapshot-14").setPartial(true).setIndices("index-00", "index-14").get();
        clusterAdmin().prepareCreateSnapshot("test-repo", "snapshot-15").setPartial(true).setIndices("index-00", "index-15").get();

        x.accept("snapshot-14/index-00");

        final var abort15Future = clusterAdmin().prepareDeleteSnapshot("test-repo", "snapshot-15").execute();

        x.accept("snapshot-15/index-00");

        x.accept("snapshot-11/index-11");

        internalCluster().getCurrentMasterNodeInstance(ClusterService.class).submitUnbatchedStateUpdateTask("blocking", blockingTask);
        safeAwait(barrier);

        x.accept("snapshot-15/index-15");
        x.accept("snapshot-12/index-12");

        Thread.sleep(500);
        safeAwait(barrier);

        x.accept("snapshot-14/index-14");

        // wait for all snapshots to complete
        awaitNoMoreRunningOperations();
        assertAcked(abort13Future.get(10, TimeUnit.SECONDS));
        assertAcked(abort15Future.get(10, TimeUnit.SECONDS));

        // ensure that another snapshot works
        masterTransportService.clearAllRules();
        clusterAdmin().prepareCreateSnapshot("test-repo", "snapshot-final").setWaitForCompletion(true).get();
    }
}
