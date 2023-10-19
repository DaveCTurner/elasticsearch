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
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
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
        createIndex("index-0", 1, 0);
        createIndex("index-1", 1, 0);
        createIndex("index-2", 1, 0);
        createIndex("index-3", 1, 0);
        clusterAdmin().prepareCreateSnapshot("test-repo", "snapshot-initial").setWaitForCompletion(true).get();

        final var masterTransportService = asInstanceOf(
            MockTransportService.class,
            internalCluster().getCurrentMasterNodeInstance(TransportService.class)
        );

        final var readyToBlockLatch = new CountDownLatch(6);
        final var readyToUnblockLatch = new CountDownLatch(1);
        final var unblockIndex1Listener = new SubscribableListener<Void>();
        final var unblockIndex2Listener = new SubscribableListener<Void>();
        final var unblockIndex3Listener = new SubscribableListener<Void>();
        masterTransportService.addRequestHandlingBehavior(
            SnapshotsService.UPDATE_SNAPSHOT_STATUS_ACTION_NAME,
            (StubbableTransport.RequestHandlingBehavior<UpdateIndexShardSnapshotStatusRequest>) (handler, request, channel, task) -> {
                switch (request.shardId().getIndexName()) {
                    case "index-0" -> handler.messageReceived(
                        request.snapshot().getSnapshotId().getName().equals("snapshot-3")
                            ? new UpdateIndexShardSnapshotStatusRequest(
                                request.snapshot(),
                                request.shardId(),
                                new SnapshotsInProgress.ShardSnapshotStatus(
                                    request.status().nodeId(),
                                    SnapshotsInProgress.ShardState.FAILED,
                                    "simulated",
                                    request.status().generation()
                                )
                            )
                            : request,
                        new TestTransportChannel(ActionTestUtils.assertNoFailureListener(response -> {
                            readyToBlockLatch.countDown();
                            channel.sendResponse(response);
                        })),
                        task
                    );

                    case "index-1" -> {
                        unblockIndex1Listener.addListener(ActionTestUtils.assertNoFailureListener(ignored -> {
                            handler.messageReceived(request, channel, task);
                            readyToUnblockLatch.countDown();
                        }));
                        readyToBlockLatch.countDown();
                    }

                    case "index-2" -> {
                        unblockIndex2Listener.addListener(ActionTestUtils.assertNoFailureListener(ignored -> {
                            handler.messageReceived(request, channel, task);
                            unblockIndex1Listener.onResponse(null);
                        }));
                        readyToBlockLatch.countDown();
                    }

                    case "index-3" -> {
                        unblockIndex3Listener.addListener(ActionTestUtils.assertNoFailureListener(ignored -> {
                            handler.messageReceived(request, channel, task);
                        }));
                        readyToBlockLatch.countDown();
                    }
                }
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
        clusterAdmin().prepareCreateSnapshot("test-repo", "snapshot-1").setPartial(true).setIndices("index-0", "index-1").get();
        clusterAdmin().prepareCreateSnapshot("test-repo", "snapshot-2").setPartial(true).setIndices("index-0", "index-2").get();
        clusterAdmin().prepareCreateSnapshot("test-repo", "snapshot-3").setPartial(true).setIndices("index-0", "index-3").get();

        // wait until the master has marked index-0 as complete in both snapshots, and index-1 and index-2 are ready to mark complete
        safeAwait(readyToBlockLatch);

        // block the master service so the submitted tasks are processed in a batch
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class).submitUnbatchedStateUpdateTask("blocking", blockingTask);
        safeAwait(barrier);

        // enqueues the task to mark index-2 complete, then the same for index-1
        unblockIndex2Listener.onResponse(null);
        safeAwait(readyToUnblockLatch);

        // release the master service
        safeAwait(barrier);

        final var abortFuture = clusterAdmin().prepareDeleteSnapshot("test-repo", "snapshot-3").execute();

        Thread.sleep(2000);

        internalCluster().getCurrentMasterNodeInstance(ClusterService.class).submitUnbatchedStateUpdateTask("blocking", blockingTask);
        safeAwait(barrier);
        unblockIndex3Listener.onResponse(null);
        safeAwait(barrier);

        // wait for all snapshots to complete
        awaitNoMoreRunningOperations();
        assertAcked(abortFuture.get(10, TimeUnit.SECONDS));

        // ensure that another snapshot works
        clusterAdmin().prepareCreateSnapshot("test-repo", "snapshot-final").setWaitForCompletion(true).get();
    }
}
