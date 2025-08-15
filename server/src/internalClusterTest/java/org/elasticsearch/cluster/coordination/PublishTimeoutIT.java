/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesTransportRequest;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class PublishTimeoutIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    public void testStuckMasterReportsUnhealthy() throws Exception {
        final var originalMaster = internalCluster().startNode(
            Settings.builder().put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), TimeValue.timeValueMillis(1))
        );

        // Start off by delaying the sending cluster state to remote nodes until we have accepted it locally, so that we do not hit the 1ms
        // publish timeout which would make us believe we are unhealthy
        final var originalMasterCoordinator = internalCluster().getCurrentMasterNodeInstance(Coordinator.class);
        final var originalMasterTransportService = MockTransportService.getInstance(originalMaster);
        originalMasterTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME)) {
                final var clusterStateVersion = getClusterStateVersion(asInstanceOf(BytesTransportRequest.class, request));
                new Thread(() -> {
                    try {
                        assertTrue(waitUntil(() -> originalMasterCoordinator.getLastAcceptedState().version() >= clusterStateVersion));
                        connection.sendRequest(requestId, action, request, options);
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }, "delay-state-" + clusterStateVersion + "-to-" + connection.getNode().getName()).start();
            } else {
                connection.sendRequest(requestId, action, request, options);
            }
        });

        logger.info("--> start 2 new nodes");

        internalCluster().startNodes(2);

        final var originalMasterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        ClusterServiceUtils.awaitClusterState(
            logger,
            cs -> cs.coordinationMetadata().getLastCommittedConfiguration().getNodeIds().size() == 3,
            originalMasterClusterService
        );
        ClusterServiceUtils.awaitNoPendingTasks(originalMasterClusterService);

        final var blockedClusterStateVersion = originalMasterClusterService.state().version();
        logger.info("--> cluster stabilised at state version [{}]; blocking original master", blockedClusterStateVersion);

        // set up a task which blocks up the original master's CLUSTER_COORDINATION threadpool, preventing the master from accepting its
        // own publication
        final var originalMasterCoordinatorExecutorBarrier = new CyclicBarrier(2);
        final var originalMasterCoordinatorExecutor = originalMasterTransportService.getThreadPool()
            .executor(ThreadPool.Names.CLUSTER_COORDINATION);

        originalMasterCoordinatorExecutor.execute(() -> {
            safeAwait(originalMasterCoordinatorExecutorBarrier); // notify main thread we are blocked
            safeAwait(originalMasterCoordinatorExecutorBarrier); // await release from main thread
        });
        safeAwait(originalMasterCoordinatorExecutorBarrier);

        // set up a task which blocks up the original master's Coordinator#mutex (not blocked until the publication messages are sent,
        // because this needs the mutex)
        final var originalMasterCoordinatorMutexBarrier = new CyclicBarrier(2);
        final var originalMasterCoordinatorMutexBlocker = new RunOnce(() -> new Thread(() -> {
            synchronized (originalMasterCoordinator.mutex) {
                safeAwait(originalMasterCoordinatorMutexBarrier); // notify main thread the mutex is blocked
                // verify that we haven't accepted the state locally
                assertEquals(blockedClusterStateVersion, originalMasterCoordinator.getLastAcceptedState().version());
                safeAwait(originalMasterCoordinatorMutexBarrier); // wait for the main thread to let us release the mutex
            }
        }, "block-coordinator-mutex").start());

        // replace the send-behaviour with one that no longer delays the outbound publish messages, so that the state can be accepted on
        // the other two nodes (and hence committed) before it's accepted locally
        final var publishRequestsSentCounter = new AtomicLong();
        originalMasterTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME)) {
                assertTrue(MasterService.isMasterUpdateThread()); // not the coordinator thread
                assertTrue(Thread.holdsLock(originalMasterCoordinator.mutex)); // but it does hold the coordinator mutex

                originalMasterCoordinatorMutexBlocker.run();

                final var currentValue = publishRequestsSentCounter.incrementAndGet();
                logger.info("--> sending publish request [{}] on [{}]", currentValue, Thread.currentThread().getName());
            }
            connection.sendRequest(requestId, action, request, options);
        });

        // submit a simple cluster state update to trigger another publication, which should get caught up in the blocks
        logger.info("--> original master blocked, now submitting a state update to detect the blocking");
        final var originalMasterClusterStateUpdateLatch = new CountDownLatch(1);
        originalMasterClusterService.submitUnbatchedStateUpdateTask("no-op update", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return ClusterState.builder(currentState).build();
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("cluster state update should eventually have succeeded", e);
            }

            @Override
            public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                originalMasterClusterStateUpdateLatch.countDown();
            }
        });

        safeAwait(originalMasterCoordinatorMutexBarrier);
        // the originalMasterCoordinatorMutexBlocker is now blocked and holds Coordinator#mutex which means the publication started, sent
        // the publish messages, and released the mutex; the original master's coordinator can now do nothing more
        assertEquals(2, publishRequestsSentCounter.get());

        logger.info("--> publication now in progress");
        // the original master will receive the publish responses, see that it has a quorum of them, start a 1ms timeout, and when the
        // timeout elapses without having accepted the state locally it reports itself as unhealthy via the LeaderChecker, triggering a
        // failover:
        final var newMasterElectedLatch = new CountDownLatch(1);
        for (ClusterService clusterService : internalCluster().getInstances(ClusterService.class)) {
            ClusterServiceUtils.addTemporaryStateListener(clusterService, cs -> {
                final var masterNode = cs.nodes().getMasterNode();
                return masterNode != null && masterNode.getName().equals(originalMaster) == false;
            }).addListener(ActionTestUtils.assertNoFailureListener(ignored -> newMasterElectedLatch.countDown()));
        }
        safeAwait(newMasterElectedLatch);

        // release the old master's coordinator so it can clean up properly
        safeAwait(originalMasterCoordinatorExecutorBarrier);
        safeAwait(originalMasterCoordinatorMutexBarrier);

        // ensure that the original cluster state completes normally (it committed, even though it was not applied on the other nodes)
        safeAwait(originalMasterClusterStateUpdateLatch);

        // ensure that the original master joins the new one
        safeAwait(ClusterServiceUtils.addTemporaryStateListener(originalMasterClusterService, cs -> {
            final var masterNode = cs.nodes().getMasterNode();
            return masterNode != null && masterNode.getName().equals(originalMaster) == false;
        }));

        logger.info("--> done");
    }

    private long getClusterStateVersion(BytesTransportRequest request) throws IOException {
        // very cut-down version of PublicationTransportHandler#handleIncomingPublishRequest
        final var compressor = Objects.requireNonNull(CompressorFactory.compressorForUnknownXContentType(request.bytes()));
        try (
            var compressedStreamInput = request.bytes().streamInput();
            var payloadStreamInput = compressor.threadLocalStreamInput(compressedStreamInput)
        ) {
            payloadStreamInput.setTransportVersion(request.version());
            if (payloadStreamInput.readBoolean()) {
                // full cluster state
                payloadStreamInput.readString(); // discard cluster name
            } else {
                // cluster state diff
                payloadStreamInput.readString(); // discard cluster name
                payloadStreamInput.readString(); // discard from-uuid
                payloadStreamInput.readString(); // discard to-uuid
            }
            return payloadStreamInput.readLong();
        }
    }
}
