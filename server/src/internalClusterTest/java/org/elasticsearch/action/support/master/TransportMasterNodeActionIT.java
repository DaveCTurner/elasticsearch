/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.master;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.coordination.LeaderChecker;
import org.elasticsearch.cluster.coordination.MasterElectionTestCase;
import org.elasticsearch.cluster.coordination.PublicationTransportHandler;
import org.elasticsearch.cluster.coordination.StatefulPreVoteCollector;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class TransportMasterNodeActionIT extends MasterElectionTestCase {

    private static final Logger logger = LogManager.getLogger(TransportMasterNodeActionIT.class);

    @SuppressWarnings("unchecked")
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(
            super.nodePlugins(),
            MockTransportService.TestPlugin.class,
            TestActionPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // detect leader failover quickly
            .put(LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), 1)
            .put(LeaderChecker.LEADER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .build();
    }

    public void testRoutingLoopProtection() {

        final var cleanupTasks = new ArrayList<Releasable>();

        try {
            final var newMaster = ensureSufficientMasterEligibleNodes();
            final long originalTerm = internalCluster().masterClient()
                .admin()
                .cluster()
                .prepareState(TEST_REQUEST_TIMEOUT)
                .get()
                .getState()
                .term();
            final var previousMasterKnowsNewMasterIsElectedLatch = configureElectionLatchForNewMaster(newMaster, cleanupTasks);

            final var newMasterReceivedReroutedMessageFuture = new PlainActionFuture<>();
            final var newMasterReceivedReroutedMessageListener = ActionListener.assertOnce(newMasterReceivedReroutedMessageFuture);
            final var reroutedMessageReceived = ActionListener.assertOnce(ActionListener.noop());
            for (final var transportService : internalCluster().getInstances(TransportService.class)) {
                final var mockTransportService = asInstanceOf(MockTransportService.class, transportService);
                cleanupTasks.add(mockTransportService::clearAllRules);

                if (mockTransportService.getLocalNode().getName().equals(newMaster)) {
                    // Complete listener when the new master receives the re-routed message, ensure it only receives it once, and only from
                    // a node in the newMaster term.
                    mockTransportService.addRequestHandlingBehavior(TEST_ACTION_TYPE.name(), (handler, request, channel, task) -> {
                        assertThat(asInstanceOf(MasterNodeRequest.class, request).masterTerm(), greaterThan(originalTerm));
                        newMasterReceivedReroutedMessageListener.onResponse(null);
                        handler.messageReceived(request, channel, task);
                    });
                } else {
                    // Disable every other node's ability to send pre-vote and publish requests
                    mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                        if (action.equals(StatefulPreVoteCollector.REQUEST_PRE_VOTE_ACTION_NAME)
                            || action.equals(PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME)) {
                            throw new ElasticsearchException("[{}] for [{}] denied", action, connection.getNode());
                        } else {
                            connection.sendRequest(requestId, action, request, options);
                        }
                    });

                    // Assert that no other node receives the re-routed message more than once, and only from a node in the original term.
                    mockTransportService.addRequestHandlingBehavior(TEST_ACTION_TYPE.name(), (handler, request, channel, task) -> {
                        assertThat(asInstanceOf(MasterNodeRequest.class, request).masterTerm(), equalTo(originalTerm));
                        reroutedMessageReceived.onResponse(null);
                        handler.messageReceived(request, channel, task);
                    });
                }
            }

            final var newMasterStateApplierBlock = blockClusterStateApplier(newMaster, cleanupTasks);

            // trigger a cluster state update, which fails, causing a master failover
            internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
                .submitUnbatchedStateUpdateTask("failover", new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return ClusterState.builder(currentState).build();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // expected
                    }
                });

            // Wait until the old master has acknowledged the new master's election
            safeAwait(previousMasterKnowsNewMasterIsElectedLatch);
            logger.info("New master is elected");

            // perform a TransportMasterNodeAction on the new master, which doesn't know it's the master yet
            final var testActionFuture = client(newMaster).execute(TEST_ACTION_TYPE, new TestRequest());

            // wait for the request to come back to the new master
            safeGet(newMasterReceivedReroutedMessageFuture);

            // Unblock state application on new master, allow it to know of its election win
            safeAwait(newMasterStateApplierBlock);

            safeGet(testActionFuture);
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(cleanupTasks));
        }
    }

    private static final ActionType<ActionResponse.Empty> TEST_ACTION_TYPE = new ActionType<>("internal:test");

    public static final class TestActionPlugin extends Plugin implements ActionPlugin {
        @Override
        public Collection<ActionHandler> getActions() {
            return List.of(new ActionHandler(TEST_ACTION_TYPE, TestTransportAction.class));
        }
    }

    public static final class TestRequest extends MasterNodeRequest<TestRequest> {
        TestRequest() {
            super(TEST_REQUEST_TIMEOUT);
        }

        TestRequest(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static final class TestTransportAction extends TransportMasterNodeAction<TestRequest, ActionResponse.Empty> {
        @Inject
        public TestTransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver
        ) {
            super(
                TEST_ACTION_TYPE.name(),
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                TestRequest::new,
                in -> ActionResponse.Empty.INSTANCE,
                threadPool.generic()
            );
        }

        @Override
        protected void masterOperation(Task task, TestRequest request, ClusterState state, ActionListener<ActionResponse.Empty> listener) {
            listener.onResponse(ActionResponse.Empty.INSTANCE);
        }

        @Override
        protected ClusterBlockException checkBlock(TestRequest request, ClusterState state) {
            return null;
        }
    }
}
