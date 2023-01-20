/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class LeaveClusterAction extends ActionType<ActionResponse.Empty> {

    private static final Logger logger = LogManager.getLogger(LeaveClusterAction.class);

    public static final LeaveClusterAction INSTANCE = new LeaveClusterAction();
    public static final String NAME = "internal:cluster/coordination/leave_cluster";

    private LeaveClusterAction() {
        super(NAME, in -> ActionResponse.Empty.INSTANCE);
    }

    public static class Request extends MasterNodeRequest<Request> {

        private final DiscoveryNode discoveryNode;

        public Request(DiscoveryNode discoveryNode) {
            this.discoveryNode = discoveryNode;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.discoveryNode = new DiscoveryNode(in);
        }

        public DiscoveryNode discoveryNode() {
            return discoveryNode;
        }

        @Override
        public String toString() {
            return "LeaveClusterAction.Request[discoveryNode=" + discoveryNode + ']';
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            discoveryNode.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class TransportAction extends TransportMasterNodeAction<LeaveClusterAction.Request, ActionResponse.Empty> {

        private final Coordinator coordinator;

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Coordinator coordinator
        ) {
            super(
                NAME,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                Request::new,
                indexNameExpressionResolver,
                in -> ActionResponse.Empty.INSTANCE,
                ThreadPool.Names.SAME
            );
            this.coordinator = coordinator;
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return null;
        }

        @Override
        protected void executeWithState(
            Task task,
            Request request,
            ClusterState clusterState,
            ActionListener<ActionResponse.Empty> listener
        ) {
            if (request.discoveryNode().equals(clusterState.nodes().getMasterNode())) {
                new ClusterStateObserver(clusterState, clusterService, request.masterNodeTimeout(), logger, threadPool.getThreadContext())
                    .waitForNextChange(new ClusterStateObserver.Listener() {
                        @Override
                        public void onNewClusterState(ClusterState newState) {
                            TransportAction.super.executeWithState(task, request, newState, listener);
                        }

                        @Override
                        public void onClusterServiceClose() {
                            listener.onFailure(new NodeClosedException(clusterService.localNode()));
                        }

                        @Override
                        public void onTimeout(TimeValue timeout) {
                            listener.onFailure(
                                new ElasticsearchTimeoutException(
                                    Strings.format(
                                        "timed out after [%s] waiting for election of node other than [%s]",
                                        request.masterNodeTimeout(),
                                        request.discoveryNode()
                                    )
                                )
                            );
                        }
                    }, newState -> {
                        logger.info("--> state [{}] has master [{}]", newState.version(), newState.nodes().getMasterNode());
                        return request.discoveryNode().equals(newState.nodes().getMasterNode()) == false;
                    });

            } else {
                super.executeWithState(task, request, clusterState, listener);
            }
        }

        @Override
        protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<ActionResponse.Empty> listener)
            throws Exception {
            if (state.nodes().nodeExists(request.discoveryNode())) {
                coordinator.removeNode(request.discoveryNode(), "shutting down", listener.map(ignored -> ActionResponse.Empty.INSTANCE));
            } else {
                listener.onResponse(ActionResponse.Empty.INSTANCE);
            }
        }
    }
}
