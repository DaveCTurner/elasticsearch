/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * An action for retrieving just the nodes (and transport versions) from the cluster state according to the local cluster service, for use
 * by REST actions that need this information.
 */
public class InternalClusterStateNodesAction {

    public static final ActionType<ClusterStateResponse> INSTANCE = new ActionType<>(
        "internal:cluster/state/nodes",
        ClusterStateResponse::new
    );

    /**
     * Retrieve a cluster state that contains only the nodes (and transport versions) according to the local cluster service.
     */
    public static void getClusterStateNodes(Client client, ActionListener<ClusterState> listener) {
        final var threadContext = client.threadPool().getThreadContext();
        try (var ignored = threadContext.stashContext()) {
            threadContext.markAsSystemContext();
            client.execute(INSTANCE, new InternalClusterStateNodesAction.Request(), listener.map(ClusterStateResponse::getState));
        }
    }

    public static class TransportAction extends HandledTransportAction<InternalClusterStateNodesAction.Request, ClusterStateResponse> {

        private final ClusterService clusterService;

        @Inject
        public TransportAction(TransportService transportService, ActionFilters actionFilters, ClusterService clusterService) {
            super(INSTANCE.name(), transportService, actionFilters, Request::new, ThreadPool.Names.SAME);
            this.clusterService = clusterService;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<ClusterStateResponse> listener) {
            final var clusterState = clusterService.state();

            if (clusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
                listener.onFailure(new ClusterBlockException(Set.of(GatewayService.STATE_NOT_RECOVERED_BLOCK)));
            } else {
                listener.onResponse(
                    new ClusterStateResponse(
                        clusterState.getClusterName(),
                        ClusterState.builder(clusterState.getClusterName())
                            .nodes(clusterState.nodes())
                            .transportVersions(getTransportVersions(clusterState))
                            .build(),
                        false
                    )
                );
            }
        }

        @SuppressForbidden(reason = "exposing ClusterState#transportVersions requires reading them")
        private static Map<String, TransportVersion> getTransportVersions(ClusterState clusterState) {
            return clusterState.transportVersions();
        }
    }

    public static class Request extends ActionRequest {
        Request() {
            super();
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            assert false : "never sent over the wire";
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }
}
