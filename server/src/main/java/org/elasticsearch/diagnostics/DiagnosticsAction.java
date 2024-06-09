/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.diagnostics;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequest;
import org.elasticsearch.action.admin.cluster.stats.TransportClusterStatsAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.transport.TransportService;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;

public class DiagnosticsAction {

    private DiagnosticsAction() {/* no instances */}

    public static final ActionType<ActionResponse.Empty> INSTANCE = new ActionType<>("cluster:monitor/diagnostics");

    public static final class Request extends ActionRequest {
        private final RestChannel restChannel;

        public Request(RestChannel restChannel) {
            this.restChannel = restChannel;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }
    }

    public static final class TransportDiagnosticsAction extends TransportAction<Request, ActionResponse.Empty> {

        private static final DateTimeFormatter FILENAME_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss", Locale.ROOT);

        private final Client client;
        private final ClusterService clusterService;

        @Inject
        public TransportDiagnosticsAction(
            Client client,
            ClusterService clusterService,
            TransportService transportService,
            ActionFilters actionFilters
        ) {
            super(INSTANCE.name(), actionFilters, transportService.getTaskManager());
            this.clusterService = clusterService;
            this.client = client;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<ActionResponse.Empty> listener) {
            assert task instanceof CancellableTask;
            doExecute(
                new ParentTaskAssigningClient(client, clusterService.localNode(), task),
                new ChunkedZipResponse(
                    "elasticsearch-internal-diagnostics-" + ZonedDateTime.now(ZoneOffset.UTC).format(FILENAME_DATE_TIME_FORMATTER),
                    request.restChannel
                ),
                listener.map(v -> ActionResponse.Empty.INSTANCE)
            );
        }

        private void doExecute(Client client, ChunkedZipResponse response, ActionListener<Void> listener) {
            SubscribableListener
                // fan out
                .<Void>newForked(l -> {
                    try (var listeners = new RefCountingListener(l)) {
                        client.execute(
                            TransportNodesInfoAction.TYPE,
                            new NodesInfoRequest(),
                            response.newXContentListener("nodes.json", listeners.acquire())
                        );

                        client.execute(
                            TransportNodesStatsAction.TYPE,
                            new NodesStatsRequest(),
                            response.newChunkedXContentListener("nodes_stats.json", listeners.acquire())
                        );

                        client.execute(
                            TransportClusterStatsAction.TYPE,
                            new ClusterStatsRequest(),
                            response.newXContentListener("cluster_stats.json", listeners.acquire())
                        );

                        response.newChunkedXContentListener("cluster_state.json", listeners.acquire()).onResponse(clusterService.state());

                        response.newChunkedXContentListener("example_exception.json", listeners.acquire())
                            .onFailure(new ElasticsearchException("test"));
                    }
                })
                // finish
                .<Void>andThen((l, v) -> response.finish(l))
                .addListener(listener);
        }
    }

}
