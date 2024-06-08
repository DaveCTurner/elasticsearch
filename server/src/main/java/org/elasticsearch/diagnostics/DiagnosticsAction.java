/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.diagnostics;

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
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.zip.ZipOutputStream;

public class DiagnosticsAction {

    private DiagnosticsAction() {/* no instances */}

    public static final ActionType<ActionResponse.Empty> INSTANCE = new ActionType<>("cluster:monitor/diagnostics");

    public static final class Request extends ActionRequest {
        private final RestChannel restChannel;
        private final Client client;

        public Request(RestChannel restChannel, Client client) {
            this.restChannel = restChannel;
            this.client = client;
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

    private static final class ChunkedZipResponse implements Releasable {

        private BytesStream target;

        private final OutputStream out = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                assert target != null;
                target.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                assert target != null;
                target.write(b, off, len);
            }
        };

        private final ZipOutputStream zipOutputStream = new ZipOutputStream(out, StandardCharsets.UTF_8);

        private final CancellableTask task;
        private final Client client;

        ChunkedZipResponse(CancellableTask task, Client client) {
            this.task = task;
            this.client = client;
        }

        public ChunkedRestResponseBodyPart getFirstBodyPart() {
            return null;
        }

        @Override
        public void close() {
            // TODO
        }

        public <EntryResponse extends ActionResponse> void execute(
            String zipEntryName,
            ActionType<EntryResponse> type,
            ActionRequest request,
            ActionListener<Void> listener
        ) {}

        public void finish(ActionListener<Void> l) {

        }
    }

    public static final class TransportAction extends org.elasticsearch.action.support.TransportAction<Request, ActionResponse.Empty> {
        private final TransportService transportService;

        @Inject
        public TransportAction(TransportService transportService, ActionFilters actionFilters) {
            super(INSTANCE.name(), actionFilters, transportService.getTaskManager());
            this.transportService = transportService;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<ActionResponse.Empty> listener) {
            assert task instanceof CancellableTask;
            final var chunkedZipResponse = new ChunkedZipResponse(
                (CancellableTask) task,
                new ParentTaskAssigningClient(request.client, transportService.getLocalNode(), task)
            );
            request.restChannel.sendResponse(
                RestResponse.chunked(RestStatus.OK, chunkedZipResponse.getFirstBodyPart(), chunkedZipResponse)
            );

            SubscribableListener
                // nodes info
                .<Void>newForked(l -> chunkedZipResponse.execute("nodes.json", TransportNodesInfoAction.TYPE, new NodesInfoRequest(), l))
                // nodes stats
                .<Void>andThen(
                    (l, v) -> chunkedZipResponse.execute("nodes_stats.json", TransportNodesStatsAction.TYPE, new NodesStatsRequest(), l)
                )
                // cluster stats
                .<Void>andThen(
                    (l, v) -> chunkedZipResponse.execute(
                        "cluster_stats.json",
                        TransportClusterStatsAction.TYPE,
                        new ClusterStatsRequest(),
                        l
                    )
                )
                // finish
                .<Void>andThen((l, v) -> chunkedZipResponse.finish(l))
                .addListener(listener.map(v -> ActionResponse.Empty.INSTANCE));

        }
    }

}
