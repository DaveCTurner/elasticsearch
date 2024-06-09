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
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedFunction;
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
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.elasticsearch.rest.RestResponse.TEXT_CONTENT_TYPE;

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

    private record ChunkedZipEntry(ZipEntry zipEntry, ChunkedRestResponseBodyPart firstBodyPart, ActionListener<Void> listener) {}

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

        private final Queue<ChunkedZipEntry> entryQueue = new LinkedBlockingQueue<>();

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
            ActionType<EntryResponse> actionType,
            ActionRequest request,
            CheckedFunction<EntryResponse, ChunkedRestResponseBodyPart, IOException> responseWriter,
            ActionListener<Void> listener
        ) {
            final var zipEntry = new ZipEntry(zipEntryName);
            client.execute(actionType, request, new ActionListener<>() {
                @Override
                public void onResponse(EntryResponse entryResponse) {
                    try {
                        entryQueue.add(new ChunkedZipEntry(zipEntry, responseWriter.apply(entryResponse), listener));
                    } catch (Exception e) {
                        enqueueFailureEntry(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    enqueueFailureEntry(e);
                }

                private void enqueueFailureEntry(Exception e) {
                    entryQueue.add(
                        new ChunkedZipEntry(
                            zipEntry,
                            ChunkedRestResponseBodyPart.fromTextChunks(
                                TEXT_CONTENT_TYPE,
                                List.<CheckedConsumer<Writer, IOException>>of(w -> e.printStackTrace(new PrintWriter(w))).iterator()
                            ),
                            listener
                        )
                    );
                }
            });
        }

        public void finish(ActionListener<Void> listener) {
            entryQueue.add(new ChunkedZipEntry(null, null, listener));
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
                .<Void>newForked(
                    l -> chunkedZipResponse.execute(
                        "nodes.json",
                        TransportNodesInfoAction.TYPE,
                        new NodesInfoRequest(),
                        response -> ChunkedRestResponseBodyPart.fromXContent(
                            p -> ChunkedToXContentHelper.singleChunk(response),
                            request.restChannel.request(),
                            request.restChannel
                        ),
                        l
                    )
                )
                // nodes stats
                .<Void>andThen(
                    (l, v) -> chunkedZipResponse.execute(
                        "nodes_stats.json",
                        TransportNodesStatsAction.TYPE,
                        new NodesStatsRequest(),
                        response -> ChunkedRestResponseBodyPart.fromXContent(response, request.restChannel.request(), request.restChannel),
                        l
                    )
                )
                // cluster stats
                .<Void>andThen(
                    (l, v) -> chunkedZipResponse.execute(
                        "cluster_stats.json",
                        TransportClusterStatsAction.TYPE,
                        new ClusterStatsRequest(),
                        response -> ChunkedRestResponseBodyPart.fromXContent(
                            p -> ChunkedToXContentHelper.singleChunk(response),
                            request.restChannel.request(),
                            request.restChannel
                        ),
                        l
                    )
                )
                // finish
                .<Void>andThen((l, v) -> chunkedZipResponse.finish(l))
                .addListener(listener.map(v -> ActionResponse.Empty.INSTANCE));

        }
    }

}
