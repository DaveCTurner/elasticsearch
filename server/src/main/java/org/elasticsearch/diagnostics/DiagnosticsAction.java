/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.diagnostics;

import org.apache.lucene.util.BytesRef;
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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
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

    private record ChunkedZipEntry(ZipEntry zipEntry, ChunkedRestResponseBodyPart firstBodyPart, Releasable releasable) {}

    private static final class ChunkedZipResponse {

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

        private final String filename;
        private final RestChannel restChannel;
        private final Queue<ChunkedZipEntry> entryQueue = new LinkedBlockingQueue<>();
        private final AtomicInteger queueLength = new AtomicInteger();

        @Nullable // if the first part hasn't been sent yet
        private SubscribableListener<ChunkedRestResponseBodyPart> continuationListener;

        ChunkedZipResponse(String filename, RestChannel restChannel) {
            this.filename = filename;
            this.restChannel = restChannel;
        }

        public ChunkedRestResponseBodyPart getFirstBodyPart() {
            return null;
        }

        public void close() {
            // TODO
        }

        public <T extends ToXContent> ActionListener<T> newXContentListener(String entryName, ActionListener<Void> listener) {
            return newRawListener(entryName, listener).map(
                response -> ChunkedRestResponseBodyPart.fromXContent(
                    p -> ChunkedToXContentHelper.singleChunk(response),
                    restChannel.request(),
                    restChannel
                )
            );
        }

        public <T extends ChunkedToXContent> ActionListener<T> newChunkedXContentListener(String entryName, ActionListener<Void> listener) {
            return newRawListener(entryName, listener).map(
                response -> ChunkedRestResponseBodyPart.fromXContent(response, restChannel.request(), restChannel)
            );
        }

        public ActionListener<ChunkedRestResponseBodyPart> newRawListener(String entryName, ActionListener<Void> listener) {
            final var zipEntry = new ZipEntry(entryName);
            return ActionListener.assertOnce(new ActionListener<>() {
                @Override
                public void onResponse(ChunkedRestResponseBodyPart firstBodyPart) {
                    try {
                        enqueueEntry(zipEntry, firstBodyPart, this::completeListener);
                    } catch (Exception e) {
                        enqueueFailureEntry(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    enqueueFailureEntry(e);
                }

                private void enqueueFailureEntry(Exception e) {
                    enqueueEntry(
                        zipEntry,
                        ChunkedRestResponseBodyPart.fromTextChunks(
                            TEXT_CONTENT_TYPE,
                            List.<CheckedConsumer<Writer, IOException>>of(w -> e.printStackTrace(new PrintWriter(w))).iterator()
                        ),
                        this::completeListener
                    );
                }

                private void completeListener() {
                    listener.onResponse(null);
                }
            });
        }

        public void finish(ActionListener<Void> listener) {
            enqueueEntry(null, null, () -> listener.onResponse(null));
        }

        private void enqueueEntry(ZipEntry zipEntry, ChunkedRestResponseBodyPart firstBodyPart, Releasable releasable) {
            entryQueue.add(new ChunkedZipEntry(zipEntry, firstBodyPart, releasable));
            if (queueLength.getAndIncrement() == 0) {
                final var continuation = new QueueConsumer();
                final var currentContinuationListener = continuationListener;
                continuationListener = new SubscribableListener<>();
                if (currentContinuationListener == null) {
                    final var restResponse = RestResponse.chunked(RestStatus.OK, continuation, this::close);
                    restResponse.addHeader("content-disposition", Strings.format("attachment; filename=\"%s\"", filename));
                    restChannel.sendResponse(restResponse);
                } else {
                    currentContinuationListener.onResponse(continuation);
                }
            }
        }

        private final class QueueConsumer implements ChunkedRestResponseBodyPart {

            private ChunkedZipEntry currentEntry;

            @Override
            public boolean isPartComplete() {
                return currentEntry != null && currentEntry.firstBodyPart().isPartComplete();
            }

            @Override
            public boolean isLastPart() {
                return currentEntry != null && currentEntry.zipEntry() == null && currentEntry.firstBodyPart().isPartComplete();
            }

            @Override
            public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
                continuationListener.addListener(listener);
            }

            @Override
            public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
                // TODO implementation
                // TODO need to handle the case where the _current_ entry has a continuation (i.e. pauses) blocking the rest of the queue
                return null;
            }

            @Override
            public String getResponseContentTypeString() {
                return "application/zip";
            }
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
            doExecute(
                new ParentTaskAssigningClient(request.client, transportService.getLocalNode(), task),
                new ChunkedZipResponse(
                    "diagnostics-" + ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT) + ".zip",
                    request.restChannel
                ),
                listener.map(v -> ActionResponse.Empty.INSTANCE)
            );
        }

        private void doExecute(Client client, ChunkedZipResponse response, ActionListener<Void> listener) {
            SubscribableListener
                // nodes info
                .<Void>newForked(
                    l -> client.execute(
                        TransportNodesInfoAction.TYPE,
                        new NodesInfoRequest(),
                        response.newXContentListener("nodes.json", l)
                    )
                )
                // nodes stats
                .<Void>andThen(
                    (l, v) -> client.execute(
                        TransportNodesStatsAction.TYPE,
                        new NodesStatsRequest(),
                        response.newChunkedXContentListener("nodes_stats.json", l)
                    )
                )
                // cluster stats
                .<Void>andThen(
                    (l, v) -> client.execute(
                        TransportClusterStatsAction.TYPE,
                        new ClusterStatsRequest(),
                        response.newXContentListener("cluster_stats.json", l)
                    )
                )
                // finish
                .<Void>andThen((l, v) -> response.finish(l))
                .addListener(listener);
        }
    }

}
