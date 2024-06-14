/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.diagnostics;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
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
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.ChunkedZipResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Iterator;
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

        private final Client rawClient;
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
            this.rawClient = client;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<ActionResponse.Empty> listener) {
            assert task instanceof CancellableTask;
            try (
                var refs = new RefCountingRunnable(() -> listener.onResponse(ActionResponse.Empty.INSTANCE));
                var response = new ChunkedZipResponse(
                    "elasticsearch-internal-diagnostics-"
                        + clusterService.getClusterName().value()
                        + "-"
                        + ZonedDateTime.now(ZoneOffset.UTC).format(FILENAME_DATE_TIME_FORMATTER),
                    request.restChannel,
                    refs.acquire()
                )
            ) {
                new AsyncAction(
                    (CancellableTask) task,
                    request.restChannel,
                    refs,
                    response,
                    new ParentTaskAssigningClient(rawClient, clusterService.localNode(), task)
                ).run(clusterService);
            }
        }
    }

    private static final class AsyncAction {

        private final CancellableTask task;
        private final RestChannel restChannel;
        private final RefCountingRunnable refs;
        private final ChunkedZipResponse response;
        private final ParentTaskAssigningClient client;

        AsyncAction(
            CancellableTask task,
            RestChannel restChannel,
            RefCountingRunnable refs,
            ChunkedZipResponse response,
            ParentTaskAssigningClient client
        ) {
            this.task = task;
            this.restChannel = restChannel;
            this.refs = refs;
            this.response = response;
            this.client = client;
        }

        public void run(ClusterService clusterService) {
            client.execute(TransportNodesInfoAction.TYPE, new NodesInfoRequest(), newToXContentListener("nodes.json", refs.acquire()));

            client.execute(
                TransportNodesStatsAction.TYPE,
                new NodesStatsRequest(),
                newChunkedXContentListener("nodes_stats.json", refs.acquire())
            );

            client.execute(
                TransportClusterStatsAction.TYPE,
                new ClusterStatsRequest(),
                newToXContentListener("cluster_stats.json", refs.acquire())
            );

            newChunkedXContentListener("cluster_state.json", refs.acquire()).onResponse(clusterService.state());

            newChunkedXContentListener("example_exception.json", refs.acquire()).onFailure(new ElasticsearchException("test"));
        }

        private <T extends ToXContent> ActionListener<T> newToXContentListener(String entryName, Releasable releasable) {
            final var completionListener = new SubscribableListener<Void>();
            completionListener.addListener(ActionListener.releasing(releasable));
            return onExceptionSendJsonListener(entryName, completionListener).map(response -> {
                if (response instanceof RefCounted refCounted) {
                    refCounted.mustIncRef();
                    completionListener.addListener(ActionListener.releasing(refCounted::decRef));
                }
                return ChunkedRestResponseBodyPart.fromXContent(op -> ChunkedToXContentHelper.singleChunk((b, p) -> {
                    b.humanReadable(true);
                    final var isFragment = response.isFragment();
                    if (isFragment) {
                        b.startObject();
                    }
                    response.toXContent(b, p);
                    if (isFragment) {
                        b.endObject();
                    }
                    return b;
                }), restChannel.request(), restChannel);
            });
        }

        private <T extends ChunkedToXContent> ActionListener<T> newChunkedXContentListener(String entryName, Releasable releasable) {
            final var completionListener = new SubscribableListener<Void>();
            completionListener.addListener(ActionListener.releasing(releasable));
            return onExceptionSendJsonListener(entryName, completionListener).map(response -> {
                if (response instanceof RefCounted refCounted) {
                    refCounted.mustIncRef();
                    completionListener.addListener(ActionListener.releasing(refCounted::decRef));
                }
                return ChunkedRestResponseBodyPart.fromXContent(new ChunkedToXContentObject() {
                    @Override
                    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
                        final var isFragment = response.isFragment();
                        return Iterators.concat(
                            ChunkedToXContentHelper.singleChunk((b, p) -> b.humanReadable(true)),
                            isFragment ? ChunkedToXContentHelper.singleChunk((b, p) -> b.startObject()) : Collections.emptyIterator(),
                            response.toXContentChunked(params),
                            isFragment ? ChunkedToXContentHelper.singleChunk((b, p) -> b.endObject()) : Collections.emptyIterator()
                        );
                    }

                    @Override
                    public Iterator<? extends ToXContent> toXContentChunkedV7(ToXContent.Params params) {
                        final var isFragment = response.isFragment();
                        return Iterators.concat(
                            ChunkedToXContentHelper.singleChunk((b, p) -> b.humanReadable(true)),
                            isFragment ? ChunkedToXContentHelper.singleChunk((b, p) -> b.startObject()) : Collections.emptyIterator(),
                            response.toXContentChunkedV7(params),
                            isFragment ? ChunkedToXContentHelper.singleChunk((b, p) -> b.endObject()) : Collections.emptyIterator()
                        );
                    }
                }, restChannel.request(), restChannel);
            });
        }

        private ActionListener<ChunkedRestResponseBodyPart> onExceptionSendJsonListener(
            String entryName,
            ActionListener<Void> completionListener
        ) {
            return response.newEntryListener(entryName, completionListener)
                .delegateResponse(
                    (l, e) -> ActionListener.completeWith(
                        l,
                        () -> ChunkedRestResponseBodyPart.fromXContent(op -> ChunkedToXContentHelper.singleChunk((b, p) -> {
                            b.humanReadable(true);
                            b.startObject();
                            ElasticsearchException.generateFailureXContent(
                                b,
                                new ToXContent.DelegatingMapParams(
                                    Map.of(ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE, "false"),
                                    p
                                ),
                                e,
                                true
                            );
                            b.field("status", ExceptionsHelper.status(e).getStatus());
                            b.endObject();
                            return b;
                        }), restChannel.request(), restChannel)
                    )
                );
        }
    }
}
