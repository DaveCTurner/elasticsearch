/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ESNetty4IntegTestCase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.ChunkedRestResponseBody;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestResponse.TEXT_CONTENT_TYPE;
import static org.hamcrest.Matchers.containsString;

public class Netty4ChunkedContinuationsIT extends ESNetty4IntegTestCase {
    private static final Logger logger = LogManager.getLogger(Netty4ChunkedContinuationsIT.class);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), YieldsContinuationsPlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    public void testBasic() throws IOException {
        final var response = getRestClient().performRequest(new Request("GET", YieldsContinuationsPlugin.ROUTE));
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertThat(response.getEntity().getContentType().toString(), containsString(TEXT_CONTENT_TYPE));
        assertTrue(response.getEntity().isChunked());
        String body;
        try (var reader = new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8)) {
            body = Streams.copyToString(reader);
        }
        assertEquals("""
            batch-0-chunk-0
            batch-0-chunk-1
            batch-0-chunk-2
            batch-1-chunk-0
            batch-1-chunk-1
            batch-1-chunk-2
            batch-2-chunk-0
            batch-2-chunk-1
            batch-2-chunk-2
            """, body);
    }

    public static class YieldsContinuationsPlugin extends Plugin implements ActionPlugin {
        static final String ROUTE = "/_test/yields_continuations";

        private static final ActionType<YieldsContinuationsPlugin.Response> TYPE = new ActionType<>("test:yields_continuations");

        @Override
        public Collection<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return List.of(new ActionHandler<>(TYPE, TransportYieldsContinuationsAction.class));
        }

        public static class Request extends ActionRequest {
            @Override
            public ActionRequestValidationException validate() {
                return null;
            }
        }

        public static class Response extends ActionResponse {
            private final Scheduler scheduler;

            public Response(Scheduler scheduler) {
                this.scheduler = scheduler;
            }

            @Override
            public void writeTo(StreamOutput out) {
                TransportAction.localOnly();
            }

            public ChunkedRestResponseBody getChunkedBody() {
                return getChunkBatch(0);
            }

            private ChunkedRestResponseBody getChunkBatch(int batchIndex) {
                return new ChunkedRestResponseBody() {

                    private final Iterator<String> lines = Iterators.forRange(0, 3, i -> "batch-" + batchIndex + "-chunk-" + i + "\n");

                    @Override
                    public boolean isDone() {
                        return lines.hasNext() == false;
                    }

                    @Override
                    public boolean isEndOfResponse() {
                        return batchIndex == 2;
                    }

                    @Override
                    public void getContinuation(ActionListener<ChunkedRestResponseBody> listener) {
                        scheduler.schedule(
                            ActionRunnable.supply(listener, () -> getChunkBatch(batchIndex + 1)),
                            TimeValue.timeValueSeconds(1),
                            EsExecutors.DIRECT_EXECUTOR_SERVICE
                        );
                    }

                    @Override
                    public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
                        assert lines.hasNext();
                        final var output = new RecyclerBytesStreamOutput(recycler);
                        boolean success = false;
                        try {
                            try (var writer = new OutputStreamWriter(Streams.flushOnCloseStream(output), StandardCharsets.UTF_8)) {
                                writer.write(lines.next());
                            }
                            final var result = new ReleasableBytesReference(output.bytes(), output);
                            success = true;
                            return result;
                        } finally {
                            if (success == false) {
                                output.close();
                            }
                        }
                    }

                    @Override
                    public String getResponseContentTypeString() {
                        return TEXT_CONTENT_TYPE;
                    }
                };
            }
        }

        public static class TransportYieldsContinuationsAction extends TransportAction<Request, Response> {
            private final ThreadPool threadPool;

            @Inject
            public TransportYieldsContinuationsAction(ActionFilters actionFilters, TransportService transportService) {
                super(TYPE.name(), actionFilters, transportService.getTaskManager());
                threadPool = transportService.getThreadPool();
            }

            @Override
            protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
                threadPool.schedule(
                    ActionRunnable.supply(listener, () -> new Response(threadPool)),
                    TimeValue.timeValueSeconds(1),
                    EsExecutors.DIRECT_EXECUTOR_SERVICE
                );
            }
        }

        @Override
        public Collection<RestHandler> getRestHandlers(
            Settings settings,
            NamedWriteableRegistry namedWriteableRegistry,
            RestController restController,
            ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings,
            SettingsFilter settingsFilter,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster
        ) {
            return List.of(new BaseRestHandler() {
                @Override
                public String getName() {
                    return ROUTE;
                }

                @Override
                public List<Route> routes() {
                    return List.of(new Route(GET, ROUTE));
                }

                @Override
                protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
                    return channel -> client.execute(TYPE, new Request(), new RestActionListener<>(channel) {
                        @Override
                        protected void processResponse(Response response) {
                            channel.sendResponse(
                                RestResponse.chunked(
                                    RestStatus.OK,
                                    response.getChunkedBody(),
                                    () -> logger.info("--> response closed") // TODO test lifecycle
                                )
                            );
                        }
                    });
                }
            });
        }
    }
}
