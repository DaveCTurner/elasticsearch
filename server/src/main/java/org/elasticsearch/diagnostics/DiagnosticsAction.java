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
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class DiagnosticsAction {

    private DiagnosticsAction() {/* no instances */}

    public static final ActionType<DiagnosticsAction.Response> INSTANCE = new ActionType("cluster:monitor/diagnostics");

    public static final class Request extends ActionRequest {
        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static final class Response extends ActionResponse implements Releasable {
        private final NodeClient client;

        public Response(NodeClient client) {
            this.client = client;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }

        public ChunkedRestResponseBodyPart getFirstBodyPart() {



            return new ChunkedRestResponseBodyPart() {
                @Override
                public boolean isPartComplete() {
                    return false;
                }

                @Override
                public boolean isLastPart() {
                    return false;
                }

                @Override
                public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {

                }

                @Override
                public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
                    return null;
                }

                @Override
                public String getResponseContentTypeString() {
                    return "application/zip";
                }
            };
        }

        @Override
        public void close() {}
    }

    private static final class ChunkedZipResponse {

    }

    public static final class TransportAction extends org.elasticsearch.action.support.TransportAction<Request, Response> {
        private final NodeClient client;

        @Inject
        public TransportAction(NodeClient client, TransportService transportService, ActionFilters actionFilters) {
            super(INSTANCE.name(), actionFilters, transportService.getTaskManager());
            this.client = client;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            listener.onResponse(new Response(client));
        }
    }

}
