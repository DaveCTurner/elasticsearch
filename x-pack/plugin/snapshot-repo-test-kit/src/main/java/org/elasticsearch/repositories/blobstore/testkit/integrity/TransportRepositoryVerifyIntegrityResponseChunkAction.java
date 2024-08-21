/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.Executor;

public class TransportRepositoryVerifyIntegrityResponseChunkAction extends HandledTransportAction<
    TransportRepositoryVerifyIntegrityResponseChunkAction.Request,
    ActionResponse.Empty> {

    static final String SNAPSHOT_CHUNK_ACTION_NAME = TransportRepositoryVerifyIntegrityCoordinationAction.INSTANCE.name()
        + "[response_chunk]";

    private final OngoingRequests ongoingRequests;

    public TransportRepositoryVerifyIntegrityResponseChunkAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Executor executor,
        OngoingRequests ongoingRequests
    ) {
        super(SNAPSHOT_CHUNK_ACTION_NAME, transportService, actionFilters, Request::new, executor);
        this.ongoingRequests = ongoingRequests;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<ActionResponse.Empty> listener) {
        ActionListener.run(
            listener,
            l -> ongoingRequests.get(request.taskId)
                .writeFragment(
                    p0 -> ChunkedToXContentHelper.singleChunk((b, p) -> b.startObject().value(request.chunkContents(), p).endObject()),
                    () -> l.onResponse(ActionResponse.Empty.INSTANCE)
                )
        );
    }

    public static class Request extends ActionRequest {

        private final long taskId;
        private final ResponseWriter.ResponseChunk chunkContents;

        public Request(long taskId, ResponseWriter.ResponseChunk chunkContents) {
            this.taskId = taskId;
            this.chunkContents = chunkContents;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            taskId = in.readVLong();
            chunkContents = new ResponseWriter.ResponseChunk(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(taskId);
            chunkContents.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public ResponseWriter.ResponseChunk chunkContents() {
            return chunkContents;
        }
    }
}
