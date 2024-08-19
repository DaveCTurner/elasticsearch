/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.ResourceNotFoundException;
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
import java.util.Map;
import java.util.concurrent.Executor;

public class TransportRepositoryVerifyIntegritySnapshotChunkAction extends HandledTransportAction<
    TransportRepositoryVerifyIntegritySnapshotChunkAction.Request,
    ActionResponse.Empty> {

    static final String SNAPSHOT_CHUNK_ACTION_NAME = TransportRepositoryVerifyIntegrityCoordinationAction.INSTANCE.name()
        + "[snapshot_chunk]";

    private final Map<Long, TransportRepositoryVerifyIntegrityCoordinationAction.Request> ongoingRequests;

    public TransportRepositoryVerifyIntegritySnapshotChunkAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Executor executor,
        Map<Long, TransportRepositoryVerifyIntegrityCoordinationAction.Request> ongoingRequests
    ) {
        super(SNAPSHOT_CHUNK_ACTION_NAME, transportService, actionFilters, Request::new, executor);
        this.ongoingRequests = ongoingRequests;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<ActionResponse.Empty> listener) {
        final var outerRequest = ongoingRequests.get(request.taskId);
        if (outerRequest == null) {
            throw new ResourceNotFoundException("verify task [" + request.taskId + "] not found");
        }

        outerRequest.writeFragment(
            p0 -> ChunkedToXContentHelper.singleChunk((b, p) -> b.startObject().field("id", request.id).endObject()),
            () -> listener.onResponse(ActionResponse.Empty.INSTANCE)
        );
    }

    public static class Request extends ActionRequest {

        private final long taskId;
        private final int id;

        public Request(long taskId, int id) {
            this.taskId = taskId;
            this.id = id;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            taskId = in.readVLong();
            id = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(taskId);
            out.writeVInt(id);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }
}
