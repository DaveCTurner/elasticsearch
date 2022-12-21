/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.repositories.integrity.VerifyRepositoryIntegrityAction;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestVerifyRepositoryIntegrityAction extends BaseRestHandler {
    private static final Logger logger = LogManager.getLogger(RestVerifyRepositoryIntegrityAction.class);

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_snapshot/{repository}/_verify_integrity"));
    }

    @Override
    public String getName() {
        return "verify_repository_integrity_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final var verifyRequest = new VerifyRepositoryIntegrityAction.Request(
            request.param("repository"),
            request.paramAsStringArray("indices", Strings.EMPTY_ARRAY),
            request.paramAsInt("thread_pool_concurrency", 0),
            request.paramAsInt("snapshot_verification_concurrency", 0),
            request.paramAsInt("index_verification_concurrency", 0),
            request.paramAsInt("index_snapshot_verification_concurrency", 0)
        );
        verifyRequest.masterNodeTimeout(request.paramAsTime("master_timeout", verifyRequest.masterNodeTimeout()));
        return channel -> {
            final var taskFuture = new ListenableActionFuture<TaskId>();
            final var listener = ActionListener.wrap(
                (ActionResponse.Empty ignored) -> taskFuture.addListener(
                    ActionListener.wrap(t -> logSuccess(t, verifyRequest), e -> logFailure(TaskId.EMPTY_TASK_ID, e, null, verifyRequest))
                ),
                e1 -> taskFuture.addListener(
                    ActionListener.wrap(
                        t -> logFailure(t, e1, null, verifyRequest),
                        e2 -> logFailure(TaskId.EMPTY_TASK_ID, e1, e2, verifyRequest)
                    )
                )
            );
            final var task = client.executeLocally(VerifyRepositoryIntegrityAction.INSTANCE, verifyRequest, listener);
            final var taskId = new TaskId(client.getLocalNodeId(), task.getId());
            taskFuture.onResponse(taskId);
            try (var builder = channel.newBuilder()) {
                builder.startObject();
                builder.field("task", taskId);
                builder.endObject();
                channel.sendResponse(new RestResponse(RestStatus.OK, builder));
            }
        };
    }

    private void logSuccess(TaskId taskId, VerifyRepositoryIntegrityAction.Request verifyRequest) {
        logger.debug("task [{}] completed repository verification of [{}]", taskId, verifyRequest.getRepository());
    }

    private void logFailure(TaskId taskId, Exception e1, Exception e2, VerifyRepositoryIntegrityAction.Request verifyRequest) {
        if (e2 != null) {
            e1.addSuppressed(e2);
        }
        logger.warn(format("task [{}] failed repository verification of [{}]", taskId, verifyRequest.getRepository()), e1);
    }
}
