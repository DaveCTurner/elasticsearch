/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Set;

public class TaskResultServiceTests extends ESTestCase {
    public void testSuccessResultStored() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var threadPool = deterministicTaskQueue.getThreadPool();
        final var localNode = DiscoveryNodeUtils.create(randomIdentifier());
        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()))
            .build();

        var client = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                logger.info("--> HERE [{}][{}]", action.name(), request);
                super.doExecute(action, request, listener);
            }
        };

        var taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of());
        taskManager.setTaskResultsService(new TaskResultsService(client, threadPool));
        taskManager.applyClusterState(new ClusterChangedEvent("test", clusterState, clusterState));

        var action = new TestTransportAction(taskManager);

        taskManager.registerAndExecute("transport", action, new TestRequest(true, TestResponse::new), null, new PlainActionFuture<>());
        taskManager.registerAndExecute(
            "transport",
            action,
            new TestRequest(true, TestXContentResponse::new),
            null,
            new PlainActionFuture<>()
        );

        taskManager.registerAndExecute(
            "transport",
            action,
            new TestRequest(true, () -> { throw new ElasticsearchException("simulated"); }),
            null,
            new PlainActionFuture<>()
        );

    }

    private static class TestRequest extends ActionRequest {
        private final boolean shouldStoreResult;
        private final CheckedSupplier<TestResponse, ? extends Exception> responseSupplier;

        TestRequest(boolean shouldStoreResult, CheckedSupplier<TestResponse, ? extends Exception> responseSupplier) {
            this.shouldStoreResult = shouldStoreResult;
            this.responseSupplier = responseSupplier;
        }

        @Override
        public boolean getShouldStoreResult() {
            return shouldStoreResult;
        }

        CheckedSupplier<TestResponse, ? extends Exception> getResponseSupplier() {
            return responseSupplier;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    private static class TestResponse extends ActionResponse {
        @Override
        public void writeTo(StreamOutput out) {}
    }

    private static class TestXContentResponse extends TestResponse implements ToXContent {
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field("response", "value");
        }
    }

    private static class TestTransportAction extends TransportAction<TestRequest, TestResponse> {
        TestTransportAction(TaskManager taskManager) {
            super("test:action", new ActionFilters(Set.of()), taskManager);
        }

        @Override
        protected void doExecute(Task task, TestRequest request, ActionListener<TestResponse> listener) {
            ActionListener.completeWith(listener, request.getResponseSupplier());
        }
    }
}
