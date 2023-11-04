/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.support.CancellableActionTestPlugin;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.elasticsearch.test.TaskAssertions.assertAllTasksHaveFinished;

public class IndicesRecoveryRestCancellationIT extends HttpSmokeTestCase {

    public void testIndicesRecoveryRestCancellation() throws Exception {
        runTest(new Request(HttpGet.METHOD_NAME, "/_recovery"));
    }

    public void testCatRecoveryRestCancellation() throws Exception {
        runTest(new Request(HttpGet.METHOD_NAME, "/_cat/recovery"));
    }

    private void runTest(Request request) throws Exception {
        createIndex("test");
        ensureGreen("test");

        final var clusterState = internalCluster().clusterService().state();
        final var blockedNode = randomFrom(
            clusterState.routingTable()
                .index("test")
                .allShards()
                .flatMap(IndexShardRoutingTable::allShards)
                .map(s -> clusterState.nodes().get(s.currentNodeId()).getName())
                .collect(Collectors.toSet())
        );

        try (var capturingAction = CancellableActionTestPlugin.capturingActionOnNode(RecoveryAction.NAME + "[n]", blockedNode)) {
            expectThrows(
                CancellationException.class,
                () -> PlainActionFuture.<Response, Exception>get(
                    responseFuture -> capturingAction.captureAndCancel(
                        getRestClient().performRequestAsync(request, wrapAsRestResponseListener(responseFuture))::cancel
                    ),
                    10,
                    TimeUnit.SECONDS
                )
            );
        }

        assertAllTasksHaveFinished(RecoveryAction.NAME);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), CancellableActionTestPlugin.class);
    }
}
