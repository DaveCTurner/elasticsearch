/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.support.CancellableActionTestPlugin;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.elasticsearch.test.TaskAssertions.assertAllTasksHaveFinished;

public class GetAliasesCancellabilityIT extends HttpSmokeTestCase {

    public void testGetAliasesCancellation() throws Exception {
        runCancellationTest(new Request("GET", "/_alias"));
    }

    public void testCatAliasesCancellation() throws Exception {
        runCancellationTest(new Request("GET", "/_cat/aliases"));
    }

    private void runCancellationTest(Request request) throws Exception {
        final var node = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        final var nodeInfo = client(node).admin().cluster().prepareNodesInfo("_local").get().getNodes();

        try (
            var client = createRestClient(nodeInfo, null, "http");
            var capturingAction = CancellableActionTestPlugin.capturingActionOnNode(GetAliasesAction.NAME, node)
        ) {
            expectThrows(
                CancellationException.class,
                () -> PlainActionFuture.<Response, Exception>get(
                    responseFuture -> capturingAction.captureAndCancel(
                        client.performRequestAsync(request, wrapAsRestResponseListener(responseFuture))::cancel
                    ),
                    10,
                    TimeUnit.SECONDS
                )
            );
        }

        assertAllTasksHaveFinished(GetAliasesAction.NAME);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), CancellableActionTestPlugin.class);
    }

}
