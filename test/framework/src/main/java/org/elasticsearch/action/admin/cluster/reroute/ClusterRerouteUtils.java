/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.reroute;

import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommand;

import static org.elasticsearch.test.ESTestCase.TEST_REQUEST_TIMEOUT;
import static org.elasticsearch.test.ESTestCase.safeGet;
import static org.junit.Assert.assertTrue;

public class ClusterRerouteUtils {
    private ClusterRerouteUtils() {/* no instances */}

    public static void doReroute(ElasticsearchClient client, AllocationCommand... commands) {
        final var request = new ClusterRerouteRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        for (final var command : commands) {
            request.add(command);
        }
        assertTrue(safeGet(client.execute(TransportClusterRerouteAction.TYPE, request)).isAcknowledged());
    }
}
