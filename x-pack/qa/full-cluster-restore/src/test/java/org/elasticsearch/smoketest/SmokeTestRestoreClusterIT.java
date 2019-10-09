/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.rest.ESRestTestCase;

public class SmokeTestRestoreClusterIT extends ESRestTestCase {
    public void testClusterEmitsNoSmoke() throws Exception {
        assertOK(client().performRequest(new Request("GET", "/_cluster/health")));
    }
}
