/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;

import static org.elasticsearch.xpack.test.SecuritySettingsSourceField.basicAuthHeaderValue;

public class SmokeTestRestoreClusterIT extends ESRestTestCase {

    @Override
    protected Settings restClientSettings() {
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization",
                basicAuthHeaderValue("powerless_user", new SecureString("x-pack-test-password".toCharArray())))
            .build();
    }

    @Override
    protected Settings restAdminSettings() {
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization",
                basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray())))
            .build();
    }

    public void testClusterEmitsNoSmoke() throws Exception {
        assertOK(client().performRequest(new Request("GET", "/_cluster/health")));
    }
}
