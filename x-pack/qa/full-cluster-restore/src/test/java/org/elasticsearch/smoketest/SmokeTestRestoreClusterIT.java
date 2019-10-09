/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.test.SecuritySettingsSourceField.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;

public class SmokeTestRestoreClusterIT extends ESRestTestCase {

    private enum ClusterPhase {
        ORIGINAL,
        RESTORING,
        RESTORED;

        public static ClusterPhase parse(String value) {
            switch (value) {
                case "original":
                    return ORIGINAL;
                case "restoring":
                    return RESTORING;
                case "restored":
                    return RESTORED;
                default:
                    throw new AssertionError("unknown cluster type: " + value);
            }
        }
    }

    protected static final ClusterPhase CLUSTER_PHASE = ClusterPhase.parse(System.getProperty("tests.test_cluster_phase"));

    @Override
    protected Settings restClientSettings() {
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization",
                basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray())))
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

        switch(CLUSTER_PHASE) {
            case ORIGINAL:
            {
                XContentBuilder settings = jsonBuilder();
                settings.startObject();
                {
                    settings.startObject("settings");
                    settings.field("number_of_shards", 1);
                    settings.field("number_of_replicas", 0);
                    settings.endObject();
                }
                settings.endObject();

                Request createIndex = new Request("PUT", "/test-index");
                createIndex.setJsonEntity(Strings.toString(settings));
                assertOK(client().performRequest(createIndex));

                final Request healthRequest = new Request("GET", "/_cluster/health");
                healthRequest.addParameter("wait_for_status", "green");
                assertOK(client().performRequest(healthRequest));
                break;
            }

            case RESTORING:
            {
                assertThat(client().performRequest(new Request("HEAD", "/test-index")).getStatusLine().getStatusCode(), equalTo(404));
                break;
            }

            case RESTORED:
            {
                final Request healthRequest = new Request("GET", "/_cluster/health/test-index");
                healthRequest.addParameter("wait_for_status", "green");
                assertOK(client().performRequest(healthRequest));
                break;
            }
        }
    }
}
