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

import java.util.Objects;

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

    private static final ClusterPhase CLUSTER_PHASE = ClusterPhase.parse(System.getProperty("tests.test_cluster_phase"));

    @Override
    protected boolean preserveClusterUponCompletion() {
        return CLUSTER_PHASE == ClusterPhase.RESTORING;
    }

    @Override
    protected boolean preserveSnapshotsUponCompletion() {
        return CLUSTER_PHASE == ClusterPhase.ORIGINAL;
    }

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
        logger.info("running in phase [{}]", CLUSTER_PHASE);

        assertOK(client().performRequest(new Request("GET", "/_cluster/health")));

        if (CLUSTER_PHASE != ClusterPhase.RESTORED) {
            final XContentBuilder createRepositoryBody = jsonBuilder();
            createRepositoryBody.startObject();
            createRepositoryBody.field("type", "fs");
            createRepositoryBody.startObject("settings");
            createRepositoryBody.field("location", Objects.requireNonNull(System.getProperty("tests.path.repo")));
            createRepositoryBody.endObject();
            createRepositoryBody.endObject();

            final Request createRepository = new Request("PUT", "/_snapshot/repo");
            createRepository.setJsonEntity(Strings.toString(createRepositoryBody));
            assertOK(client().performRequest(createRepository));
        }

        switch (CLUSTER_PHASE) {
            case ORIGINAL: {
                final XContentBuilder createIndexBody = jsonBuilder();
                createIndexBody.startObject();
                {
                    createIndexBody.startObject("settings");
                    createIndexBody.field("number_of_shards", 1);
                    createIndexBody.field("number_of_replicas", 0);
                    createIndexBody.endObject();
                }
                createIndexBody.endObject();

                final Request createIndex = new Request("PUT", "/test-index");
                createIndex.setJsonEntity(Strings.toString(createIndexBody));
                assertOK(client().performRequest(createIndex));

                final Request healthRequest = new Request("GET", "/_cluster/health");
                healthRequest.addParameter("wait_for_status", "green");
                assertOK(client().performRequest(healthRequest));

                final Request createSnapshot = new Request("PUT", "/_snapshot/repo/full_snapshot");
                createSnapshot.addParameter("wait_for_completion", "true");
                assertOK(client().performRequest(createSnapshot));

                break;
            }

            case RESTORING: {
                assertThat(client().performRequest(new Request("HEAD", "/test-index")).getStatusLine().getStatusCode(), equalTo(404));

                final long delayMillis = between(0, 20000);
                logger.info("delaying for {}ms", delayMillis);
                Thread.sleep(delayMillis);

                final XContentBuilder restoreSnapshotBody = jsonBuilder();
                restoreSnapshotBody.startObject();
                restoreSnapshotBody.field("include_global_state", "true");
                restoreSnapshotBody.endObject();

                final Request restoreSnapshot = new Request("POST", "/_snapshot/repo/full_snapshot/_restore");
                restoreSnapshot.addParameter("wait_for_completion", "true");
                restoreSnapshot.setJsonEntity(Strings.toString(restoreSnapshotBody));
                assertOK(client().performRequest(restoreSnapshot));

                break;
            }

            case RESTORED: {
                final Request healthRequest = new Request("GET", "/_cluster/health");
                healthRequest.addParameter("wait_for_status", "green");
                assertOK(client().performRequest(healthRequest));

                assertThat(client().performRequest(new Request("HEAD", "/test-index")).getStatusLine().getStatusCode(), equalTo(200));

                break;
            }
        }
    }
}
