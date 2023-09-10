/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.test.rest.ObjectPath;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.oneOf;

public class TransportVersionClusterStateUpgradeIT extends AbstractUpgradeTestCase {

    public void testReadsInferredTransportVersions() throws Exception {
        // waitUntil because the versions fixup on upgrade happens in the background so may need a retry
        assertTrue(waitUntil(() -> {
            try {
                return runTransportVersionsTest();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }));
    }

    private boolean runTransportVersionsTest() throws Exception {
        final var clusterState = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/_cluster/state/nodes")));
        final var description = clusterState.toString();

        final var nodeIds = clusterState.evaluateMapKeys("nodes");
        final Map<String, Version> versionsByNodeId = Maps.newHashMapWithExpectedSize(nodeIds.size());
        for (final var nodeId : nodeIds) {
            versionsByNodeId.put(nodeId, Version.fromString(clusterState.evaluate("nodes." + nodeId + ".version")));
        }

        final var hasTransportVersions = clusterState.evaluate("transport_versions") != null;
        final var hasNodesVersions = clusterState.evaluate("nodes_versions") != null;
        assertFalse(description, hasNodesVersions && hasTransportVersions);

        switch (CLUSTER_TYPE) {
            case OLD -> {
                if (UPGRADE_FROM_VERSION.before(Version.V_8_8_0)) {
                    assertFalse(description, hasTransportVersions);
                    assertFalse(description, hasNodesVersions);
                } else if (UPGRADE_FROM_VERSION.before(Version.V_8_11_0)) {
                    assertTrue(description, hasTransportVersions);
                    assertFalse(description, hasNodesVersions);
                } else {
                    assertFalse(description, hasTransportVersions);
                    assertTrue(description, hasNodesVersions);
                }
            }
            case MIXED -> {
                if (UPGRADE_FROM_VERSION.before(Version.V_8_8_0)) {
                    assertFalse(description, hasTransportVersions);
                } else if (UPGRADE_FROM_VERSION.before(Version.V_8_11_0)) {
                    assertTrue(description, hasNodesVersions || hasTransportVersions);
                } else {
                    assertFalse(description, hasTransportVersions);
                    assertTrue(description, hasNodesVersions);
                }
            }
            case UPGRADED -> {
                assertFalse(description, hasTransportVersions);
                assertTrue(description, hasNodesVersions);
            }
        }

        if (hasTransportVersions) {
            assertTrue(description, UPGRADE_FROM_VERSION.before(Version.V_8_11_0));
            assertTrue(description, UPGRADE_FROM_VERSION.onOrAfter(Version.V_8_8_0));
            assertNotEquals(description, ClusterType.UPGRADED, CLUSTER_TYPE);

            assertEquals(description, nodeIds.size(), clusterState.evaluateArraySize("transport_versions"));
            for (int i = 0; i < nodeIds.size(); i++) {
                final var path = "transport_versions." + i;
                final String nodeId = clusterState.evaluate(path + ".node_id");
                final var nodeDescription = nodeId + "/" + description;
                final var transportVersion = TransportVersion.fromString(clusterState.evaluate(path + ".transport_version"));
                final var nodeVersion = versionsByNodeId.get(nodeId);
                assertNotNull(nodeDescription, nodeVersion);
                if (nodeVersion.equals(Version.CURRENT)) {
                    assertEquals(nodeDescription, TransportVersion.current(), transportVersion);
                } else if (nodeVersion.after(Version.V_8_8_0)) {
                    assertTrue(nodeDescription, TransportVersions.V_8_8_0.after(transportVersion));
                } else {
                    assertEquals(nodeDescription, TransportVersions.V_8_8_0, transportVersion);
                }
            }
        } else if (hasNodesVersions) {
            assertFalse(description, UPGRADE_FROM_VERSION.before(Version.V_8_11_0) && CLUSTER_TYPE == ClusterType.OLD);
            assertEquals(description, nodeIds.size(), clusterState.evaluateArraySize("nodes_versions"));
            for (int i = 0; i < nodeIds.size(); i++) {
                final var path = "nodes_versions." + i;
                final String nodeId = clusterState.evaluate(path + ".node_id");
                final var nodeDescription = nodeId + "/" + description;
                final var transportVersion = TransportVersion.fromString(clusterState.evaluate(path + ".transport_version"));
                final var nodeVersion = versionsByNodeId.get(nodeId);
                assertNotNull(nodeDescription, nodeVersion);
                if (nodeVersion.equals(Version.CURRENT)) {
                    assertThat(
                        nodeDescription,
                        transportVersion,
                        UPGRADE_FROM_VERSION.onOrAfter(Version.V_8_8_0)
                            ? equalTo(TransportVersion.current())
                            : oneOf(TransportVersion.current(), TransportVersions.V_8_8_0)
                    );
                    if (transportVersion.equals(TransportVersions.V_8_8_0)) {
                        logger.info("{} - not fixed up yet, retrying", nodeDescription);
                        // not fixed up yet
                        return false;
                    }
                } else if (nodeVersion.after(Version.V_8_8_0)) {
                    assertTrue(nodeDescription, TransportVersions.V_8_8_0.after(transportVersion));
                } else {
                    assertEquals(nodeDescription, TransportVersions.V_8_8_0, transportVersion);
                }
            }
        }

        return true;
    }
}
