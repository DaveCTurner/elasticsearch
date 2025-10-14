/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.delete;

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.concurrent.CountDownLatch;

public class InvalidDataTierIT extends ESIntegTestCase {
    public void testInvalidDataTier() {

        final var badIndexName = "bad-index-" + randomIdentifier();
        final var goodIndexName = "good-index-" + randomIdentifier();

        final var cdl = new CountDownLatch(1);

        internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
            .submitUnbatchedStateUpdateTask("test", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    final var badIndexMetadata = IndexMetadata.builder(badIndexName)
                        .settings(
                            indexSettings(1, 0).put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersion.current())
                                .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                                .put(DataTier.TIER_PREFERENCE, DataTier.DATA_FROZEN)
                        )
                        .build();
                    final var goodIndexMetadata = IndexMetadata.builder(goodIndexName)
                        .settings(
                            indexSettings(1, 0).put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersion.current())
                                .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                        )
                        .build();
                    return ClusterState.builder(currentState)
                        .metadata(Metadata.builder(currentState.metadata()).put(badIndexMetadata, true).put(goodIndexMetadata, true))
                        .routingTable(
                            RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY, currentState.routingTable())
                                .addAsNew(badIndexMetadata)
                                .addAsNew(goodIndexMetadata)
                                .build()
                        )
                        .build();
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e);
                }

                @Override
                public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                    cdl.countDown();
                }
            });

        safeAwait(cdl);
        safeGet(
            client().execute(TransportClusterRerouteAction.TYPE, new ClusterRerouteRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT))
        );

        safeGet(client().execute(TransportDeleteIndexAction.TYPE, new DeleteIndexRequest(goodIndexName)));

    }
}
