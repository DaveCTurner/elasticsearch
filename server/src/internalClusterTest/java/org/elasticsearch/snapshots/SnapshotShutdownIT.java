/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.hasSize;

public class SnapshotShutdownIT extends AbstractSnapshotIntegTestCase {

    public void testRemoveNodeDuringSnapshot() throws Exception {
        final var oldNode = internalCluster().startDataOnlyNode();
        final var indexName = randomIdentifier();
        createIndexWithContent(
            indexName,
            indexSettings(1, 0).put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name", oldNode).build()
        );

        final var repoName = randomIdentifier();
        createRepository(repoName, "mock");

        final var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var snapshotFuture = startFullSnapshotBlockedOnDataNode("snapshot-1", repoName, oldNode);

        final var snapshotPausedListener = new SubscribableListener<>();
        final ClusterStateListener snapshotPauseListener = event -> {
            final var entriesForRepo = SnapshotsInProgress.get(event.state()).forRepo(repoName);
            if (entriesForRepo.isEmpty()) {
                return;
            }
            assertThat(entriesForRepo, hasSize(1));
            final var shardSnapshotStatuses = entriesForRepo.iterator().next().shards().values();
            assertThat(shardSnapshotStatuses, hasSize(1));
            final var shardState = shardSnapshotStatuses.iterator().next().state();
            if (shardState == SnapshotsInProgress.ShardState.WAITING) {
                snapshotPausedListener.onResponse(null);
            } else {
                assertEquals(SnapshotsInProgress.ShardState.INIT, shardState);
            }
        };
        clusterService.addListener(snapshotPauseListener);
        snapshotPausedListener.addListener(ActionListener.running(() -> clusterService.removeListener(snapshotPauseListener)));

        internalCluster().ensureAtLeastNumDataNodes(2);
        updateIndexSettings(Settings.builder().putNull(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name"), indexName);

        final var oldNodeId = clusterService.state().nodes().resolveNode(oldNode).getId();
        PlainActionFuture.get(fut -> clusterService.submitUnbatchedStateUpdateTask("mark node for removal", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return currentState.copyAndUpdateMetadata(
                    mdb -> mdb.putCustom(
                        NodesShutdownMetadata.TYPE,
                        new NodesShutdownMetadata(
                            Map.of(
                                oldNodeId,
                                SingleNodeShutdownMetadata.builder()
                                    .setNodeId(oldNodeId)
                                    .setType(SingleNodeShutdownMetadata.Type.REMOVE)
                                    .setStartedAtMillis(clusterService.threadPool().absoluteTimeInMillis())
                                    .setReason("test")
                                    .build()
                            )
                        )
                    )
                );
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                fut.onResponse(null);
            }
        }), 10, TimeUnit.SECONDS);

        logger.info("--> unblock");
        unblockAllDataNodes(repoName);
        PlainActionFuture.get(snapshotPausedListener::addListener, 10, TimeUnit.SECONDS);

        // reroute to work around https://github.com/elastic/elasticsearch/issues/101514
        PlainActionFuture.get(
            fut -> clusterService.getRerouteService().reroute("test", Priority.NORMAL, fut.map(ignored -> null)),
            10,
            TimeUnit.SECONDS
        );

        assertEquals(SnapshotState.SUCCESS, snapshotFuture.get(10, TimeUnit.SECONDS).getSnapshotInfo().state());

        PlainActionFuture.get(fut -> clusterService.submitUnbatchedStateUpdateTask("remove restart marker", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return currentState.copyAndUpdateMetadata(mdb -> mdb.putCustom(NodesShutdownMetadata.TYPE, NodesShutdownMetadata.EMPTY));
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                fut.onResponse(null);
            }
        }), 10, TimeUnit.SECONDS);
    }
}
