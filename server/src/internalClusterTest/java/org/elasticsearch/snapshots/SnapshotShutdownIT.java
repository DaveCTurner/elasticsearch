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
        internalCluster().ensureAtLeastNumDataNodes(1);
        final var originalNode = internalCluster().startDataOnlyNode();
        final var indexName = randomIdentifier();
        createIndexWithContent(
            indexName,
            indexSettings(1, 0).put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name", originalNode).build()
        );

        final var repoName = randomIdentifier();
        createRepository(repoName, "mock");

        final var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var snapshotFuture = startFullSnapshotBlockedOnDataNode("snapshot-1", repoName, originalNode);
        final var snapshotPausedListener = createSnapshotPausedListener(clusterService, repoName);

        updateIndexSettings(Settings.builder().putNull(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name"), indexName);
        putShutdownMetadata(originalNode, clusterService);
        unblockAllDataNodes(repoName); // lets the shard snapshot abort, which frees up the shard so it can move
        PlainActionFuture.get(snapshotPausedListener::addListener, 10, TimeUnit.SECONDS);

        // reroute to work around https://github.com/elastic/elasticsearch/issues/101514
        PlainActionFuture.get(
            fut -> clusterService.getRerouteService().reroute("test", Priority.NORMAL, fut.map(ignored -> null)),
            10,
            TimeUnit.SECONDS
        );

        // snapshot completes when the node vacates even though it hasn't been removed yet
        assertEquals(SnapshotState.SUCCESS, snapshotFuture.get(10, TimeUnit.SECONDS).getSnapshotInfo().state());

        if (randomBoolean()) {
            internalCluster().stopNode(originalNode);
        }

        clearShutdownMetadata(clusterService);
    }

    public void testStartRemoveNodeButDoNotComplete() throws Exception {
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
        final var snapshotPausedListener = createSnapshotPausedListener(clusterService, repoName);

        putShutdownMetadata(oldNode, clusterService);
        unblockAllDataNodes(repoName); // lets the shard snapshot abort, but allocation filtering stops it from moving
        PlainActionFuture.get(snapshotPausedListener::addListener, 10, TimeUnit.SECONDS);
        assertFalse(snapshotFuture.isDone());

        // give up on the node shutdown so the shard snapshot can restart
        clearShutdownMetadata(clusterService);

        assertEquals(SnapshotState.SUCCESS, snapshotFuture.get(10, TimeUnit.SECONDS).getSnapshotInfo().state());
    }

    private static SubscribableListener<Object> createSnapshotPausedListener(ClusterService clusterService, String repoName) {
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
        return snapshotPausedListener;
    }

    private static void putShutdownMetadata(String nodeName, ClusterService clusterService) {
        final var nodeId = clusterService.state().nodes().resolveNode(nodeName).getId();
        PlainActionFuture.get(fut -> clusterService.submitUnbatchedStateUpdateTask("mark node for removal", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return currentState.copyAndUpdateMetadata(
                    mdb -> mdb.putCustom(
                        NodesShutdownMetadata.TYPE,
                        new NodesShutdownMetadata(
                            Map.of(
                                nodeId,
                                SingleNodeShutdownMetadata.builder()
                                    .setNodeId(nodeId)
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
    }

    private static void clearShutdownMetadata(ClusterService clusterService) {
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
