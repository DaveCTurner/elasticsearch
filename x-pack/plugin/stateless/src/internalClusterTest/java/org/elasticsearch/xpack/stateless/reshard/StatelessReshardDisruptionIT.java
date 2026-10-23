/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.PendingClusterTask;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.stateless.reshard.SplitSourceService.RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests that resharding operations are resilient in presence of failures.
 */
public class StatelessReshardDisruptionIT extends StatelessReshardDisruptionBaseIT {
    public void testReshardWithDisruption() throws InterruptedException, ExecutionException {
        var masterNode = startMasterOnlyNode();

        String dedicatedCoordinatorNode = startSearchNode();
        // Exclude coordinator from allocation.
        client().admin()
            .cluster()
            .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .setPersistentSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", dedicatedCoordinatorNode))
            .get();

        int shards = randomIntBetween(1, 5);

        // At least two of each role so we still have usable nodes when `ISOLATE_NODE` disruption is applied.
        int indexNodes = randomIntBetween(2, shards * 2);
        startIndexNodes(indexNodes);
        int searchNodes = randomIntBetween(2, shards * 2);
        startSearchNodes(searchNodes);

        int clusterSize = 1 + 1 + indexNodes + searchNodes;
        ensureStableCluster(clusterSize);

        final String indexName = randomIndexName();
        createIndex(indexName, indexSettings(shards, 1).build());
        ensureGreen(indexName);
        Index index = resolveIndex(indexName);

        checkNumberOfShardsSetting(indexName, shards);

        var multiple = 2;

        final int numDocs = randomIntBetween(10, 100);
        indexDocs(indexName, numDocs);

        try (var disruptionExecutorService = Executors.newSingleThreadExecutor()) {
            logger.info("--> start inducing failures");

            var failuresStarted = new CountDownLatch(1);
            var stop = new AtomicBoolean(false);
            var disruptionFuture = disruptionExecutorService.submit(() -> {
                failuresStarted.countDown();
                do {
                    Failure randomFailure = randomFrom(Failure.values());
                    try {
                        induceFailure(randomFailure, index, dedicatedCoordinatorNode);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } while (stop.get() == false);
            });

            failuresStarted.await();

            try {
                logger.info("--> resharding an index [{}] under disruption", indexName);
                client(masterNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet(TEST_REQUEST_TIMEOUT);
                waitForReshardCompletion(index);
                logger.info("--> done resharding an index [{}]", indexName);
            } finally {
                stop.set(true);
                disruptionFuture.get();
            }

            checkNumberOfShardsSetting(indexName, shards * multiple);

            ensureGreen(indexName);
        }
    }

    @Override
    protected boolean addMockFsRepository() {
        // Use FS repository because it supports blob copy
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), AddSettingPlugin.class);
    }

    /** Registers the test-only setting for retrying split requests to a source shard. */
    public static class AddSettingPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(SplitTargetService.START_SPLIT_RETRY_TIMEOUT);
        }
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings()
            // These tests are not performing writes and do not need the grace period.
            .put(RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD.getKey(), TimeValue.ZERO)
            // If the source moves, retry recovery with its new location before the test's cluster health wait times out.
            .put(SplitTargetService.START_SPLIT_RETRY_TIMEOUT.getKey(), TimeValue.timeValueSeconds(5));
    }

    private static void checkNumberOfShardsSetting(String indexName, int expected_shards) {
        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(
                client().admin()
                    .indices()
                    .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
                    .execute()
                    .actionGet()
                    .getIndexToSettings()
                    .get(indexName)
            ),
            equalTo(expected_shards)
        );
    }

    private void waitForReshardCompletion(Index index) {
        try {
            awaitClusterState(state -> state.metadata().findIndex(index).get().getReshardingMetadata() == null);
        } catch (AssertionError e) {
            try {
                logReshardStillInProgress(index);
            } catch (Exception logFailure) {
                logger.info("failed to log reshard timeout state for [{}]", index, logFailure);
            }
            throw e;
        }
    }

    private void logReshardStillInProgress(Index index) {
        String masterName = internalCluster().getMasterName();
        ClusterService clusterService = internalCluster().clusterService(masterName);
        ClusterState state = clusterService.state();
        List<PendingClusterTask> pendingTasks = clusterService.getMasterService().pendingTasks();
        logger.info(
            "reshard still in progress for [{}] after [{}], master [{}], version [{}], pending tasks {}",
            index,
            TimeValue.THIRTY_SECONDS,
            masterName,
            state.version(),
            pendingTasks.stream()
                .map(task -> task.getSource() + " queued=" + task.getTimeInQueue() + (task.isExecuting() ? " executing" : ""))
                .toList()
        );

        ProjectMetadata project = state.metadata().lookupProject(index).orElse(null);
        IndexMetadata indexMetadata = project == null ? null : project.index(index);
        if (indexMetadata == null || indexMetadata.getReshardingMetadata() == null) {
            logger.info("resharding metadata for [{}] is absent", index);
            return;
        }
        var split = indexMetadata.getReshardingMetadata().getSplit();
        IndexRoutingTable routingTable = state.routingTable(project.id()).index(index.getName());
        for (int shardId = 0; shardId < split.shardCountAfter(); shardId++) {
            String roleState = split.isSourceShard(shardId)
                ? "source " + split.getSourceShardState(shardId)
                : "target " + split.getTargetShardState(shardId);
            logger.info(
                "reshard [{}][{}] {} primary [{}] search [{}]",
                index.getName(),
                shardId,
                roleState,
                describeCopies(state, routingTable, shardId, true),
                describeCopies(state, routingTable, shardId, false)
            );
        }
    }

    private static String describeCopies(ClusterState state, IndexRoutingTable routingTable, int shardId, boolean primary) {
        if (routingTable == null) {
            return "no routing table";
        }
        IndexShardRoutingTable shard = routingTable.shard(shardId);
        if (shard == null) {
            return "missing";
        }
        if (primary) {
            return describeShardRouting(state, shard.primaryShard());
        }
        List<ShardRouting> copies = shard.unpromotableShards();
        if (copies.isEmpty()) {
            return "none";
        }
        return copies.stream().map(copy -> describeShardRouting(state, copy)).collect(Collectors.joining(", "));
    }

    private static String describeShardRouting(ClusterState state, ShardRouting routing) {
        if (routing == null) {
            return "missing";
        }
        StringBuilder description = new StringBuilder().append(routing.state());
        if (routing.currentNodeId() != null) {
            description.append(" on ").append(nodeName(state, routing.currentNodeId()));
        }
        if (routing.relocating()) {
            description.append(" relocating to ").append(nodeName(state, routing.relocatingNodeId()));
        }
        return description.toString();
    }

    private static String nodeName(ClusterState state, String nodeId) {
        if (nodeId == null) {
            return "null";
        }
        DiscoveryNode node = state.nodes().get(nodeId);
        return node == null ? nodeId : node.getName();
    }
}
