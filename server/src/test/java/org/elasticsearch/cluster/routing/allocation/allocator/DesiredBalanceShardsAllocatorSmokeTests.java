/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_VERSION_CREATED;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;

public class DesiredBalanceShardsAllocatorSmokeTests extends ESTestCase {

    public void testSimpleCase() {

        final TestHarness testHarness = new TestHarness();
        testHarness.addNode();
        testHarness.addIndex("test", 1, 0);

        testHarness.runAllocator();
        testHarness.runAsyncTasksExpectingReroute();

        assertFalse(testHarness.clusterState.routingTable().shardRoutingTable("test", 0).primaryShard().assignedToNode());

        testHarness.runAllocator();
        testHarness.runAsyncTasksExpectingNoReroute();

        assertTrue(testHarness.clusterState.routingTable().shardRoutingTable("test", 0).primaryShard().assignedToNode());

        testHarness.startInitializingShards();

        testHarness.assertDesiredBalanceAchieved();
    }

    private static class TestHarness {
        private final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
        private final ThreadPool threadPool = deterministicTaskQueue.getThreadPool();
        private final DesiredBalanceShardsAllocator desiredBalanceShardsAllocator = new DesiredBalanceShardsAllocator(
            new ShardsAllocator() {
                @Override
                public void allocate(RoutingAllocation allocation) {
                    final var dataNodeId = allocation.nodes().getDataNodes().valuesIt().next().getId();
                    final var unassignedIterator = allocation.routingNodes().unassigned().iterator();
                    while (unassignedIterator.hasNext()) {
                        unassignedIterator.next();
                        unassignedIterator.initialize(dataNodeId, null, 0L, allocation.changes());
                    }
                }

                @Override
                public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                    throw new AssertionError("only used for allocation explain");
                }
            },
            threadPool,
            () -> this::reroute
        );
        private final AllocationService allocationService;

        ClusterState clusterState;
        int nextNodeId;

        boolean expectReroute;

        TestHarness() {
            final DiscoveryNode masterNode = newDiscoveryNode();
            clusterState = ClusterState.builder(ClusterName.DEFAULT)
                .nodes(DiscoveryNodes.builder().add(masterNode).localNodeId(masterNode.getId()).masterNodeId(masterNode.getId()))
                .build();

            allocationService = new AllocationService(

                new AllocationDeciders(List.of(new AllocationDecider() {

                })),
                new GatewayAllocator() {

                    @Override
                    public void beforeAllocation(RoutingAllocation allocation) {}

                    @Override
                    public void allocateUnassigned(
                        ShardRouting shardRouting,
                        RoutingAllocation allocation,
                        UnassignedAllocationHandler unassignedAllocationHandler
                    ) {}

                    @Override
                    public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {}
                },
                desiredBalanceShardsAllocator,
                () -> ClusterInfo.EMPTY,
                () -> SnapshotShardSizeInfo.EMPTY
            );

        }

        private DiscoveryNode newDiscoveryNode() {
            return newDiscoveryNode(Set.of(DiscoveryNodeRole.MASTER_ROLE));
        }

        private DiscoveryNode newDataNode() {
            return newDiscoveryNode(Set.of(DiscoveryNodeRole.DATA_ROLE));
        }

        private String nextNodeId() {
            return "node-" + nextNodeId++;
        }

        private DiscoveryNode newDiscoveryNode(Set<DiscoveryNodeRole> masterRole) {
            final var transportAddress = buildNewFakeTransportAddress();
            final var nodeId = nextNodeId();
            return new DiscoveryNode(
                nodeId,
                nodeId,
                UUIDs.randomBase64UUID(random()),
                transportAddress.address().getHostString(),
                transportAddress.getAddress(),
                transportAddress,
                Map.of(),
                masterRole,
                Version.CURRENT
            );
        }

        private void reroute(String reason, Priority priority, ActionListener<ClusterState> listener) {
            assertTrue("unexpected reroute", expectReroute);
            expectReroute = false;
        }

        void addNode() {
            clusterState = ClusterState.builder(clusterState)
                .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newDataNode()))
                .build();
        }

        void addIndex(String indexName, int numberOfShards, int numberOfReplicas) {

            final var indexMetadata = IndexMetadata.builder(indexName)
                .settings(
                    Settings.builder()
                        .put(SETTING_NUMBER_OF_SHARDS, numberOfShards)
                        .put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
                        .put(SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
                )
                .build();

            clusterState = ClusterState.builder(clusterState)
                .metadata(Metadata.builder(clusterState.metadata()).put(indexMetadata, true))
                .routingTable(RoutingTable.builder(clusterState.routingTable()).addAsNew(indexMetadata))
                .build();
        }

        void runAllocator() {
            clusterState = allocationService.reroute(clusterState, "test");
        }

        void runAsyncTasksExpectingReroute() {
            assertFalse("already expecting a reroute", expectReroute);
            expectReroute = true;
            deterministicTaskQueue.runAllTasks();
            assertFalse("no reroute occurred", expectReroute);
        }

        void runAsyncTasksExpectingNoReroute() {
            assertFalse("already expecting a reroute", expectReroute);
            deterministicTaskQueue.runAllTasks();
            assertFalse("expectReroute was set by async task?!", expectReroute);
        }

        void startInitializingShards() {
            final List<ShardRouting> initializingShards = new ArrayList<>();
            for (final var indexRoutingTable : clusterState.routingTable()) {
                for (final var indexShardRoutingTable : indexRoutingTable) {
                    initializingShards.addAll(indexShardRoutingTable.getAllInitializingShards());
                }
            }
            clusterState = allocationService.applyStartedShards(clusterState, initializingShards);
        }

        void assertDesiredBalanceAchieved() {
            for (final var indexRoutingTable : clusterState.routingTable()) {
                for (final var indexShardRoutingTable : indexRoutingTable) {
                    final var shardId = indexShardRoutingTable.shardId();
                    final var desiredNodes = desiredBalanceShardsAllocator.getCurrentDesiredBalance().getDesiredNodeIds(shardId);
                    assertNotNull(shardId + " should have a desired balance entry", desiredNodes);
                    for (final ShardRouting shardRouting : indexShardRoutingTable) {
                        assertTrue(shardRouting + " should be STARTED", shardRouting.started());
                        assertTrue(
                            shardRouting + " should be on nodes " + desiredNodes,
                            desiredNodes.contains(shardRouting.currentNodeId())
                        );
                    }
                }
            }
        }
    }

}
