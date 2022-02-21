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
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.indices.cluster.ClusterStateChanges;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;

public class DesiredBalanceShardsAllocatorTests extends ESTestCase {

    public void testSimpleCase() {

        final TestHarness testHarness = new TestHarness(xContentRegistry());
        testHarness.addNode();
        testHarness.addIndex("test", 1, 0);

        testHarness.runAllocator();
        testHarness.runAsyncTasksExpectingReroute();

        testHarness.runAllocator();
        testHarness.runAsyncTasksExpectingNoReroute();

        logger.info("--> {}", testHarness.clusterState);

    }

    private static class TestHarness {
        private final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
        private final ThreadPool threadPool = deterministicTaskQueue.getThreadPool();
        private final ClusterStateChanges clusterStateChanges;
        private final DesiredBalanceShardsAllocator desiredBalanceShardsAllocator = new DesiredBalanceShardsAllocator(
            new ShardsAllocator() {
                @Override
                public void allocate(RoutingAllocation allocation) {

                }

                @Override
                public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                    throw new AssertionError("only used for allocation explain");
                }
            },
            threadPool,
            () -> this::reroute
        );

        ClusterState clusterState;

        boolean expectReroute;

        TestHarness(NamedXContentRegistry xContentRegistry) {
            clusterStateChanges = new ClusterStateChanges(xContentRegistry, threadPool);

            final DiscoveryNode masterNode = newDiscoveryNode();
            clusterState = ClusterState.builder(ClusterName.DEFAULT)
                .nodes(DiscoveryNodes.builder().add(masterNode).localNodeId(masterNode.getId()).masterNodeId(masterNode.getId()))
                .build();
        }

        private DiscoveryNode newDiscoveryNode() {
            return newDiscoveryNode(Set.of(DiscoveryNodeRole.MASTER_ROLE));
        }

        private DiscoveryNode newDataNode() {
            return newDiscoveryNode(Set.of(DiscoveryNodeRole.DATA_ROLE));
        }

        private DiscoveryNode newDiscoveryNode(Set<DiscoveryNodeRole> masterRole) {
            final var transportAddress = buildNewFakeTransportAddress();
            return new DiscoveryNode(
                randomAlphaOfLength(10),
                UUIDs.randomBase64UUID(random()),
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
            clusterState = clusterStateChanges.addNode(clusterState, newDataNode());
        }

        void addIndex(String indexName, int numberOfShards, int numberOfReplicas) {
            clusterState = clusterStateChanges.createIndex(clusterState, new CreateIndexRequest(
                indexName,
                Settings.builder().put(SETTING_NUMBER_OF_SHARDS, numberOfShards).put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas).build()
            ));
        }

        void runAllocator() {

            final var allocationDeciders = new AllocationDeciders(List.of(new AllocationDecider() {

            }));

            final var routingAllocation = new RoutingAllocation(
                allocationDeciders,
                clusterState.mutableRoutingNodes(),
                clusterState,
                ClusterInfo.EMPTY,
                SnapshotShardSizeInfo.EMPTY,
                0L
            );

            desiredBalanceShardsAllocator.allocate(routingAllocation);

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
    }

}
