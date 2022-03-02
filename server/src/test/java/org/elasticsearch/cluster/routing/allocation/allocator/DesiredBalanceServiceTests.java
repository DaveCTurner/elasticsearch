/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_VERSION_CREATED;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.hamcrest.Matchers.equalTo;

public class DesiredBalanceServiceTests extends ESTestCase {

    private static final String TEST_INDEX = "test-index";

    // TODO dry this lot up now that a pattern is emerging

    public void testSimple() {
        final var desiredBalanceService = new DesiredBalanceService(new ShardsAllocator() {
            @Override
            public void allocate(RoutingAllocation allocation) {
                final var unassignedIterator = allocation.routingNodes().unassigned().iterator();
                while (unassignedIterator.hasNext()) {
                    final var shardRouting = unassignedIterator.next();
                    if (shardRouting.primary()) {
                        unassignedIterator.initialize("node-0", null, 0L, allocation.changes());
                    } else if (allocation.routingNodes()
                        .assignedShards(shardRouting.shardId())
                        .stream()
                        .anyMatch(r -> r.primary() && r.started())) {
                            unassignedIterator.initialize("node-1", null, 0L, allocation.changes());
                        }
                }
            }

            @Override
            public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                throw new AssertionError("only used for allocation explain");
            }
        });

        final var discoveryNodes = DiscoveryNodes.builder();
        for (int i = 0; i < 3; i++) {
            final var transportAddress = buildNewFakeTransportAddress();
            final var discoveryNode = new DiscoveryNode(
                "node-" + i,
                "node-" + i,
                UUIDs.randomBase64UUID(random()),
                transportAddress.address().getHostString(),
                transportAddress.getAddress(),
                transportAddress,
                Map.of(),
                Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE),
                Version.CURRENT
            );
            discoveryNodes.add(discoveryNode);
        }
        discoveryNodes.masterNodeId("node-0").localNodeId("node-0");

        final var indexMetadata = IndexMetadata.builder(TEST_INDEX)
            .settings(
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 2)
                    .put(SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
            )
            .build();

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(RoutingTable.builder().addAsNew(indexMetadata))
            .build();

        final var index = clusterState.metadata().index(TEST_INDEX).getIndex();

        assertDesiredAssignments(desiredBalanceService, Map.of());

        assertTrue(
            desiredBalanceService.updateDesiredBalanceAndReroute(
                new DesiredBalanceInput(
                    new RoutingAllocation(
                        new AllocationDeciders(List.of()),
                        clusterState,
                        ClusterInfo.EMPTY,
                        SnapshotShardSizeInfo.EMPTY,
                        0L
                    ),
                    List.of()
                ),
                () -> true
            )
        );

        assertDesiredAssignments(
            desiredBalanceService,
            Map.of(new ShardId(index, 0), Set.of("node-0", "node-1"), new ShardId(index, 1), Set.of("node-0", "node-1"))
        );

        assertFalse(
            desiredBalanceService.updateDesiredBalanceAndReroute(
                new DesiredBalanceInput(
                    new RoutingAllocation(
                        new AllocationDeciders(List.of()),
                        clusterState,
                        ClusterInfo.EMPTY,
                        SnapshotShardSizeInfo.EMPTY,
                        0L
                    ),
                    List.of()
                ),
                () -> true
            )
        );
    }

    public void testStopsComputingWhenStale() {
        final var desiredBalanceService = new DesiredBalanceService(new ShardsAllocator() {
            @Override
            public void allocate(RoutingAllocation allocation) {
                final var unassignedIterator = allocation.routingNodes().unassigned().iterator();
                while (unassignedIterator.hasNext()) {
                    final var shardRouting = unassignedIterator.next();
                    if (shardRouting.primary()) {
                        unassignedIterator.initialize("node-0", null, 0L, allocation.changes());
                    } else if (allocation.routingNodes()
                        .assignedShards(shardRouting.shardId())
                        .stream()
                        .anyMatch(r -> r.primary() && r.started())) {
                            unassignedIterator.initialize("node-1", null, 0L, allocation.changes());
                        }
                }
            }

            @Override
            public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                throw new AssertionError("only used for allocation explain");
            }
        });

        final var discoveryNodes = DiscoveryNodes.builder();
        for (int i = 0; i < 3; i++) {
            final var transportAddress = buildNewFakeTransportAddress();
            final var discoveryNode = new DiscoveryNode(
                "node-" + i,
                "node-" + i,
                UUIDs.randomBase64UUID(random()),
                transportAddress.address().getHostString(),
                transportAddress.getAddress(),
                transportAddress,
                Map.of(),
                Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE),
                Version.CURRENT
            );
            discoveryNodes.add(discoveryNode);
        }
        discoveryNodes.masterNodeId("node-0").localNodeId("node-0");

        final var indexMetadata = IndexMetadata.builder(TEST_INDEX)
            .settings(
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 2)
                    .put(SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
            )
            .build();

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(RoutingTable.builder().addAsNew(indexMetadata))
            .build();

        final var index = clusterState.metadata().index(TEST_INDEX).getIndex();

        assertDesiredAssignments(desiredBalanceService, Map.of());

        assertTrue(
            desiredBalanceService.updateDesiredBalanceAndReroute(
                new DesiredBalanceInput(
                    new RoutingAllocation(
                        new AllocationDeciders(List.of()),
                        clusterState,
                        ClusterInfo.EMPTY,
                        SnapshotShardSizeInfo.EMPTY,
                        0L
                    ),
                    List.of()
                ),
                () -> false // bail out after one iteration
            )
        );

        assertDesiredAssignments(
            desiredBalanceService,
            Map.of(new ShardId(index, 0), Set.of("node-0"), new ShardId(index, 1), Set.of("node-0"))
        );

        assertTrue(
            desiredBalanceService.updateDesiredBalanceAndReroute(
                new DesiredBalanceInput(
                    new RoutingAllocation(
                        new AllocationDeciders(List.of()),
                        clusterState,
                        ClusterInfo.EMPTY,
                        SnapshotShardSizeInfo.EMPTY,
                        0L
                    ),
                    List.of()
                ),
                () -> true
            )
        );

        assertDesiredAssignments(
            desiredBalanceService,
            Map.of(new ShardId(index, 0), Set.of("node-0", "node-1"), new ShardId(index, 1), Set.of("node-0", "node-1"))
        );

    }

    public void testIgnoresOutOfScopeShards() {
        final var desiredBalanceService = new DesiredBalanceService(new ShardsAllocator() {
            @Override
            public void allocate(RoutingAllocation allocation) {
                final var unassignedIterator = allocation.routingNodes().unassigned().iterator();
                while (unassignedIterator.hasNext()) {
                    final var shardRouting = unassignedIterator.next();
                    if (shardRouting.primary()) {
                        unassignedIterator.initialize("node-0", null, 0L, allocation.changes());
                    } else if (allocation.routingNodes()
                        .assignedShards(shardRouting.shardId())
                        .stream()
                        .anyMatch(r -> r.primary() && r.started())) {
                            unassignedIterator.initialize("node-1", null, 0L, allocation.changes());
                        }
                }
            }

            @Override
            public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                throw new AssertionError("only used for allocation explain");
            }
        });

        final var discoveryNodes = DiscoveryNodes.builder();
        for (int i = 0; i < 3; i++) {
            final var transportAddress = buildNewFakeTransportAddress();
            final var discoveryNode = new DiscoveryNode(
                "node-" + i,
                "node-" + i,
                UUIDs.randomBase64UUID(random()),
                transportAddress.address().getHostString(),
                transportAddress.getAddress(),
                transportAddress,
                Map.of(),
                Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE),
                Version.CURRENT
            );
            discoveryNodes.add(discoveryNode);
        }
        discoveryNodes.masterNodeId("node-0").localNodeId("node-0");

        final var indexMetadata = IndexMetadata.builder(TEST_INDEX)
            .settings(
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 2)
                    .put(SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
            )
            .build();

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(RoutingTable.builder().addAsNew(indexMetadata))
            .build();

        final var index = clusterState.metadata().index(TEST_INDEX).getIndex();

        assertDesiredAssignments(desiredBalanceService, Map.of());

        assertTrue(
            desiredBalanceService.updateDesiredBalanceAndReroute(
                new DesiredBalanceInput(
                    new RoutingAllocation(
                        new AllocationDeciders(List.of()),
                        clusterState,
                        ClusterInfo.EMPTY,
                        SnapshotShardSizeInfo.EMPTY,
                        0L
                    ),
                    List.of(clusterState.routingTable().shardRoutingTable(new ShardId(index, 0)).primaryShard())
                ),
                () -> true
            )
        );

        assertDesiredAssignments(desiredBalanceService, Map.of(new ShardId(index, 1), Set.of("node-0", "node-1")));
    }

    public void testRespectsAssignmentOfUnknownPrimaries() {
        final var desiredBalanceService = new DesiredBalanceService(new ShardsAllocator() {
            @Override
            public void allocate(RoutingAllocation allocation) {
                final var unassignedIterator = allocation.routingNodes().unassigned().iterator();
                while (unassignedIterator.hasNext()) {
                    final var shardRouting = unassignedIterator.next();
                    if (shardRouting.primary()) {
                        unassignedIterator.initialize("node-0", null, 0L, allocation.changes());
                    } else if (allocation.routingNodes()
                        .assignedShards(shardRouting.shardId())
                        .stream()
                        .anyMatch(r -> r.primary() && r.started())) {
                            unassignedIterator.initialize("node-1", null, 0L, allocation.changes());
                        }
                }
            }

            @Override
            public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                throw new AssertionError("only used for allocation explain");
            }
        });

        final var discoveryNodes = DiscoveryNodes.builder();
        for (int i = 0; i < 3; i++) {
            final var transportAddress = buildNewFakeTransportAddress();
            final var discoveryNode = new DiscoveryNode(
                "node-" + i,
                "node-" + i,
                UUIDs.randomBase64UUID(random()),
                transportAddress.address().getHostString(),
                transportAddress.getAddress(),
                transportAddress,
                Map.of(),
                Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE),
                Version.CURRENT
            );
            discoveryNodes.add(discoveryNode);
        }
        discoveryNodes.masterNodeId("node-0").localNodeId("node-0");

        final var indexMetadata = IndexMetadata.builder(TEST_INDEX)
            .settings(
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 2)
                    .put(SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
            )
            .build();

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(RoutingTable.builder().addAsNew(indexMetadata))
            .build();

        final var index = clusterState.metadata().index(TEST_INDEX).getIndex();

        assertDesiredAssignments(desiredBalanceService, Map.of());

        final var changes = new RoutingChangesObserver.DelegatingRoutingChangesObserver();
        final var routingNodes = clusterState.mutableRoutingNodes();
        for (final var iterator = routingNodes.unassigned().iterator(); iterator.hasNext();) {
            final var shardRouting = iterator.next();
            if (shardRouting.shardId().id() == 0 && shardRouting.primary()) {
                switch (between(1, 3)) {
                    case 1 -> iterator.initialize("node-2", null, 0L, changes);
                    case 2 -> routingNodes.startShard(logger, iterator.initialize("node-2", null, 0L, changes), changes);
                    case 3 -> routingNodes.relocateShard(
                        routingNodes.startShard(logger, iterator.initialize("node-1", null, 0L, changes), changes),
                        "node-2",
                        0L,
                        changes
                    );
                }
                break;
            }
        }

        clusterState = ClusterState.builder(clusterState)
            .routingTable(new RoutingTable.Builder().updateNodes(clusterState.routingTable().version(), routingNodes))
            .build();

        assertTrue(
            desiredBalanceService.updateDesiredBalanceAndReroute(
                new DesiredBalanceInput(
                    new RoutingAllocation(
                        new AllocationDeciders(List.of()),
                        clusterState,
                        ClusterInfo.EMPTY,
                        SnapshotShardSizeInfo.EMPTY,
                        0L
                    ),
                    List.of()
                ),
                () -> true
            )
        );

        assertDesiredAssignments(
            desiredBalanceService,
            Map.of(new ShardId(index, 0), Set.of("node-2", "node-1"), new ShardId(index, 1), Set.of("node-0", "node-1"))
        );

    }

    public void testRespectsAssignmentOfUnknownReplicas() {
        final var desiredBalanceService = new DesiredBalanceService(new ShardsAllocator() {
            @Override
            public void allocate(RoutingAllocation allocation) {
                final var unassignedIterator = allocation.routingNodes().unassigned().iterator();
                while (unassignedIterator.hasNext()) {
                    final var shardRouting = unassignedIterator.next();
                    if (shardRouting.primary()) {
                        unassignedIterator.initialize("node-0", null, 0L, allocation.changes());
                    } else if (allocation.routingNodes()
                        .assignedShards(shardRouting.shardId())
                        .stream()
                        .anyMatch(r -> r.primary() && r.started())) {
                            unassignedIterator.initialize("node-1", null, 0L, allocation.changes());
                        }
                }
            }

            @Override
            public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                throw new AssertionError("only used for allocation explain");
            }
        });

        final var discoveryNodes = DiscoveryNodes.builder();
        for (int i = 0; i < 3; i++) {
            final var transportAddress = buildNewFakeTransportAddress();
            final var discoveryNode = new DiscoveryNode(
                "node-" + i,
                "node-" + i,
                UUIDs.randomBase64UUID(random()),
                transportAddress.address().getHostString(),
                transportAddress.getAddress(),
                transportAddress,
                Map.of(),
                Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE),
                Version.CURRENT
            );
            discoveryNodes.add(discoveryNode);
        }
        discoveryNodes.masterNodeId("node-0").localNodeId("node-0");

        final var indexMetadata = IndexMetadata.builder(TEST_INDEX)
            .settings(
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 2)
                    .put(SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
            )
            .build();

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(RoutingTable.builder().addAsNew(indexMetadata))
            .build();

        final var index = clusterState.metadata().index(TEST_INDEX).getIndex();

        assertDesiredAssignments(desiredBalanceService, Map.of());

        final var changes = new RoutingChangesObserver.DelegatingRoutingChangesObserver();
        final var routingNodes = clusterState.mutableRoutingNodes();
        for (final var iterator = routingNodes.unassigned().iterator(); iterator.hasNext();) {
            final var shardRouting = iterator.next();
            if (shardRouting.shardId().id() == 0 && shardRouting.primary()) {
                routingNodes.startShard(logger, iterator.initialize("node-2", null, 0L, changes), changes);
                break;
            }
        }

        for (final var iterator = routingNodes.unassigned().iterator(); iterator.hasNext();) {
            final var shardRouting = iterator.next();
            if (shardRouting.shardId().id() == 0) {
                assert shardRouting.primary() == false;
                switch (between(1, 3)) {
                    case 1 -> iterator.initialize("node-0", null, 0L, changes);
                    case 2 -> routingNodes.startShard(logger, iterator.initialize("node-0", null, 0L, changes), changes);
                    case 3 -> routingNodes.relocateShard(
                        routingNodes.startShard(logger, iterator.initialize("node-1", null, 0L, changes), changes),
                        "node-0",
                        0L,
                        changes
                    );
                }
                break;
            }
        }

        clusterState = ClusterState.builder(clusterState)
            .routingTable(new RoutingTable.Builder().updateNodes(clusterState.routingTable().version(), routingNodes))
            .build();

        assertTrue(
            desiredBalanceService.updateDesiredBalanceAndReroute(
                new DesiredBalanceInput(
                    new RoutingAllocation(
                        new AllocationDeciders(List.of()),
                        clusterState,
                        ClusterInfo.EMPTY,
                        SnapshotShardSizeInfo.EMPTY,
                        0L
                    ),
                    List.of()
                ),
                () -> true
            )
        );

        assertDesiredAssignments(
            desiredBalanceService,
            Map.of(new ShardId(index, 0), Set.of("node-2", "node-0"), new ShardId(index, 1), Set.of("node-0", "node-1"))
        );

    }

    public void testSimulatesAchievingDesiredBalanceBeforeDelegating() {

        final var desiredBalanceService = new DesiredBalanceService(new ShardsAllocator() {
            @Override
            public void allocate(RoutingAllocation allocation) {
                for (final var routingNode : allocation.routingNodes()) {
                    assertThat(
                        allocation.routingNodes().toString(),
                        routingNode.numberOfOwningShards(),
                        equalTo(routingNode.nodeId().equals("node-2") ? 0 : 2)
                    );
                }
            }

            @Override
            public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                throw new AssertionError("only used for allocation explain");
            }
        });

        final var discoveryNodes = DiscoveryNodes.builder();
        for (int i = 0; i < 3; i++) {
            final var transportAddress = buildNewFakeTransportAddress();
            final var discoveryNode = new DiscoveryNode(
                "node-" + i,
                "node-" + i,
                UUIDs.randomBase64UUID(random()),
                transportAddress.address().getHostString(),
                transportAddress.getAddress(),
                transportAddress,
                Map.of(),
                Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE),
                Version.CURRENT
            );
            discoveryNodes.add(discoveryNode);
        }
        discoveryNodes.masterNodeId("node-0").localNodeId("node-0");

        final var indexMetadata = IndexMetadata.builder(TEST_INDEX)
            .settings(
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 2)
                    .put(SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
            )
            .build();

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(RoutingTable.builder().addAsNew(indexMetadata))
            .build();

        final var index = clusterState.metadata().index(TEST_INDEX).getIndex();

        assertDesiredAssignments(desiredBalanceService, Map.of());

        final var changes = new RoutingChangesObserver.DelegatingRoutingChangesObserver();
        final var desiredRoutingNodes = clusterState.mutableRoutingNodes();
        for (final var iterator = desiredRoutingNodes.unassigned().iterator(); iterator.hasNext();) {
            final var shardRouting = iterator.next();
            desiredRoutingNodes.startShard(
                logger,
                iterator.initialize(shardRouting.primary() ? "node-0" : "node-1", null, 0L, changes),
                changes
            );
        }

        final var desiredClusterState = ClusterState.builder(clusterState)
            .routingTable(new RoutingTable.Builder().updateNodes(clusterState.routingTable().version(), desiredRoutingNodes))
            .build();

        assertTrue(
            desiredBalanceService.updateDesiredBalanceAndReroute(
                new DesiredBalanceInput(
                    new RoutingAllocation(
                        new AllocationDeciders(List.of()),
                        desiredClusterState,
                        ClusterInfo.EMPTY,
                        SnapshotShardSizeInfo.EMPTY,
                        0L
                    ),
                    List.of()
                ),
                () -> true
            )
        );

        assertDesiredAssignments(
            desiredBalanceService,
            Map.of(new ShardId(index, 0), Set.of("node-0", "node-1"), new ShardId(index, 1), Set.of("node-0", "node-1"))
        );

        final var randomRoutingNodes = clusterState.mutableRoutingNodes();
        for (int shard = 0; shard < 2; shard++) {
            final var primaryRoutingState = randomFrom(ShardRoutingState.values());
            final var replicaRoutingState = switch (primaryRoutingState) {
                case UNASSIGNED, INITIALIZING -> ShardRoutingState.UNASSIGNED;
                case STARTED -> randomFrom(ShardRoutingState.values());
                case RELOCATING -> randomValueOtherThan(ShardRoutingState.RELOCATING, () -> randomFrom(ShardRoutingState.values()));
            };
            final var nodes = new ArrayList<>(List.of("node-0", "node-1", "node-2"));
            Randomness.shuffle(nodes);

            if (primaryRoutingState == ShardRoutingState.UNASSIGNED) {
                continue;
            }
            for (final var iterator = randomRoutingNodes.unassigned().iterator(); iterator.hasNext();) {
                final var shardRouting = iterator.next();
                if (shardRouting.shardId().getId() == shard && shardRouting.primary()) {
                    switch (primaryRoutingState) {
                        case INITIALIZING -> iterator.initialize(nodes.remove(0), null, 0L, changes);
                        case STARTED -> randomRoutingNodes.startShard(
                            logger,
                            iterator.initialize(nodes.remove(0), null, 0L, changes),
                            changes
                        );
                        case RELOCATING -> randomRoutingNodes.relocateShard(
                            randomRoutingNodes.startShard(logger, iterator.initialize(nodes.remove(0), null, 0L, changes), changes),
                            nodes.remove(0),
                            0L,
                            changes
                        );
                    }
                    break;
                }
            }

            if (replicaRoutingState == ShardRoutingState.UNASSIGNED) {
                continue;
            }
            for (final var iterator = randomRoutingNodes.unassigned().iterator(); iterator.hasNext();) {
                final var shardRouting = iterator.next();
                if (shardRouting.shardId().getId() == shard && shardRouting.primary() == false) {
                    switch (replicaRoutingState) {
                        case INITIALIZING -> iterator.initialize(nodes.remove(0), null, 0L, changes);
                        case STARTED -> randomRoutingNodes.startShard(
                            logger,
                            iterator.initialize(nodes.remove(0), null, 0L, changes),
                            changes
                        );
                        case RELOCATING -> randomRoutingNodes.relocateShard(
                            randomRoutingNodes.startShard(logger, iterator.initialize(nodes.remove(0), null, 0L, changes), changes),
                            nodes.remove(0),
                            0L,
                            changes
                        );
                    }
                    break;
                }
            }
        }

        final var randomClusterState = ClusterState.builder(clusterState)
            .routingTable(new RoutingTable.Builder().updateNodes(clusterState.routingTable().version(), randomRoutingNodes))
            .build();

        assertFalse(
            desiredBalanceService.updateDesiredBalanceAndReroute(
                new DesiredBalanceInput(
                    new RoutingAllocation(
                        new AllocationDeciders(List.of()),
                        randomClusterState,
                        ClusterInfo.EMPTY,
                        SnapshotShardSizeInfo.EMPTY,
                        0L
                    ),
                    List.of()
                ),
                () -> true
            )
        );

        assertDesiredAssignments(
            desiredBalanceService,
            Map.of(new ShardId(index, 0), Set.of("node-0", "node-1"), new ShardId(index, 1), Set.of("node-0", "node-1"))
        );

    }

    private static void assertDesiredAssignments(DesiredBalanceService desiredBalanceService, Map<ShardId, Set<String>> expected) {
        assertThat(
            desiredBalanceService.getCurrentDesiredBalance()
                .desiredAssignments()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e ->  Set.copyOf(e.getValue()))),
            equalTo(expected)
        );
    }
}
