/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.gateway.PriorityComparator;
import org.elasticsearch.index.shard.ShardId;

import java.util.Comparator;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Given the current allocation of shards and the desired balance, performs the next (legal) shard movements towards the goal.
 */
public class DesiredBalanceReconciler {

    private static final Logger logger = LogManager.getLogger(DesiredBalanceReconciler.class);

    private final DesiredBalance desiredBalance;
    private final RoutingAllocation allocation; // name chosen to align with code in BalancedShardsAllocator but TODO rename
    private final RoutingNodes routingNodes;

    DesiredBalanceReconciler(DesiredBalance desiredBalance, RoutingAllocation routingAllocation) {
        this.desiredBalance = desiredBalance;
        this.allocation = routingAllocation;
        this.routingNodes = routingAllocation.routingNodes();
    }

    void run() {

        logger.trace("starting to reconcile current allocation with desired balance {}", desiredBalance);

        if (routingNodes.size() == 0) {
            // no data nodes, so fail allocation to report red health
            failAllocationOfNewPrimaries(allocation);
            logger.trace("no nodes available, nothing to reconcile");
            return;
            // TODO test that we do this even if desired balance is empty
        }

        if (desiredBalance.desiredAssignments().isEmpty()) {
            // no desired state yet but it is on its way and we'll reroute again when its ready
            logger.trace("desired balance is empty, nothing to reconcile");
            return;
        }

        // compute next moves towards current desired balance:

        // 1. allocate unassigned shards first
        logger.trace("Reconciler#allocateUnassigned");
        allocateUnassigned();
        assert allocateUnassignedInvariant();

        // 2. move any shards that cannot remain where they are
        logger.trace("Reconciler#moveShards");
        moveShards();
        // 3. move any other shards that are desired elsewhere
        logger.trace("Reconciler#balance");
        balance();

        logger.trace("done");
    }

    private boolean allocateUnassignedInvariant() {
        // after allocateUnassigned, every shard must be either assigned or ignored

        assert routingNodes.unassigned().isEmpty();

        final var shardCounts = allocation.metadata()
            .stream()
            .flatMap(
                indexMetadata -> IntStream.range(0, indexMetadata.getNumberOfShards())
                    .mapToObj(
                        shardId -> Tuple.tuple(new ShardId(indexMetadata.getIndex(), shardId), indexMetadata.getNumberOfReplicas() + 1)
                    )
            )
            .collect(Collectors.toMap(Tuple::v1, Tuple::v2));

        for (final var shardRouting : routingNodes.unassigned().ignored()) {
            shardCounts.computeIfPresent(shardRouting.shardId(), (ignored, count) -> count == 1 ? null : count - 1);
        }

        for (final var routingNode : routingNodes) {
            for (final var shardRouting : routingNode) {
                shardCounts.computeIfPresent(shardRouting.shardId(), (ignored, count) -> count == 1 ? null : count - 1);
            }
        }

        assert shardCounts.isEmpty() : shardCounts;

        return true;
    }

    private void failAllocationOfNewPrimaries(RoutingAllocation allocation) {
        RoutingNodes routingNodes = allocation.routingNodes();
        assert routingNodes.size() == 0 : routingNodes;
        final RoutingNodes.UnassignedShards.UnassignedIterator unassignedIterator = routingNodes.unassigned().iterator();
        while (unassignedIterator.hasNext()) {
            final ShardRouting shardRouting = unassignedIterator.next();
            final UnassignedInfo unassignedInfo = shardRouting.unassignedInfo();
            if (shardRouting.primary() && unassignedInfo.getLastAllocationStatus() == UnassignedInfo.AllocationStatus.NO_ATTEMPT) {
                unassignedIterator.updateUnassigned(
                    new UnassignedInfo(
                        unassignedInfo.getReason(),
                        unassignedInfo.getMessage(),
                        unassignedInfo.getFailure(),
                        unassignedInfo.getNumFailedAllocations(),
                        unassignedInfo.getUnassignedTimeInNanos(),
                        unassignedInfo.getUnassignedTimeInMillis(),
                        unassignedInfo.isDelayed(),
                        UnassignedInfo.AllocationStatus.DECIDERS_NO,
                        unassignedInfo.getFailedNodeIds(),
                        unassignedInfo.getLastAllocatedNodeId()
                    ),
                    shardRouting.recoverySource(),
                    allocation.changes()
                );
            }
        }
    }

    private void allocateUnassigned() {
        RoutingNodes.UnassignedShards unassigned = routingNodes.unassigned();
        if (logger.isTraceEnabled()) {
            logger.trace("Start allocating unassigned shards");
        }
        if (unassigned.isEmpty()) {
            return;
        }

        /*
         * TODO: We could be smarter here and group the shards by index and then
         * use the sorter to save some iterations.
         */
        final PriorityComparator secondaryComparator = PriorityComparator.getAllocationComparator(allocation);
        final Comparator<ShardRouting> comparator = (o1, o2) -> {
            if (o1.primary() ^ o2.primary()) {
                return o1.primary() ? -1 : 1;
            }
            if (o1.getIndexName().compareTo(o2.getIndexName()) == 0) {
                return o1.getId() - o2.getId();
            }
            // this comparator is more expensive than all the others up there
            // that's why it's added last even though it could be easier to read
            // if we'd apply it earlier. this comparator will only differentiate across
            // indices all shards of the same index is treated equally.
            final int secondary = secondaryComparator.compare(o1, o2);
            assert secondary != 0 : "Index names are equal, should be returned early.";
            return secondary;
        };
        /*
         * we use 2 arrays and move replicas to the second array once we allocated an identical
         * replica in the current iteration to make sure all indices get allocated in the same manner.
         * The arrays are sorted by primaries first and then by index and shard ID so a 2 indices with
         * 2 replica and 1 shard would look like:
         * [(0,P,IDX1), (0,P,IDX2), (0,R,IDX1), (0,R,IDX1), (0,R,IDX2), (0,R,IDX2)]
         * if we allocate for instance (0, R, IDX1) we move the second replica to the secondary array and proceed with
         * the next replica. If we could not find a node to allocate (0,R,IDX1) we move all it's replicas to ignoreUnassigned.
         */
        ShardRouting[] primary = unassigned.drain();
        ShardRouting[] secondary = new ShardRouting[primary.length];
        int secondaryLength = 0;
        int primaryLength = primary.length;
        ArrayUtil.timSort(primary, comparator);
        do {
            nextShard: for (int i = 0; i < primaryLength; i++) {
                final var shard = primary[i];
                final var desiredNodeIds = desiredBalance.getDesiredNodeIds(shard.shardId());
                var isThrottled = false;
                for (final var desiredNodeId : desiredNodeIds) {
                    final var routingNode = routingNodes.node(desiredNodeId);
                    if (routingNode == null) {
                        // desired node no longer exists
                        continue;
                    }

                    final var canAllocateDecision = allocation.deciders().canAllocate(shard, routingNode, allocation);
                    switch (canAllocateDecision.type()) {
                        case YES -> {
                            if (logger.isTraceEnabled()) {
                                logger.trace("Assigned shard [{}] to [{}]", shard, desiredNodeId);
                            }
                            final long shardSize = DiskThresholdDecider.getExpectedShardSize(
                                shard,
                                ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE,
                                allocation.clusterInfo(),
                                allocation.snapshotShardSizeInfo(),
                                allocation.metadata(),
                                allocation.routingTable()
                            );
                            routingNodes.initializeShard(shard, desiredNodeId, null, shardSize, allocation.changes());
                            if (shard.primary() == false) {
                                // copy over the same replica shards to the secondary array so they will get allocated
                                // in a subsequent iteration, allowing replicas of other shards to be allocated first
                                while (i < primaryLength - 1 && comparator.compare(primary[i], primary[i + 1]) == 0) {
                                    secondary[secondaryLength++] = primary[++i];
                                }
                            }
                            continue nextShard;
                        }
                        case THROTTLE -> isThrottled = true;
                    }
                }

                if (logger.isTraceEnabled()) {
                    logger.trace("No eligible node found to assign shard [{}] amongst [{}]", shard, desiredNodeIds);
                }

                final var allocationStatus = UnassignedInfo.AllocationStatus.fromDecision(
                    isThrottled ? Decision.Type.THROTTLE : Decision.Type.NO
                );
                unassigned.ignoreShard(shard, allocationStatus, allocation.changes());
                if (shard.primary() == false) {
                    // we could not allocate it and we are a replica - check if we can ignore the other replicas
                    while (i < primaryLength - 1 && comparator.compare(primary[i], primary[i + 1]) == 0) {
                        unassigned.ignoreShard(primary[++i], allocationStatus, allocation.changes());
                    }
                }
            }
            primaryLength = secondaryLength;
            ShardRouting[] tmp = primary;
            primary = secondary;
            secondary = tmp;
            secondaryLength = 0;
        } while (primaryLength > 0);
    }

    private void moveShards() {
        // Iterate over the started shards interleaving between nodes, and check if they can remain. In the presence of throttling
        // shard movements, the goal of this iteration order is to achieve a fairer movement of shards from the nodes that are
        // offloading the shards.
        for (final var iterator = routingNodes.nodeInterleavedShardIterator(); iterator.hasNext();) {
            final var shardRouting = iterator.next();

            if (shardRouting.started() == false) {
                // can only move started shards
                continue;
            }

            final var desiredNodeIds = desiredBalance.getDesiredNodeIds(shardRouting.shardId());
            if (desiredNodeIds.contains(shardRouting.currentNodeId())) {
                // shard is already on a desired node
                continue;
            }

            if (allocation.deciders().canAllocate(shardRouting, allocation).type() != Decision.Type.YES) {
                // cannot allocate anywhere, no point in looking for a target node
                continue;
            }

            final var routingNode = routingNodes.node(shardRouting.currentNodeId());
            final var canRemainDecision = allocation.deciders().canRemain(shardRouting, routingNode, allocation);
            if (canRemainDecision.type() != Decision.Type.NO) {
                // it's desired elsewhere but technically it can remain on its current node. Defer its movement until later on to give
                // priority to shards that _must_ move.
                continue;
            }

            final var moveTarget = findRelocationTarget(shardRouting, desiredNodeIds);
            if (moveTarget != null) {
                routingNodes.relocateShard(
                    shardRouting,
                    moveTarget.getId(),
                    allocation.clusterInfo().getShardSize(shardRouting, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE),
                    allocation.changes()
                );
            }
        }
    }

    private void balance() {
        if (allocation.deciders().canRebalance(allocation).type() != Decision.Type.YES) {
            return;
        }

        // Iterate over the started shards interleaving between nodes, and try to move any which are on undesired nodes. In the presence of
        // throttling shard movements, the goal of this iteration order is to achieve a fairer movement of shards from the nodes that are
        // offloading the shards.
        for (final var iterator = routingNodes.nodeInterleavedShardIterator(); iterator.hasNext();) {
            final var shardRouting = iterator.next();

            if (shardRouting.started() == false) {
                // can only rebalance started shards
                continue;
            }

            final var desiredNodeIds = desiredBalance.getDesiredNodeIds(shardRouting.shardId());
            if (desiredNodeIds.contains(shardRouting.currentNodeId())) {
                // shard is already on a desired node
                continue;
            }

            if (allocation.deciders().canRebalance(shardRouting, allocation).type() != Decision.Type.YES) {
                // rebalancing disabled for this shard
                continue;
            }

            if (allocation.deciders().canAllocate(shardRouting, allocation).type() != Decision.Type.YES) {
                // cannot allocate anywhere, no point in looking for a target node
                continue;
            }

            final var rebalanceTarget = findRelocationTarget(shardRouting, desiredNodeIds, this::decideCanAllocate);
            if (rebalanceTarget != null) {
                routingNodes.relocateShard(
                    shardRouting,
                    rebalanceTarget.getId(),
                    allocation.clusterInfo().getShardSize(shardRouting, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE),
                    allocation.changes()
                );
            }
        }
    }

    private DiscoveryNode findRelocationTarget(final ShardRouting shardRouting, Set<String> desiredNodeIds) {
        final var moveDecision = findRelocationTarget(shardRouting, desiredNodeIds, this::decideCanAllocate);
        if (moveDecision != null) {
            return moveDecision;
        }

        final var shutdown = allocation.nodeShutdowns().get(shardRouting.currentNodeId());
        final var shardsOnReplacedNode = shutdown != null && shutdown.getType().equals(SingleNodeShutdownMetadata.Type.REPLACE);
        if (shardsOnReplacedNode) {
            return findRelocationTarget(shardRouting, desiredNodeIds, this::decideCanForceAllocateForVacate);
        }
        return null;
    }

    private DiscoveryNode findRelocationTarget(
        ShardRouting shardRouting,
        Set<String> desiredNodeIds,
        BiFunction<ShardRouting, RoutingNode, Decision> canAllocateDecider
    ) {
        for (final var nodeId : desiredNodeIds) {
            if (nodeId.equals(shardRouting.currentNodeId()) == false) {
                final var currentNode = routingNodes.node(nodeId);
                if (canAllocateDecider.apply(shardRouting, currentNode).type() == Decision.Type.YES) {
                    return currentNode.node();
                }
            }
        }

        return null;
    }

    private Decision decideCanAllocate(ShardRouting shardRouting, RoutingNode target) {
        return allocation.deciders().canAllocate(shardRouting, target, allocation);
    }

    private Decision decideCanForceAllocateForVacate(ShardRouting shardRouting, RoutingNode target) {
        return allocation.deciders().canForceAllocateDuringReplace(shardRouting, target, allocation);
    }

}
