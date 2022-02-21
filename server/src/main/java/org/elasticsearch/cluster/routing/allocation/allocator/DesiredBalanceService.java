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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

public class DesiredBalanceService {

    private static final Logger logger = LogManager.getLogger();

    private final ShardsAllocator delegateAllocator;

    private volatile DesiredBalance currentDesiredBalance= new DesiredBalance(Map.of());

    public DesiredBalanceService(ShardsAllocator delegateAllocator) {
        this.delegateAllocator = delegateAllocator;
    }

    /**
     * @return {@code true} if the desired balance changed, in which case reconciliation may be necessary so the the caller should schedule
     * another reroute.
     */
    boolean updateDesiredBalanceAndReroute(DesiredBalanceInput desiredBalanceInput, BooleanSupplier isFreshSupplier) {

        logger.trace("starting to recompute desired balance");

        final var routingAllocation = desiredBalanceInput.routingAllocation().mutableCloneForSimulation();
        final var routingNodes = routingAllocation.routingNodes();
        final var ignoredShards = new HashSet<>(desiredBalanceInput.ignoredShards());
        final var desiredBalance = currentDesiredBalance;
        final var changes = routingAllocation.changes();
        final var knownNodeIds = routingAllocation.nodes().stream().map(DiscoveryNode::getId).collect(Collectors.toSet());

        // we assume that all ongoing recoveries will complete
        for (final var routingNode : routingNodes) {
            for (final var shardRouting : routingNode) {
                if (shardRouting.initializing()) {
                    routingNodes.startShard(logger, shardRouting, changes);
                    // TODO adjust disk usage info to reflect the assumed shard movement
                }
            }
        }

        // we are not responsible for allocating unassigned primaries of existing shards, and we're only responsible for allocating
        // unassigned replicas if the ReplicaShardAllocator gives up, so we must respect these ignored shards
        final RoutingNodes.UnassignedShards unassigned = routingNodes.unassigned();
        for (final var shardRouting : unassigned) {
            if (ignoredShards.contains(shardRouting)) {
                unassigned.ignoreShard(shardRouting, UnassignedInfo.AllocationStatus.NO_ATTEMPT, changes);
            }
        }

        // we can assume that all possible shards will be allocated/relocated to one of their desired locations
        final var unassignedShardsToInitialize = new HashMap<ShardRouting, LinkedList<String>>();
        for (final var shardAndAssignments : routingNodes.getAssignedShards().entrySet()) {
            final var shardId = shardAndAssignments.getKey();
            final List<ShardRouting> shardRoutings = shardAndAssignments.getValue();

            final var shardsToAssign = new ArrayList<ShardRouting>();
            // treesets so that we are consistent about the order of future relocations
            final var shardsToRelocate = new TreeSet<>(Comparator.comparing(ShardRouting::currentNodeId));
            final var targetNodes = new TreeSet<>(desiredBalance.getDesiredNodeIds(shardId));
            targetNodes.retainAll(knownNodeIds);

            for (ShardRouting shardRouting : shardRoutings) {
                if (shardRouting.started()) {
                    if (targetNodes.remove(shardRouting.currentNodeId()) == false) {
                        shardsToRelocate.add(shardRouting);
                    }
                } else {
                    // TODO ugh this never happens because routingNodes.getAssignedShards() doesn't mention unassigned ones.
                    assert shardRouting.unassigned() : shardRouting;
                    shardsToAssign.add(shardRouting);
                }
            }

            final var targetNodesIterator = targetNodes.iterator();
            final var shardsIterator = Iterators.concat(shardsToRelocate.iterator(), shardsToAssign.iterator());
            while (targetNodesIterator.hasNext() && shardsIterator.hasNext()) {
                final ShardRouting shardRouting = shardsIterator.next();
                if (shardRouting.started()) {
                    routingNodes.startShard(
                        logger,
                        routingNodes.relocateShard(shardRouting, targetNodesIterator.next(), 0L, changes).v2(),
                        changes
                    );
                } else {
                    unassignedShardsToInitialize.computeIfAbsent(shardRouting, ignored -> new LinkedList<>())
                        .add(targetNodesIterator.next());
                }
            }
        }

        final var unassignedIterator = routingNodes.unassigned().iterator();
        while (unassignedIterator.hasNext()) {
            final var shardRouting = unassignedIterator.next();
            final var nodeIds = unassignedShardsToInitialize.get(shardRouting);
            if (nodeIds != null && nodeIds.isEmpty() == false) {
                final String nodeId = nodeIds.removeFirst();
                routingNodes.startShard(logger, unassignedIterator.initialize(nodeId, null, 0L, changes), changes);
            }
            // TODO must also bypass ResizeAllocationDecider
            // TODO must also bypass RestoreInProgressAllocationDecider
            // TODO what about delayed allocation?
        }

        boolean hasChanges;
        do {
            delegateAllocator.allocate(routingAllocation);

            hasChanges = false;
            for (final var routingNode : routingNodes) {
                for (final var shardRouting : routingNode) {
                    if (shardRouting.initializing()) {
                        hasChanges = true;
                        routingNodes.startShard(logger, shardRouting, changes);
                        // TODO adjust disk usage info to reflect the assumed shard movement
                    }
                }
            }

            // TODO what if we never converge?
            // TODO maybe expose interim desired balances computed here

            // NB we run at least one iteration, but if another reroute happened meanwhile then publish the interim state and restart the
            // calculation
        } while (hasChanges && isFreshSupplier.getAsBoolean());

        final var desiredAssignments = new HashMap<ShardId, List<String>>();
        for (var shardAndAssignments : routingNodes.getAssignedShards().entrySet()) {
            desiredAssignments.put(
                shardAndAssignments.getKey(),
                shardAndAssignments.getValue().stream().map(ShardRouting::currentNodeId).collect(Collectors.toList())
            );
        }

        final DesiredBalance newDesiredBalance = new DesiredBalance(desiredAssignments);
        assert desiredBalance == currentDesiredBalance;
        if (newDesiredBalance.equals(desiredBalance) == false) {
            if (logger.isTraceEnabled()) {
                for (Map.Entry<ShardId, List<String>> desiredAssignment : newDesiredBalance.desiredAssignments().entrySet()) {
                    final var shardId = desiredAssignment.getKey();
                    final var newNodes = desiredAssignment.getValue();
                    final var oldNodes = desiredBalance.desiredAssignments().get(shardId);
                    if (newNodes.equals(oldNodes)) {
                        logger.trace("{} desired balance unchanged,   allocating to {}", shardId, newNodes);
                    } else {
                        logger.trace("{} desired balance changed, now allocating to {} vs previous {}", shardId, newNodes, oldNodes);
                    }
                }
                logger.trace("desired balance updated");
            }
            currentDesiredBalance = newDesiredBalance;
            return true;
        } else {
            logger.trace("desired balance unchanged");
            return false;
        }
    }

    public DesiredBalance getCurrentDesiredBalance() {
        return currentDesiredBalance;
    }
}
