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
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

/**
 * Holds the desired balance and updates it as the cluster evolves.
 */
public class DesiredBalanceService {

    private static final Logger logger = LogManager.getLogger(DesiredBalanceService.class);

    private final ShardsAllocator delegateAllocator;

    private volatile DesiredBalance currentDesiredBalance = new DesiredBalance(Map.of());

    public DesiredBalanceService(ShardsAllocator delegateAllocator) {
        this.delegateAllocator = delegateAllocator;
    }

    // TODO reset desired balance to empty if new master is elected?

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
        final var unassignedPrimaries = new HashSet<ShardId>();

        if (routingNodes.size() == 0) {
            final var clearDesiredBalance = currentDesiredBalance.desiredAssignments().size() != 0;
            if (clearDesiredBalance) {
                currentDesiredBalance = new DesiredBalance(Map.of());
            }
            return clearDesiredBalance;
            // TODO test for this case
        }

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
        final var shardCopiesByShard = new HashMap<ShardId, Tuple<List<ShardRouting>, List<ShardRouting>>>();
        for (final var primary : new boolean[] { true, false }) {
            final RoutingNodes.UnassignedShards unassigned = routingNodes.unassigned();
            for (final var iterator = unassigned.iterator(); iterator.hasNext();) {
                final var shardRouting = iterator.next();
                if (shardRouting.primary() == primary) {
                    if (ignoredShards.contains(shardRouting)) {
                        iterator.removeAndIgnore(UnassignedInfo.AllocationStatus.NO_ATTEMPT, changes);
                        if (shardRouting.primary()) {
                            unassignedPrimaries.add(shardRouting.shardId());
                        }
                    } else {
                        shardCopiesByShard.computeIfAbsent(shardRouting.shardId(), ignored -> Tuple.tuple(new ArrayList<>(), List.of()))
                            .v1()
                            .add(shardRouting);
                    }
                }
            }
        }

        for (final var shardAndAssignments : routingNodes.getAssignedShards().entrySet()) {
            shardCopiesByShard.compute(
                shardAndAssignments.getKey(),
                (ignored, tuple) -> Tuple.tuple(tuple == null ? List.of() : tuple.v1(), shardAndAssignments.getValue())
            );
        }

        // we can assume that all possible shards will be allocated/relocated to one of their desired locations
        final var unassignedShardsToInitialize = new HashMap<ShardRouting, LinkedList<String>>();
        for (final var shardAndAssignments : shardCopiesByShard.entrySet()) {
            final var shardId = shardAndAssignments.getKey();
            final List<ShardRouting> unassignedShardRoutings = shardAndAssignments.getValue().v1();
            final List<ShardRouting> assignedShardRoutings = shardAndAssignments.getValue().v2();

            // treesets so that we are consistent about the order of future relocations
            final var shardsToRelocate = new TreeSet<>(Comparator.comparing(ShardRouting::currentNodeId));
            final var targetNodes = new TreeSet<>(desiredBalance.getDesiredNodeIds(shardId));
            targetNodes.retainAll(knownNodeIds);

            for (final var shardRouting : assignedShardRoutings) {
                assert shardRouting.started();
                if (targetNodes.remove(shardRouting.currentNodeId()) == false) {
                    shardsToRelocate.add(shardRouting);
                }
            }

            final var targetNodesIterator = targetNodes.iterator();
            for (final var shardRouting : unassignedShardRoutings) {
                assert shardRouting.unassigned();
                if (targetNodesIterator.hasNext()) {
                    unassignedShardsToInitialize.computeIfAbsent(shardRouting, ignored -> new LinkedList<>())
                        .add(targetNodesIterator.next());
                } else {
                    break;
                }
            }

            for (final var shardRouting : shardsToRelocate) {
                assert shardRouting.started();
                if (targetNodesIterator.hasNext()) {
                    routingNodes.startShard(
                        logger,
                        routingNodes.relocateShard(shardRouting, targetNodesIterator.next(), 0L, changes).v2(),
                        changes
                    );
                } else {
                    break;
                }
            }
        }

        final var unassignedPrimaryIterator = routingNodes.unassigned().iterator();
        while (unassignedPrimaryIterator.hasNext()) {
            final var shardRouting = unassignedPrimaryIterator.next();
            if (shardRouting.primary()) {
                final var nodeIds = unassignedShardsToInitialize.get(shardRouting);
                if (nodeIds != null && nodeIds.isEmpty() == false) {
                    final String nodeId = nodeIds.removeFirst();
                    routingNodes.startShard(logger, unassignedPrimaryIterator.initialize(nodeId, null, 0L, changes), changes);
                }
            }
        }

        final var unassignedReplicaIterator = routingNodes.unassigned().iterator();
        while (unassignedReplicaIterator.hasNext()) {
            final var shardRouting = unassignedReplicaIterator.next();
            if (unassignedPrimaries.contains(shardRouting.shardId()) == false) {
                final var nodeIds = unassignedShardsToInitialize.get(shardRouting);
                if (nodeIds != null && nodeIds.isEmpty() == false) {
                    final String nodeId = nodeIds.removeFirst();
                    routingNodes.startShard(logger, unassignedReplicaIterator.initialize(nodeId, null, 0L, changes), changes);
                }
            }
        }

        // TODO must also bypass ResizeAllocationDecider
        // TODO must also bypass RestoreInProgressAllocationDecider
        // TODO what about delayed allocation?

        // TODO consider also whether to unassign any shards that cannot remain on their current nodes so that the desired balance reflects
        // the actual desired state of the cluster. But this would mean that a REPLACE shutdown needs special handling at reconciliation
        // time too. But maybe it needs special handling anyway since reconciliation also tries to respect allocation rules.

        boolean hasChanges = false;
        do {

            if (hasChanges) {
                final var unassigned = routingAllocation.routingNodes().unassigned();

                // Not the first iteration, so every remaining unassigned shard has been ignored, perhaps due to throttling. We must bring
                // them all back out of the ignored list to give the allocator another go...
                unassigned.resetIgnored();

                // ... but not if they're ignored because they're out of scope for allocation
                for (final var iterator = unassigned.iterator(); iterator.hasNext();) {
                    final var shardRouting = iterator.next();
                    if (ignoredShards.contains(shardRouting)) {
                        iterator.removeAndIgnore(UnassignedInfo.AllocationStatus.NO_ATTEMPT, changes);
                    }
                }

                // TODO test that we reset ignored shards properly
            }

            logger.trace("running delegate allocator");
            delegateAllocator.allocate(routingAllocation);
            assert routingAllocation.routingNodes().unassigned().size() == 0; // any unassigned shards should now be ignored

            hasChanges = false;
            for (final var routingNode : routingNodes) {
                for (final var shardRouting : routingNode) {
                    if (shardRouting.initializing()) {
                        hasChanges = true;
                        routingNodes.startShard(logger, shardRouting, changes);
                        logger.trace("starting shard {}", shardRouting);
                        // TODO adjust disk usage info to reflect the assumed shard movement
                    }
                }
            }

            // TODO what if we never converge?
            // TODO maybe expose interim desired balances computed here

            // NB we run at least one iteration, but if another reroute happened meanwhile then publish the interim state and restart the
            // calculation
        } while (hasChanges && isFreshSupplier.getAsBoolean());

        final var desiredAssignments = new HashMap<ShardId, Set<String>>();
        for (var shardAndAssignments : routingNodes.getAssignedShards().entrySet()) {
            desiredAssignments.put(
                shardAndAssignments.getKey(),
                shardAndAssignments.getValue().stream().map(ShardRouting::currentNodeId).collect(Collectors.toUnmodifiableSet())
            );
        }

        logger.trace(
            hasChanges
                ? "newer cluster state received, publishing incomplete desired balance and restarting computation"
                : "desired balance computation converged"
        );

        final DesiredBalance newDesiredBalance = new DesiredBalance(desiredAssignments);
        assert desiredBalance == currentDesiredBalance;
        if (newDesiredBalance.equals(desiredBalance) == false) {
            if (logger.isTraceEnabled()) {
                for (Map.Entry<ShardId, Set<String>> desiredAssignment : newDesiredBalance.desiredAssignments().entrySet()) {
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
            logger.trace("desired balance unchanged: {}", desiredBalance);
            return false;
        }
    }

    public DesiredBalance getCurrentDesiredBalance() {
        return currentDesiredBalance;
    }
}
