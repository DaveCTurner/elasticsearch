/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;

/**
 * This class contains the logic used to check the cluster-wide shard limit before shards are created and ensuring that the limit is
 * updated correctly on setting updates, etc.
 *
 * NOTE: This is the limit applied at *shard creation time*. If you are looking for the limit applied at *allocation* time, which is
 * controlled by a different setting,
 * see {@link org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider}.
 */
public class ShardLimitValidator {
    public static final Setting<Integer> SETTING_CLUSTER_MAX_SHARDS_PER_NODE = Setting.intSetting(
        "cluster.max_shards_per_node",
        1000,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<Integer> SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN = Setting.intSetting(
        "cluster.max_shards_per_node.frozen",
        3000,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final String FROZEN_GROUP = "frozen";
    private static final String NORMAL_GROUP = "normal";
    static final Set<String> VALID_GROUPS = Set.of(NORMAL_GROUP, FROZEN_GROUP);
    public static final Setting<String> INDEX_SETTING_SHARD_LIMIT_GROUP = Setting.simpleString(
        "index.shard_limit.group",
        NORMAL_GROUP,
        value -> {
            if (VALID_GROUPS.contains(value) == false) {
                throw new IllegalArgumentException("[" + value + "] is not a valid shard limit group");
            }
        },
        Setting.Property.IndexScope,
        Setting.Property.PrivateIndex,
        Setting.Property.NotCopyableOnResize
    );
    protected final AtomicInteger shardLimitPerNode = new AtomicInteger();
    protected final AtomicInteger shardLimitPerNodeFrozen = new AtomicInteger();

    public ShardLimitValidator(final Settings settings, ClusterSettings clusterSettings) {
        shardLimitPerNode.set(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.get(settings));
        shardLimitPerNodeFrozen.set(SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN.get(settings));
        clusterSettings.addSettingsUpdateConsumer(SETTING_CLUSTER_MAX_SHARDS_PER_NODE, shardLimitPerNode::set);
        clusterSettings.addSettingsUpdateConsumer(SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN, shardLimitPerNodeFrozen::set);
    }

    /**
     * Checks whether an index can be created without going over the cluster shard limit.
     *
     * @param settings       the settings of the index to be created
     * @param state          the current cluster state
     * @throws ValidationException if creating this index would put the cluster over the cluster shard limit
     */
    public void validateShardLimit(final Settings settings, final ClusterState state) {
        final int numberOfShards = INDEX_NUMBER_OF_SHARDS_SETTING.get(settings);
        final int numberOfReplicas = IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(settings);
        final int shardsToCreate = numberOfShards * (1 + numberOfReplicas);
        final boolean frozen = FROZEN_GROUP.equals(INDEX_SETTING_SHARD_LIMIT_GROUP.get(settings));

        final Optional<String> shardLimit = checkShardLimit(frozen == false ? shardsToCreate : 0, frozen ? shardsToCreate : 0, state);
        if (shardLimit.isPresent()) {
            final ValidationException e = new ValidationException();
            e.addValidationError(shardLimit.get());
            throw e;
        }
    }

    /**
     * Validates whether a list of indices can be opened without going over the cluster shard limit.  Only counts indices which are
     * currently closed and will be opened, ignores indices which are already open.
     *
     * @param currentState The current cluster state.
     * @param indicesToOpen The indices which are to be opened.
     * @throws ValidationException If this operation would take the cluster over the limit and enforcement is enabled.
     */
    public void validateShardLimit(ClusterState currentState, Index[] indicesToOpen) {
        int frozen = 0;
        int normal = 0;
        for (Index index : indicesToOpen) {
            IndexMetadata imd = currentState.metadata().index(index);
            if (imd.getState().equals(IndexMetadata.State.CLOSE)) {
                int totalNewShards = imd.getNumberOfShards() * (1 + imd.getNumberOfReplicas());
                if (FROZEN_GROUP.equals(INDEX_SETTING_SHARD_LIMIT_GROUP.get(imd.getSettings()))) {
                    frozen += totalNewShards;
                } else {
                    normal += totalNewShards;
                }
            }
        }

        Optional<String> error = checkShardLimit(normal, frozen, currentState);
        if (error.isPresent()) {
            ValidationException ex = new ValidationException();
            ex.addValidationError(error.get());
            throw ex;
        }
    }

    public void validateShardLimitOnReplicaUpdate(ClusterState currentState, Index[] indices, int replicas) {
        int frozen = 0;
        int normal = 0;
        for (Index index : indices) {
            IndexMetadata indexMetadata = currentState.metadata().index(index);
            int shardsInIndex = indexMetadata.getNumberOfShards();
            int oldNumberOfReplicas = indexMetadata.getNumberOfReplicas();
            int replicaIncrease = replicas - oldNumberOfReplicas;
            int totalNewShards = replicaIncrease * shardsInIndex;
            if (indexMetadata.getState() == IndexMetadata.State.OPEN) {
                if (FROZEN_GROUP.equals(INDEX_SETTING_SHARD_LIMIT_GROUP.get(indexMetadata.getSettings()))) {
                    frozen += totalNewShards;
                } else {
                    normal += totalNewShards;
                }
            }
        }

        Optional<String> error = checkShardLimit(normal, frozen, currentState);
        if (error.isPresent()) {
            ValidationException ex = new ValidationException();
            ex.addValidationError(error.get());
            throw ex;
        }
    }

    /**
     * Checks to see if an operation can be performed without taking the cluster over the cluster-wide shard limit.
     * Returns an error message if appropriate, or an empty {@link Optional} otherwise.
     *
     * @param newShards         The number of normal shards to be added by this operation
     * @param newFrozenShards   The number of frozen shards to be added by this operation
     * @param state             The current cluster state
     * @return If present, an error message to be given as the reason for failing
     * an operation. If empty, a sign that the operation is valid.
     */
    private Optional<String> checkShardLimit(int newShards, int newFrozenShards, ClusterState state) {
        // we verify the two limits independently. This also means that if they have mixed frozen and other data-roles nodes, such a mixed
        // node can have both 1000 normal and 3000 frozen shards. This is the trade-off to keep the simplicity of the counts. We advocate
        // against such mixed nodes for production use anyway.
        int frozenNodeCount = nodeCount(state, ShardLimitValidator::hasFrozen);
        int normalNodeCount = nodeCount(state, ShardLimitValidator::hasNonFrozen);
        return checkShardLimit(newShards, state, shardLimitPerNode.get(), normalNodeCount, NORMAL_GROUP).or(
            () -> checkShardLimit(newFrozenShards, state, shardLimitPerNodeFrozen.get(), frozenNodeCount, FROZEN_GROUP)
        );
    }

    // package-private for testing
    static Optional<String> checkShardLimit(int newShards, ClusterState state, int maxShardsPerNode, int nodeCount, String group) {
        // Only enforce the shard limit if we have at least one data node, so that we don't block
        // index creation during cluster setup
        if (nodeCount == 0 || newShards <= 0) {
            return Optional.empty();
        }
        int maxShardsInCluster = maxShardsPerNode * nodeCount;
        int currentOpenShards = state.getMetadata().getTotalOpenIndexShards();

        if ((currentOpenShards + newShards) > maxShardsInCluster) {
            Predicate<IndexMetadata> indexMetadataPredicate = imd -> imd.getState().equals(IndexMetadata.State.OPEN)
                && group.equals(INDEX_SETTING_SHARD_LIMIT_GROUP.get(imd.getSettings()));
            long currentFilteredShards = state.metadata()
                .indices()
                .values()
                .stream()
                .filter(indexMetadataPredicate)
                .mapToInt(IndexMetadata::getTotalNumberOfShards)
                .sum();
            if ((currentFilteredShards + newShards) > maxShardsInCluster) {
                String errorMessage = "this action would add ["
                    + newShards
                    + "] shards, but this cluster currently has ["
                    + currentFilteredShards
                    + "]/["
                    + maxShardsInCluster
                    + "] maximum "
                    + group
                    + " shards open";
                return Optional.of(errorMessage);
            }
        }
        return Optional.empty();
    }

    private static int nodeCount(ClusterState state, Predicate<DiscoveryNode> nodePredicate) {
        return (int) state.getNodes().getDataNodes().values().stream().filter(nodePredicate).count();
    }

    private static boolean hasFrozen(DiscoveryNode node) {
        return node.getRoles().contains(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE);
    }

    private static boolean hasNonFrozen(DiscoveryNode node) {
        return node.getRoles().stream().anyMatch(r -> r.canContainData() && r != DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE);
    }

    /**
     * @param initialState The starting state for the validation.
     * @return A validator that can validate a sequence of replica count changes.
     */
    public ProgressiveReplicaCountChangeValidator getProgressiveReplicaCountChangeValidator(ClusterState initialState) {
        final var normalGroupValidator = new SingleGroupValidator(
            initialState,
            shardLimitPerNode.get(),
            ShardLimitValidator::hasNonFrozen,
            NORMAL_GROUP
        );
        final var frozenGroupValidator = new SingleGroupValidator(
            initialState,
            shardLimitPerNodeFrozen.get(),
            ShardLimitValidator::hasFrozen,
            FROZEN_GROUP
        );

        return (indices, newReplicaCount) -> {
            final var newNormalShards = normalGroupValidator.validateReplicaCountChange(indices, newReplicaCount);
            final var newFrozenShards = frozenGroupValidator.validateReplicaCountChange(indices, newReplicaCount);
            return () -> {
                normalGroupValidator.updateShardCount(newNormalShards, indices, newReplicaCount);
                frozenGroupValidator.updateShardCount(newFrozenShards, indices, newReplicaCount);
            };
        };
    }

    /**
     * Validates a sequence of replica count changes.
     */
    public interface ProgressiveReplicaCountChangeValidator {
        /**
         * Validates whether the given replica count update applied to the given set of indices respects the shards-per-node limits.
         * @param indices         The indices whose replica counts are being updated.
         * @param newReplicaCount The new replica count.
         * @return A {@link Runnable} which adjusts the validator's state to reflect this update.
         * @throws ValidationException if the given update may not be applied.
         */
        Runnable validateReplicaCountChange(Index[] indices, int newReplicaCount);
    }

    private static class SingleGroupValidator {
        private final Metadata initialMetadata;
        private final String groupName;
        private final int nodeCount;
        private final int maxShardsInCluster;
        private final int initialShardCountEstimate;
        private final Map<Index, Integer> updatedReplicaCounts = new HashMap<>();

        private int initialShardCountAccurate = -1;
        private int previouslyAddedNewShards = 0;

        SingleGroupValidator(
            ClusterState initialState,
            int shardLimitPerNode,
            Predicate<DiscoveryNode> discoveryNodePredicate,
            String groupName
        ) {
            this.initialMetadata = initialState.metadata();
            this.groupName = groupName;
            this.nodeCount = nodeCount(initialState, discoveryNodePredicate);
            this.maxShardsInCluster = shardLimitPerNode * nodeCount;
            this.initialShardCountEstimate = initialState.metadata().getTotalOpenIndexShards();
        }

        int validateReplicaCountChange(Index[] indices, int newReplicaCount) {
            if (nodeCount == 0) {
                return 0;
            }

            int currentNewShards = 0;
            for (final var index : indices) {
                final var indexMetadata = initialMetadata.index(index);
                final var initialReplicaCount = indexMetadata.getNumberOfReplicas();
                if (countsTowardsLimit(indexMetadata)) {
                    final var currentReplicaCount = updatedReplicaCounts.compute(index, (ignored, storedReplicaCount) -> {
                        if (storedReplicaCount != null) {
                            return storedReplicaCount;
                        } else {
                            if (initialReplicaCount == newReplicaCount) {
                                return null;
                            } else {
                                return initialReplicaCount;
                            }
                        }
                    });
                    if (currentReplicaCount != null) {
                        currentNewShards += (newReplicaCount - currentReplicaCount) * indexMetadata.getNumberOfShards();
                    }
                }
            }

            if (currentNewShards <= 0) {
                return currentNewShards;
            }

            if (initialShardCountEstimate + previouslyAddedNewShards + currentNewShards <= maxShardsInCluster) {
                return currentNewShards;
            }

            if (initialShardCountAccurate == -1) {
                // the coarse estimate was too coarse, so we must compute an accurate count of the relevant shards instead
                initialShardCountAccurate = 0;
                for (final var indexMetadata : initialMetadata.indices().values()) {
                    if (countsTowardsLimit(indexMetadata)) {
                        initialShardCountAccurate += indexMetadata.getTotalNumberOfShards();
                    }
                }
                assert initialShardCountAccurate <= initialShardCountEstimate;
            }

            if (initialShardCountAccurate + previouslyAddedNewShards + currentNewShards <= maxShardsInCluster) {
                return currentNewShards;
            }

            final var validationException = new ValidationException();
            validationException.addValidationError(
                String.format(
                    Locale.ROOT,
                    "this action would add [%d] shards, but this cluster currently has [%d]/[%d] maximum %s shards open",
                    currentNewShards,
                    initialShardCountAccurate + previouslyAddedNewShards,
                    maxShardsInCluster,
                    groupName
                )
            );
            throw validationException;
        }

        private boolean countsTowardsLimit(IndexMetadata indexMetadata) {
            return indexMetadata.getState() == IndexMetadata.State.OPEN
                && groupName.equals(INDEX_SETTING_SHARD_LIMIT_GROUP.get(indexMetadata.getSettings()));
        }

        void updateShardCount(int newShards, Index[] indices, int newReplicaCount) {
            previouslyAddedNewShards += newShards;
            assert 0 <= initialShardCountEstimate + previouslyAddedNewShards;
            assert initialShardCountAccurate == -1 || 0 <= initialShardCountAccurate + previouslyAddedNewShards;

            for (final var index : indices) {
                updatedReplicaCounts.computeIfPresent(index, (ignored1, ignored2) -> newReplicaCount);
            }
        }
    }
}
