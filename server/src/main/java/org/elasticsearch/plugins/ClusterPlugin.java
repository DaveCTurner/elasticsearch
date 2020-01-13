/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugins;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;

/**
 * An extension point for {@link Plugin} implementations to customer behavior of cluster management.
 */
public interface ClusterPlugin {

    /**
     * Return deciders used to customize where shards are allocated.
     *
     * @param settings Settings for the node
     * @param clusterSettings Settings for the cluster
     * @return Custom {@link AllocationDecider} instances
     */
    default Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
        return Collections.emptyList();
    }

    /**
     * Return {@link ShardsAllocator} implementations added by this plugin.
     *
     * The key of the returned {@link Map} is the name of the allocator, and the value
     * is a function to construct the allocator.
     *
     * @param settings Settings for the node
     * @param clusterSettings Settings for the cluster
     * @return A map of allocator implementations
     */
    default Map<String, Supplier<ShardsAllocator>> getShardsAllocators(Settings settings, ClusterSettings clusterSettings) {
        return Collections.emptyMap();
    }

    /**
     * Called when the node is started
     */
    default void onNodeStarted() {
    }

    /**
     * An interface that describes how to set the recovery source on a failed primary in the case that there are no active replicas that can
     * be promoted to primary instead.
     */
    @FunctionalInterface
    interface PrimaryRecoverySourceFactory {
        /**
         * Returns the recovery source for a failed primary in the case that there are no active replicas to promote.
         */
        RecoverySource getPrimaryRecoverySource(IndexMetaData indexMetaData);
    }

    /**
     * The {@link PrimaryRecoverySourceFactory} mappings for this plugin. When a primary shard fails and there are no active replicas which
     * can be promoted to primary then the unassigned primary's recovery source is obtained from the factory according to the
     * {@link org.elasticsearch.cluster.routing.allocation.AllocationService#PRIMARY_RECOVERY_SOURCE_TYPE_SETTING}.
     */
    default Map<String, PrimaryRecoverySourceFactory> getPrimaryRecoverySourceFactories() {
        return Collections.emptyMap();
    }

}
