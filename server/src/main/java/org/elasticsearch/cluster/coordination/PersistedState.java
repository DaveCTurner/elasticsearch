/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.core.Nullable;

import java.io.Closeable;
import java.io.IOException;

/**
 * Pluggable persistence layer for {@link CoordinationState}.
 */
public interface PersistedState extends Closeable {

    /**
     * Returns the current term
     */
    long getCurrentTerm();

    /**
     * Returns the last accepted cluster state
     */
    ClusterState getLastAcceptedState();

    /**
     * Sets a new current term.
     * After a successful call to this method, {@link #getCurrentTerm()} should return the last term that was set.
     * The value returned by {@link #getLastAcceptedState()} should not be influenced by calls to this method.
     */
    void setCurrentTerm(long currentTerm);

    /**
     * Sets a new last accepted cluster state.
     * After a successful call to this method, {@link #getLastAcceptedState()} should return the last cluster state that was set.
     * The value returned by {@link #getCurrentTerm()} should not be influenced by calls to this method.
     */
    void setLastAcceptedState(ClusterState clusterState);

    /**
     * Marks the last accepted cluster state as committed.
     * After a successful call to this method, {@link #getLastAcceptedState()} should return the last cluster state that was set,
     * with the last committed configuration now corresponding to the last accepted configuration, and the cluster uuid, if set,
     * marked as committed.
     */
    default void markLastAcceptedStateAsCommitted() {
        final ClusterState lastAcceptedState = getLastAcceptedState();
        Metadata.Builder metadataBuilder = null;
        if (lastAcceptedState.getLastAcceptedConfiguration().equals(lastAcceptedState.getLastCommittedConfiguration()) == false) {
            final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder(lastAcceptedState.coordinationMetadata())
                .lastCommittedConfiguration(lastAcceptedState.getLastAcceptedConfiguration())
                .build();
            metadataBuilder = Metadata.builder(lastAcceptedState.metadata());
            metadataBuilder.coordinationMetadata(coordinationMetadata);
        }
        assert lastAcceptedState.metadata().clusterUUID().equals(Metadata.UNKNOWN_CLUSTER_UUID) == false :
            "received cluster state with empty cluster uuid: " + lastAcceptedState;
        if (lastAcceptedState.metadata().clusterUUID().equals(Metadata.UNKNOWN_CLUSTER_UUID) == false &&
            lastAcceptedState.metadata().clusterUUIDCommitted() == false) {
            if (metadataBuilder == null) {
                metadataBuilder = Metadata.builder(lastAcceptedState.metadata());
            }
            metadataBuilder.clusterUUIDCommitted(true);
            LogManager.getLogger(PersistedState.class).info("cluster UUID set to [{}]", lastAcceptedState.metadata().clusterUUID());
        }
        if (metadataBuilder != null) {
            setLastAcceptedState(ClusterState.builder(lastAcceptedState).metadata(metadataBuilder).build());
        }
    }

    default void close() throws IOException {
    }

    /**
     * @return statistics about cluster state persistence, for example how much data has been persisted and how long it took, or {@code
     * null} if this node is not a data node or a master-eligible node and therefore does not persist cluster states.
     */
    @Nullable
    PersistedStateStats getStats();
}
