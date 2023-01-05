/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

public interface ShardCopyRoleFactory {

    /**
     * @return the role for a new replica copy of an existing shard.
     */
    ShardCopyRole newReplicaRole();

    /**
     * @return the role for a copy of a new shard being restored from snapshot, where {@code copyIndex} is the index of the copy ({@code 0}
     * for the primary and {@code 1..N} for replicas).
     */
    ShardCopyRole newRestoredRole(int copyIndex);

    /**
     * @return the role for a copy of a new empty shard, where {@code copyIndex} is the index of the copy ({@code 0} for the primary and
     * {@code 1..N} for replicas).
     */
    ShardCopyRole newEmptyRole(int copyIndex);
}
