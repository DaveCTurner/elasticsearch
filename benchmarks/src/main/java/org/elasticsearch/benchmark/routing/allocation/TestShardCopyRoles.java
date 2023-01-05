/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.routing.allocation;

import org.elasticsearch.cluster.routing.ShardCopyRole;
import org.elasticsearch.cluster.routing.ShardCopyRoleFactory;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

public class TestShardCopyRoles {
    public static final ShardCopyRole EMPTY_ROLE = new ShardCopyRole() {
        @Override
        public boolean isPromotableToPrimary() {
            return true;
        }

        @Override
        public boolean isSearchable() {
            return true;
        }

        @Override
        public String getWriteableName() {
            return ShardCopyRole.WRITEABLE_NAME;
        }

        @Override
        public void writeTo(StreamOutput out) {}

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) {
            return builder;
        }
    };

    public static final ShardCopyRoleFactory EMPTY_FACTORY = new ShardCopyRoleFactory() {
        @Override
        public ShardCopyRole newReplicaRole() {
            return EMPTY_ROLE;
        }

        @Override
        public ShardCopyRole newRestoredRole(int copyIndex) {
            return EMPTY_ROLE;
        }

        @Override
        public ShardCopyRole newEmptyRole(int copyIndex) {
            return EMPTY_ROLE;
        }
    };

    private TestShardCopyRoles() {
        // no instances
    }
}
