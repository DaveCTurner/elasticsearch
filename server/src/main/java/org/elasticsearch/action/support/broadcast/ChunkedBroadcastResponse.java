/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.broadcast;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class ChunkedBroadcastResponse extends BaseBroadcastResponse implements ChunkedToXContent {

    public ChunkedBroadcastResponse(StreamInput in) throws IOException {
        super(in);
    }

    public ChunkedBroadcastResponse(
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
    }

    /**
     * Override in subclass to add custom fields following the common `_shards` field
     */
    protected Iterator<? extends ToXContent> customXContentChunks(ToXContent.Params params) {
        return Iterators.concat();
    }

    @Override
    public final Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.<ToXContent>concat(Iterators.single((builder, p) -> {
            builder.startObject();
            RestActions.buildBroadcastShardsHeader(builder, p, this);
            return builder;
        }), customXContentChunks(params), Iterators.single((builder, p) -> builder.endObject()));
    }
}
