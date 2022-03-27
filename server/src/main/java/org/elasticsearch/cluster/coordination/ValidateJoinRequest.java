/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

public class ValidateJoinRequest extends TransportRequest {
    private final ClusterState state;
    private final RefCounted refCounted;

    public ValidateJoinRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_8_2_0)) {
            // it's a BytesTransportRequest containing the compressed state
            final var bytes = in.readReleasableBytesReference();
            this.state = readCompressed(in.getVersion(), bytes, in.namedWriteableRegistry());
            this.refCounted = bytes;
        } else {
            this.state = ClusterState.readFrom(in, null);
            this.refCounted = AbstractRefCounted.of(() -> {});
        }
    }

    private static ClusterState readCompressed(Version version, BytesReference bytes, NamedWriteableRegistry namedWriteableRegistry)
        throws IOException {
        final var compressor = CompressorFactory.compressor(bytes);
        StreamInput in = bytes.streamInput();
        try {
            if (compressor != null) {
                in = new InputStreamStreamInput(compressor.threadLocalInputStream(in));
            }
            in = new NamedWriteableAwareStreamInput(in, namedWriteableRegistry);
            in.setVersion(version);
            try (StreamInput input = in) {
                return ClusterState.readFrom(input, null);
            } catch (Exception e) {
                assert false : e;
                throw e;
            }
        } finally {
            IOUtils.close(in);
        }
    }

    public ValidateJoinRequest(ClusterState state) {
        // TODO this should not be called
        this.state = state;
        this.refCounted = AbstractRefCounted.of(() -> {});
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // TODO this should not be called
        super.writeTo(out);
        this.state.writeTo(out);
    }

    public ClusterState getState() {
        return state;
    }

    @Override
    public void incRef() {
        refCounted.incRef();
    }

    @Override
    public boolean tryIncRef() {
        return refCounted.tryIncRef();
    }

    @Override
    public boolean decRef() {
        return refCounted.decRef();
    }

    @Override
    public boolean hasReferences() {
        return refCounted.hasReferences();
    }
}
