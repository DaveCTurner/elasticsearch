/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.master;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Objects;

/**
 * A base request for requests that are routed to the elected master node.
 */
public abstract class MasterNodeRequest extends ActionRequest {

    public static final TimeValue DEFAULT_MASTER_NODE_TIMEOUT = TimeValue.timeValueSeconds(30);

    private final TimeValue masterNodeTimeout;

    /**
     * @param masterNodeTimeout Specifies how long to wait when the master has not been discovered yet, or is disconnected, or is busy
     *                          processing other tasks. The value {@link TimeValue#MINUS_ONE} means to wait forever.
     */
    protected MasterNodeRequest(TimeValue masterNodeTimeout) {
        this.masterNodeTimeout = Objects.requireNonNull(masterNodeTimeout);
    }

    protected MasterNodeRequest(StreamInput in) throws IOException {
        super(in);
        masterNodeTimeout = in.readTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        assert hasReferences();
        out.writeTimeValue(masterNodeTimeout);
    }

    /**
     * @return how long to wait when the master has not been discovered yet, or is disconnected, or is busy processing other tasks. The
     * value {@link TimeValue#MINUS_ONE} means to wait forever.
     */
    public final TimeValue masterNodeTimeout() {
        return this.masterNodeTimeout;
    }
}
