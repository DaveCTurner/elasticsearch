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
import org.elasticsearch.core.UpdateForV9;

import java.io.IOException;

/**
 * A based request for master based operation.
 */
public abstract class MasterNodeRequest<Request extends MasterNodeRequest<Request>> extends ActionRequest {

    /**
     * In production code it's almost certainly a mistake to use this default for the master node timeout. Master-node actions triggered by
     * user requests should respect the timeout specified in the request, using {@link
     * org.elasticsearch.rest.RestUtils#getMasterNodeTimeout} which implements the common API behaviour described in the reference docs.
     * Internal master-node actions should probably not time out after just 30s - in many cases they should keep trying forever, and in the
     * few cases where that doesn't apply they should still be explicit about the desired timeout behaviour.
     */
    @Deprecated(forRemoval = true)
    public static final TimeValue TRAPPY_DEFAULT_MASTER_NODE_TIMEOUT = TimeValue.timeValueSeconds(30);

    @UpdateForV9 // replace with MINUS_ONE when such infinite timeouts are fully supported
    public static final TimeValue VERY_LONG_MASTER_NODE_TIMEOUT = TimeValue.MAX_VALUE;

    private TimeValue masterNodeTimeout0;

    protected MasterNodeRequest(TimeValue masterNodeTimeout) {
        this.masterNodeTimeout0 = masterNodeTimeout;
    }

    protected MasterNodeRequest(StreamInput in) throws IOException {
        super(in);
        masterNodeTimeout0 = in.readTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        assert hasReferences();
        out.writeTimeValue(masterNodeTimeout0);
    }

    /**
     * Specifies how long to wait when the master has not been discovered yet, or is disconnected, or is busy processing other tasks. The
     * value {@link TimeValue#MINUS_ONE} means to wait forever.
     */
    @SuppressWarnings("unchecked")
    public final Request masterNodeTimeout(TimeValue timeout) {
        this.masterNodeTimeout0 = timeout;
        return (Request) this;
    }

    /**
     * @return how long to wait when the master has not been discovered yet, or is disconnected, or is busy processing other tasks. The
     * value {@link TimeValue#MINUS_ONE} means to wait forever.
     */
    public final TimeValue masterNodeTimeout() {
        return this.masterNodeTimeout0;
    }
}
