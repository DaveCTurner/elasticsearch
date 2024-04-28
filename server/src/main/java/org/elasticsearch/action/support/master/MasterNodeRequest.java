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

/**
 * A based request for master based operation.
 */
public abstract class MasterNodeRequest<Request extends MasterNodeRequest<Request>> extends ActionRequest {

    /**
     * A default value for the {@link #masterNodeTimeout}. Historically this was set implicitly on every {@link MasterNodeRequest}, relying
     * on callers to override it where appropriate, but in practice it is <i>always</i> appropriate to override this in production code.
     * Forgetting to do so leads to subtle but critical bugs that only arise when a cluster is struggling to process cluster state updates
     * as fast as normal, in which there is no way to lengthen these timeouts at runtime to bring the cluster back to health.
     * <p>
     * Therefore, do not use this constant. Instead, in production code, specify an appropriate the timeout when creating the request
     * instance. For example, instances that relate to a REST request should derive this timeout from the {@code ?master_timeout} request
     * parameter, using {@link org.elasticsearch.rest.RestUtils#getMasterNodeTimeout} to impose consistent behaviour across all APIs.
     * Instances that relate to internal activities should probably set this timeout very long since it's normally better to wait patiently
     * instead of failing sooner, especially if the failure simply triggers a retry. Alternatively, expose the relevant timeout using a
     * setting. In test code, use an explicit timeout, choosing 30s unless the test specifically needs a different value.
     *
     * @deprecated specify an appropriate timeout instead
     */
    @Deprecated(forRemoval = true)
    public static final TimeValue TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT = TimeValue.timeValueSeconds(30);

    protected TimeValue masterNodeTimeout;

    protected MasterNodeRequest(TimeValue masterNodeTimeout) {
        this.masterNodeTimeout = masterNodeTimeout;
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
     * Specifies how long to wait when the master has not been discovered yet, or is disconnected, or is busy processing other tasks. The
     * value {@link TimeValue#MINUS_ONE} means to wait forever.
     */
    @SuppressWarnings("unchecked")
    public final Request masterNodeTimeout(TimeValue timeout) {
        this.masterNodeTimeout = timeout;
        return (Request) this;
    }

    /**
     * @return how long to wait when the master has not been discovered yet, or is disconnected, or is busy processing other tasks. The
     * value {@link TimeValue#MINUS_ONE} means to wait forever.
     */
    public final TimeValue masterNodeTimeout() {
        return this.masterNodeTimeout;
    }
}
