/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.support.master;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

import static org.elasticsearch.core.TimeValue.timeValueSeconds;

/**
 * Abstract class that allows to mark action requests that support acknowledgements.
 * Facilitates consistency across different api.
 */
public abstract class AcknowledgedRequest<Request extends MasterNodeRequest<Request>> extends MasterNodeRequest<Request>
    implements
        AckedRequest {

    public static final TimeValue DEFAULT_ACK_TIMEOUT = timeValueSeconds(30);

    protected TimeValue ackTimeout;

    protected AcknowledgedRequest() {
        this(DEFAULT_ACK_TIMEOUT);
    }

    protected AcknowledgedRequest(TimeValue ackTimeout) {
        this.ackTimeout = ackTimeout;
    }

    protected AcknowledgedRequest(StreamInput in) throws IOException {
        super(in);
        this.ackTimeout = in.readTimeValue();
    }

    /**
     * Allows to set the timeout
     * @param ackTimeout timeout as a string (e.g. 1s)
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public final Request ackTimeout(String ackTimeout) {
        this.ackTimeout = TimeValue.parseTimeValue(ackTimeout, this.ackTimeout, getClass().getSimpleName() + ".ackTimeout");
        return (Request) this;
    }

    /**
     * Allows to set the timeout
     * @param timeout timeout as a {@link TimeValue}
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public final Request ackTimeout(TimeValue timeout) {
        this.ackTimeout = timeout;
        return (Request) this;
    }

    @Override
    public final TimeValue ackTimeout() {
        return ackTimeout;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeTimeValue(ackTimeout);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * AcknowledgedRequest that does not have any additional fields. Should be used instead of implementing noop children for
     * AcknowledgedRequest.
     */
    public static final class Plain extends AcknowledgedRequest<Plain> {

        public Plain(StreamInput in) throws IOException {
            super(in);
        }

        public Plain() {}
    }
}
