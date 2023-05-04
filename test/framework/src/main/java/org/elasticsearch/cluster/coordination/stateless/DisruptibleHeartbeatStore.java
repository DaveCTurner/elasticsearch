/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination.stateless;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.transport.DisruptableMockTransport;

import java.io.IOException;

public abstract class DisruptibleHeartbeatStore implements HeartbeatStore {

    private static final Logger logger = LogManager.getLogger(DisruptibleHeartbeatStore.class);

    private final HeartbeatStore delegate;

    protected DisruptibleHeartbeatStore(HeartbeatStore delegate) {
        this.delegate = delegate;
    }

    protected abstract DisruptableMockTransport.ConnectionStatus getConnectionStatus();

    @Override
    public final void writeHeartbeat(Heartbeat newHeartbeat, ActionListener<Void> listener) {
        switch (getConnectionStatus()) {
            case CONNECTED -> delegate.writeHeartbeat(newHeartbeat, listener);
            case DISCONNECTED -> listener.onFailure(new IOException("simulating disrupted access to shared store"));
            case BLACK_HOLE, BLACK_HOLE_REQUESTS_ONLY -> logger.trace("dropping request to write heartbeat [{}]", newHeartbeat);
        }
    }

    @Override
    public final void readLatestHeartbeat(ActionListener<Heartbeat> listener) {
        switch (getConnectionStatus()) {
            case CONNECTED -> delegate.readLatestHeartbeat(listener);
            case DISCONNECTED -> listener.onFailure(new IOException("simulating disrupted access to shared store"));
            case BLACK_HOLE, BLACK_HOLE_REQUESTS_ONLY -> logger.trace("dropping request to read latest heartbeat");
        }
    }
}
