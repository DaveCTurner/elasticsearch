/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.client.internal.RemoteClusterClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.Writeable;

import java.util.concurrent.Executor;

final class RemoteClusterAwareClient implements RemoteClusterClient {

    private final TransportService service;
    private final String clusterAlias;
    private final RemoteClusterService remoteClusterService;
    private final Executor responseExecutor;
    private final boolean ensureConnected;

    RemoteClusterAwareClient(TransportService service, String clusterAlias, Executor responseExecutor, boolean ensureConnected) {
        this.service = service;
        this.clusterAlias = clusterAlias;
        this.remoteClusterService = service.getRemoteClusterService();
        this.responseExecutor = responseExecutor;
        this.ensureConnected = ensureConnected;
    }

    @Override
    public <Request extends TransportRequest, Response extends TransportResponse> void execute(
        String actionName,
        Request request,
        Writeable.Reader<Response> responseReader,
        ActionListener<Response> listener
    ) {
        maybeEnsureConnected(listener.delegateFailureAndWrap((delegateListener, v) -> {
            final Transport.Connection connection;
            try {
                if (request instanceof RemoteClusterAwareRequest) {
                    DiscoveryNode preferredTargetNode = ((RemoteClusterAwareRequest) request).getPreferredTargetNode();
                    connection = remoteClusterService.getConnection(preferredTargetNode, clusterAlias);
                } else {
                    connection = remoteClusterService.getConnection(clusterAlias);
                }
            } catch (NoSuchRemoteClusterException e) {
                if (ensureConnected == false) {
                    // trigger another connection attempt, but don't wait for it to complete
                    remoteClusterService.ensureConnected(clusterAlias, ActionListener.noop());
                }
                throw e;
            }
            service.sendRequest(
                connection,
                actionName,
                request,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(delegateListener, responseReader, responseExecutor)
            );
        }));
    }

    private void maybeEnsureConnected(ActionListener<Void> ensureConnectedListener) {
        if (ensureConnected) {
            remoteClusterService.ensureConnected(clusterAlias, ensureConnectedListener);
        } else {
            ensureConnectedListener.onResponse(null);
        }
    }
}
