/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class VerifyNodeRepositoryAction {

    private static final Logger logger = LogManager.getLogger(VerifyNodeRepositoryAction.class);

    private static final String ACTION_NAME = "internal:admin/repository/verify";

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final RepositoriesService repositoriesService;

    VerifyNodeRepositoryAction(TransportService transportService, ClusterService clusterService, RepositoriesService repositoriesService) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.repositoriesService = repositoriesService;
        transportService.registerRequestHandler(
            ACTION_NAME,
            transportService.getThreadPool().executor(ThreadPool.Names.SNAPSHOT),
            VerifyNodeRepositoryRequest::new,
            new VerifyNodeRepositoryRequestHandler()
        );
    }

    void verify(String repository, String verificationToken, final ActionListener<List<DiscoveryNode>> listener) {
        try (var verification = new AsyncVerification(repository, verificationToken, listener)) {
            for (final var node : clusterService.state().nodes().getMasterAndDataNodes().values()) {
                if (RepositoriesService.isDedicatedVotingOnlyNode(node.getRoles()) == false) {
                    verification.verifyNode(node);
                }
            }
        }
    }

    private class AsyncVerification implements Releasable {
        private final String repository;
        private final String verificationToken;
        private final RefCountingRunnable refs;
        private boolean hasError = false;
        private final StringBuilder errorBuilder = new StringBuilder(0);
        private final List<DiscoveryNode> nodes = new ArrayList<>();

        AsyncVerification(String repository, String verificationToken, ActionListener<List<DiscoveryNode>> listener) {
            this.repository = repository;
            this.verificationToken = verificationToken;
            this.refs = new RefCountingRunnable(() -> {
                if (hasError) {
                    errorBuilder.append(']');
                    listener.onFailure(new RepositoryVerificationException(repository, errorBuilder.toString()));
                } else {
                    listener.onResponse(nodes);
                }
            });
        }

        void verifyNode(DiscoveryNode discoveryNode) {
            nodes.add(discoveryNode);
            transportService.sendRequest(
                discoveryNode,
                ACTION_NAME,
                new VerifyNodeRepositoryRequest(repository, verificationToken),
                TransportResponseHandler.empty(
                    TransportResponseHandler.TRANSPORT_WORKER,
                    ActionListener.releaseAfter(new ActionListener<>() {
                        @Override
                        public void onResponse(Void unused) {}

                        @Override
                        public void onFailure(Exception e) {
                            addError(discoveryNode, e);
                        }
                    }, refs.acquire())
                )
            );
        }

        private synchronized void addError(DiscoveryNode discoveryNode, Exception e) {
            if (hasError) {
                errorBuilder.append(',');
            } else {
                errorBuilder.append('[');
                hasError = true;
            }
            errorBuilder.append("[").append(discoveryNode.getId()).append(", '").append(e.toString()).append("']");
        }

        @Override
        public void close() {
            refs.close();
        }
    }

    private static class VerifyNodeRepositoryRequest extends TransportRequest {

        private final String repository;
        private final String verificationToken;

        VerifyNodeRepositoryRequest(StreamInput in) throws IOException {
            super(in);
            repository = in.readString();
            verificationToken = in.readString();
        }

        VerifyNodeRepositoryRequest(String repository, String verificationToken) {
            this.repository = repository;
            this.verificationToken = verificationToken;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(repository);
            out.writeString(verificationToken);
        }
    }

    private class VerifyNodeRepositoryRequestHandler implements TransportRequestHandler<VerifyNodeRepositoryRequest> {
        @Override
        public void messageReceived(VerifyNodeRepositoryRequest request, TransportChannel channel, Task task) throws Exception {
            try {
                assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SNAPSHOT);
                repositoriesService.repository(request.repository).verify(request.verificationToken, clusterService.localNode());
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            } catch (Exception ex) {
                logger.warn(() -> "[" + request.repository + "] failed to verify repository", ex);
                throw ex;
            }
        }
    }
}
