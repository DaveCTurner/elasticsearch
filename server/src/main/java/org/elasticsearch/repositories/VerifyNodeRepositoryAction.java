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
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class VerifyNodeRepositoryAction {

    private static final Logger logger = LogManager.getLogger(VerifyNodeRepositoryAction.class);

    public static final String ACTION_NAME = "internal:admin/repository/verify";

    private final TransportService transportService;

    private final ClusterService clusterService;

    public VerifyNodeRepositoryAction(
        TransportService transportService,
        ClusterService clusterService,
        RepositoriesService repositoriesService
    ) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        transportService.registerRequestHandler(
            ACTION_NAME,
            ThreadPool.Names.SNAPSHOT,
            VerifyNodeRepositoryRequest::new,
            (request, channel, task) -> ActionListener.run(new ChannelActionListener<>(channel), l -> {
                try {
                    repositoriesService.repository(request.repository)
                        .verify(request.verificationToken, request.includesRootBlob, transportService.getLocalNode());
                    l.onResponse(TransportResponse.Empty.INSTANCE);
                } catch (Exception ex) {
                    logger.warn(() -> "[" + request.repository + "] failed to verify repository", ex);
                    throw ex;
                }
            })
        );
    }

    public void verify(String repository, String verificationToken, final ActionListener<List<DiscoveryNode>> listener) {
        final List<DiscoveryNode> nodes = new ArrayList<>();
        for (DiscoveryNode node : clusterService.state().nodes().getMasterAndDataNodes().values()) {
            if (RepositoriesService.isDedicatedVotingOnlyNode(node.getRoles()) == false) {
                nodes.add(node);
            }
        }
        final List<VerificationFailure> errors = Collections.synchronizedList(new ArrayList<>());
        try (var refs = new RefCountingRunnable(() -> {
            if (errors.isEmpty()) {
                listener.onResponse(nodes);
            } else {
                final RepositoryVerificationException e = new RepositoryVerificationException(repository, errors.toString());
                for (VerificationFailure error : errors) {
                    e.addSuppressed(error.getCause());
                }
                listener.onFailure(e);
            }
        })) {
            for (final DiscoveryNode node : nodes) {
                transportService.sendRequest(
                    node,
                    ACTION_NAME,
                    new VerifyNodeRepositoryRequest(repository, verificationToken),
                    new ActionListenerResponseHandler<>(
                        ActionListener.releaseAfter(
                            ActionListener.noop().delegateResponse((l, e) -> errors.add(new VerificationFailure(node.getId(), e))),
                            refs.acquire()
                        ),
                        in -> TransportResponse.Empty.INSTANCE,
                        ThreadPool.Names.SAME
                    )
                );
            }
        }
    }

    static class VerifyNodeRepositoryRequest extends TransportRequest {

        final String repository;
        final String verificationToken;
        final boolean includesRootBlob;

        VerifyNodeRepositoryRequest(StreamInput in) throws IOException {
            super(in);
            repository = in.readString();
            verificationToken = in.readString();
            includesRootBlob = in.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0);
        }

        VerifyNodeRepositoryRequest(String repository, String verificationToken) {
            this.repository = repository;
            this.verificationToken = verificationToken;
            this.includesRootBlob = true;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(repository);
            out.writeString(verificationToken);
        }
    }

}
