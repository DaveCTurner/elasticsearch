/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesTransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.coordination.JoinHelper.JOIN_VALIDATE_CLUSTER_STATE_ACTION_NAME;

public class JoinValidationService {

    private static final Logger logger = LogManager.getLogger(JoinValidationService.class);

    private static final TransportRequestOptions REQUEST_OPTIONS = TransportRequestOptions.of(null, TransportRequestOptions.Type.STATE);

    private final TransportService transportService;
    private final Supplier<ClusterState> clusterStateSupplier;
    private final AtomicInteger queueSize = new AtomicInteger();
    private final Queue<QueueItem> queue = new ConcurrentLinkedQueue<>();
    private final Map<Version, ReleasableBytesReference> statesByVersion = new HashMap<>();
    private final RefCounted executeRefs = AbstractRefCounted.of(() -> execute(QueueItem.CLEAR_CACHE));

    private final AbstractRunnable processor = new AbstractRunnable() {
        @Override
        protected void doRun() {
            processNextItem();
        }

        @Override
        public void onRejection(Exception e) {
            assert e instanceof EsRejectedExecutionException esre && esre.isExecutorShutdown();
            onShutdown();
        }

        @Override
        public void onFailure(Exception e) {
            assert false : e;
        }
    };

    public JoinValidationService(TransportService transportService, Supplier<ClusterState> clusterStateSupplier) {
        this.transportService = transportService;
        this.clusterStateSupplier = clusterStateSupplier;
    }

    public void validateJoin(DiscoveryNode discoveryNode, ActionListener<TransportResponse.Empty> listener) {
        if (executeRefs.tryIncRef()) {
            try {
                execute(new QueueItem(discoveryNode, listener));
            } finally {
                executeRefs.decRef();
            }
        } else {
            listener.onFailure(new NodeClosedException(transportService.getLocalNode()));
        }
    }

    public void stop() {
        executeRefs.decRef();
    }

    private void execute(QueueItem queueItem) {
        assert queueItem == QueueItem.CLEAR_CACHE || executeRefs.hasReferences();
        queue.add(queueItem);
        if (queueSize.getAndIncrement() == 0) {
            runProcessor();
        }
    }

    private void runProcessor() {
        transportService.getThreadPool().executor(ThreadPool.Names.CLUSTER_COORDINATION).execute(processor);
    }

    private void processNextItem() {
        final var pendingJoinValidation = queue.poll();
        assert pendingJoinValidation != null;
        final var listener = pendingJoinValidation.listener();

        try {
            if (pendingJoinValidation == QueueItem.CLEAR_CACHE) {
                clearCache();
                listener.onResponse(TransportResponse.Empty.INSTANCE);
            } else {
                sendJoinValidationRequest(pendingJoinValidation.discoveryNode(), listener);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        } finally {
            final var remaining = queueSize.decrementAndGet();
            assert remaining >= 0;
            if (remaining > 0) {
                runProcessor();
            }
        }
    }

    private void sendJoinValidationRequest(DiscoveryNode discoveryNode, ActionListener<TransportResponse.Empty> listener) {
        assert discoveryNode != null;
        final var cachedBytes = statesByVersion.get(discoveryNode.getVersion());
        final var bytes = Objects.requireNonNullElseGet(cachedBytes, () -> {
            final var bytesStream = transportService.newNetworkBytesStream();
            final var clusterState = clusterStateSupplier.get();
            final var version = discoveryNode.getVersion();
            var success = false;
            try {
                try (
                    var stream = new OutputStreamStreamOutput(
                        CompressorFactory.COMPRESSOR.threadLocalOutputStream(Streams.flushOnCloseStream(bytesStream))
                    )
                ) {
                    stream.setVersion(version);
                    clusterState.writeTo(stream);
                } catch (IOException e) {
                    throw new ElasticsearchException("failed to serialize cluster state for publishing to node {}", e, discoveryNode);
                }
                final ReleasableBytesReference newBytes = new ReleasableBytesReference(bytesStream.bytes(), bytesStream);
                logger.trace(
                    "serialized join validation cluster state version [{}] for node version [{}] with size [{}]",
                    clusterState.version(),
                    version,
                    newBytes.length()
                );
                final var previousBytes = statesByVersion.put(version, newBytes);
                assert previousBytes == null;
                success = true;
                return newBytes;
            } finally {
                if (success == false) {
                    bytesStream.close();
                }
            }
        });
        assert bytes.hasReferences() : "already closed";
        bytes.incRef();
        if (cachedBytes == null) {
            transportService.getThreadPool()
                .schedule(
                    () -> execute(QueueItem.CLEAR_CACHE),
                    TimeValue.timeValueSeconds(60) /* TODO make configurable */,
                    ThreadPool.Names.CLUSTER_COORDINATION
                );
        }
        transportService.sendRequest(
            discoveryNode,
            JOIN_VALIDATE_CLUSTER_STATE_ACTION_NAME,
            new BytesTransportRequest(bytes, discoveryNode.getVersion()),
            REQUEST_OPTIONS,
            new ActionListenerResponseHandler<>(
                ActionListener.runAfter(listener, bytes::decRef),
                in -> TransportResponse.Empty.INSTANCE,
                ThreadPool.Names.CLUSTER_COORDINATION
            )
        );
    }

    private void clearCache() {
        for (final var bytes : statesByVersion.values()) {
            bytes.decRef();
        }
        statesByVersion.clear();
    }

    private void onShutdown() {
        // shutting down when enqueueing the next processor task which means there is no active processor so it's safe to clear out the
        // cache ...
        for (final var bytes : statesByVersion.values()) {
            bytes.decRef();
        }
        statesByVersion.clear();

        // ... and drain the queue
        do {
            final var pendingJoinValidation = queue.poll();
            assert pendingJoinValidation != null;
            if (pendingJoinValidation.discoveryNode() == null) {
                pendingJoinValidation.listener().onResponse(TransportResponse.Empty.INSTANCE);
            } else {
                pendingJoinValidation.listener().onFailure(new NodeClosedException(transportService.getLocalNode()));
            }
        } while (queueSize.decrementAndGet() > 0);
    }

    private record QueueItem(@Nullable // to represent a cleanup action in the queue
    DiscoveryNode discoveryNode, ActionListener<TransportResponse.Empty> listener) {

        static QueueItem CLEAR_CACHE = new QueueItem(null, new ActionListener<>() {
            @Override
            public void onResponse(TransportResponse.Empty empty) {
                logger.trace("join validation cache cleared");
            }

            @Override
            public void onFailure(Exception e) {
                assert false : e;
            }
        });
    }
}
