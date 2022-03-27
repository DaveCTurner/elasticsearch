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
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesTransportRequest;
import org.elasticsearch.transport.TransportRequest;
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

public class JoinValidationService {

    private static final Logger logger = LogManager.getLogger(JoinValidationService.class);

    public static final String JOIN_VALIDATE_ACTION_NAME = "internal:cluster/coordination/join/validate";

    // the timeout for each cached value
    public static final Setting<TimeValue> JOIN_VALIDATION_CACHE_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.join_validation.cache_timeout",
        TimeValue.timeValueSeconds(60),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope
    );

    private static final TransportRequestOptions REQUEST_OPTIONS = TransportRequestOptions.of(null, TransportRequestOptions.Type.STATE);

    private final TimeValue cacheTimeout;
    private final TransportService transportService;
    private final Supplier<ClusterState> clusterStateSupplier;
    private final AtomicInteger queueSize = new AtomicInteger();
    private final Queue<AbstractRunnable> queue = new ConcurrentLinkedQueue<>();
    private final Map<Version, ReleasableBytesReference> statesByVersion = new HashMap<>();
    private final RefCounted executeRefs;

    public JoinValidationService(Settings settings, TransportService transportService, Supplier<ClusterState> clusterStateSupplier) {
        this.cacheTimeout = JOIN_VALIDATION_CACHE_TIMEOUT_SETTING.get(settings);
        this.transportService = transportService;
        this.clusterStateSupplier = clusterStateSupplier;
        this.executeRefs = AbstractRefCounted.of(() -> execute(cacheClearer));
    }

    public void validateJoin(DiscoveryNode discoveryNode, ActionListener<TransportResponse.Empty> listener) {
        if (executeRefs.tryIncRef()) {
            try {
                execute(new JoinValidation(discoveryNode, listener));
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

    boolean isIdle() {
        // this is for single-threaded tests to assert that the service becomes idle, so it is not properly synchronized
        return queue.isEmpty() && queueSize.get() == 0 && statesByVersion.isEmpty();
    }

    private void execute(AbstractRunnable task) {
        assert task == cacheClearer || executeRefs.hasReferences();
        queue.add(task);
        if (queueSize.getAndIncrement() == 0) {
            runProcessor();
        }
    }

    private void runProcessor() {
        transportService.getThreadPool().executor(ThreadPool.Names.CLUSTER_COORDINATION).execute(processor);
    }

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
            logger.error("unexpectedly failed to process queue item", e);
            assert false : e;
        }
    };

    private void processNextItem() {
        if (executeRefs.hasReferences() == false) {
            onShutdown();
            return;
        }

        final var nextItem = queue.poll();
        assert nextItem != null;
        nextItem.run();
        final var remaining = queueSize.decrementAndGet();
        assert remaining >= 0;
        if (remaining > 0) {
            runProcessor();
        }
    }

    private void onShutdown() {
        // shutting down when enqueueing the next processor run which means there is no active processor so it's safe to clear out the
        // cache ...
        cacheClearer.run();

        // ... and drain the queue
        do {
            final var nextItem = queue.poll();
            assert nextItem != null;
            if (nextItem != cacheClearer) {
                nextItem.onFailure(new NodeClosedException(transportService.getLocalNode()));
            }
        } while (queueSize.decrementAndGet() > 0);
    }

    private final AbstractRunnable cacheClearer = new AbstractRunnable() {
        @Override
        public void onFailure(Exception e) {
            logger.error("unexpectedly failed to clear cache", e);
            assert false : e;
        }

        @Override
        protected void doRun() {
            for (final var bytes : statesByVersion.values()) {
                bytes.decRef();
            }
            statesByVersion.clear();
            logger.trace("join validation cache cleared");
        }

        @Override
        public String toString() {
            return "clear join validation cache";
        }
    };

    private class JoinValidation extends ActionRunnable<TransportResponse.Empty> {
        private final DiscoveryNode discoveryNode;

        JoinValidation(DiscoveryNode discoveryNode, ActionListener<TransportResponse.Empty> listener) {
            super(listener);
            this.discoveryNode = discoveryNode;
        }

        @Override
        protected void doRun() throws Exception {
            final var cachedBytes = statesByVersion.get(discoveryNode.getVersion());
            final var bytes = Objects.requireNonNullElseGet(cachedBytes, () -> serializeClusterState(discoveryNode));
            assert bytes.hasReferences() : "already closed";
            bytes.incRef();
            transportService.sendRequest(
                discoveryNode,
                JOIN_VALIDATE_ACTION_NAME,
                discoveryNode.getVersion().onOrAfter(Version.V_8_2_0)
                    ? new BytesTransportRequest(bytes, discoveryNode.getVersion())
                    : new RawJoinValidationRequest(bytes),
                REQUEST_OPTIONS,
                new ActionListenerResponseHandler<>(
                    ActionListener.runAfter(listener, bytes::decRef),
                    in -> TransportResponse.Empty.INSTANCE,
                    ThreadPool.Names.CLUSTER_COORDINATION
                )
            );
            if (cachedBytes == null) {
                transportService.getThreadPool().schedule(() -> execute(cacheClearer), cacheTimeout, ThreadPool.Names.CLUSTER_COORDINATION);
            }
        }

        @Override
        public String toString() {
            return "send cached join validation request to " + discoveryNode;
        }
    }

    private ReleasableBytesReference serializeClusterState(DiscoveryNode discoveryNode) {
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
                assert false;
                bytesStream.close();
            }
        }
    }

    private static class RawJoinValidationRequest extends TransportRequest {

        private final BytesReference compressedBytes;

        RawJoinValidationRequest(BytesReference compressedBytes) {
            this.compressedBytes = compressedBytes;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            final var compressor = CompressorFactory.compressor(compressedBytes);
            assert compressor != null;
            try (var stream = compressor.threadLocalInputStream(compressedBytes.streamInput())) {
                final var buffer = new byte[PageCacheRecycler.BYTE_PAGE_SIZE];
                while (true) {
                    final var bytesRead = stream.read(buffer);
                    if (bytesRead == 0) {
                        break;
                    }
                    out.write(buffer, 0, bytesRead);
                }
            }
        }
    }
}
