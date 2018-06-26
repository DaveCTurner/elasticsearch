/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.cluster;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.discovery.zen.MasterFaultDetection;
import org.elasticsearch.discovery.zen.NodesFaultDetection;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.settings.Setting.Property;
import static org.elasticsearch.common.settings.Setting.positiveTimeSetting;


/**
 * This component is responsible for connecting to nodes once they are added to the cluster state, and disconnect when they are
 * removed. Also, it periodically checks that all connections are still open and if needed restores them.
 * Note that this component is *not* responsible for removing nodes from the cluster if they disconnect / do not respond
 * to pings. This is done by {@link NodesFaultDetection}. Master fault detection
 * is done by {@link MasterFaultDetection}.
 */
public class NodeConnectionsService extends AbstractLifecycleComponent {

    public static final Setting<TimeValue> CLUSTER_NODE_RECONNECT_INTERVAL_SETTING =
            positiveTimeSetting("cluster.nodes.reconnect_interval", TimeValue.timeValueSeconds(10), Property.NodeScope);

    public static final Setting<TimeValue> CLUSTER_NODE_CONNECT_ON_NEW_STATE_WAIT_SETTING =
            positiveTimeSetting("cluster.nodes.connect_wait", TimeValue.timeValueSeconds(3), Property.NodeScope);

    private final ThreadPool threadPool;
    private final TransportService transportService;

    private Map<DiscoveryNode, NodeConnection> nodeConnections = ConcurrentCollections.newConcurrentMap();

    private final TimeValue reconnectInterval;
    private final TimeValue connectWaitTime;

    private volatile ScheduledFuture<?> backgroundFuture = null;

    @Inject
    public NodeConnectionsService(Settings settings, ThreadPool threadPool, TransportService transportService) {
        super(settings);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.reconnectInterval = NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.get(settings);
        this.connectWaitTime = NodeConnectionsService.CLUSTER_NODE_CONNECT_ON_NEW_STATE_WAIT_SETTING.get(settings);
    }

    public void connectToNodes(DiscoveryNodes discoveryNodes) {
        CountDownLatch latch = new CountDownLatch(discoveryNodes.getSize());
        for (final DiscoveryNode node : discoveryNodes) {
            nodeConnections.computeIfAbsent(node, NodeConnection::new).ensureConnectedAsync(latch::countDown);
        }
        try {
            if (latch.await(connectWaitTime.millis(), TimeUnit.MILLISECONDS) == false) {
                logger.info("timed out after [{}] when waiting for node connections, proceeding anyway", connectWaitTime);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Disconnects from all nodes except the ones provided as parameter
     */
    public void disconnectFromNodesExcept(DiscoveryNodes nodesToKeep) {
        Set<NodeConnection> toDisconnect = new HashSet<>();
        for (final NodeConnection nodeConnection : nodeConnections.values()) {
            if (nodesToKeep.nodeExists(nodeConnection.discoveryNode) == false) {
                toDisconnect.add(nodeConnection);
            }
        }

        CountDownLatch latch = new CountDownLatch(toDisconnect.size());
        for (final NodeConnection nodeConnection : toDisconnect) {
            boolean removed = nodeConnections.remove(nodeConnection.discoveryNode, nodeConnection);
            assert removed;
            nodeConnection.disconnect(latch::countDown);
        }

        try {
            if (latch.await(connectWaitTime.millis(), TimeUnit.MILLISECONDS) == false) {
                logger.info("timed out after [{}] when waiting for node disconnections, proceeding anyway", connectWaitTime);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    class ConnectionChecker extends AbstractRunnable {

        @Override
        public void onFailure(Exception e) {
            logger.warn("unexpected error while checking for node reconnects", e);
        }

        protected void doRun() {
            for (final NodeConnection nodeConnection : nodeConnections.values()) {
                nodeConnection.ensureConnected();
            }
        }

        @Override
        public void onAfter() {
            if (lifecycle.started()) {
                backgroundFuture = threadPool.schedule(reconnectInterval, ThreadPool.Names.GENERIC, this);
            }
        }
    }

    @Override
    protected void doStart() {
        backgroundFuture = threadPool.schedule(reconnectInterval, ThreadPool.Names.GENERIC, new ConnectionChecker());
    }

    @Override
    protected void doStop() {
        FutureUtils.cancel(backgroundFuture);
    }

    @Override
    protected void doClose() {

    }

    private class NodeConnection {
        private final Object mutex = new Object(); // protects connectionInProgress and connectionCompletionListeners
        private volatile boolean connectionInProgress; // volatile so we can assert it without using the mutex
        private List<Runnable> connectionCompletionListeners = new ArrayList<>();

        private final DiscoveryNode discoveryNode;
        private final AtomicInteger connectionFailureCount = new AtomicInteger();

        NodeConnection(DiscoveryNode discoveryNode) {
            this.discoveryNode = discoveryNode;
        }

        void ensureConnectedAsync(Runnable onCompletion) {
            synchronized (mutex) {
                connectionCompletionListeners.add(onCompletion);
                if (connectionInProgress) {
                    return;
                }
                connectionInProgress = true;
            }

            if (transportService.nodeConnected(discoveryNode)) {
                onConnectionCompletion();
                return;
            }

            // spawn to another thread to do in parallel
            threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    // both errors and rejections are logged here. the service
                    // will try again after `cluster.nodes.reconnect_interval` on all nodes but the current master.
                    // On the master, node fault detection will remove these nodes from the cluster as their are not
                    // connected. Note that it is very rare that we end up here on the master.
                    logger.warn(() -> new ParameterizedMessage("failed to connect to {}", discoveryNode), e);
                }

                @Override
                protected void doRun() {
                    validateAndConnect();
                }

                @Override
                public void onAfter() {
                    onConnectionCompletion();
                }
            });
        }

        private void validateAndConnect() {
            assert connectionInProgress;

            if (lifecycle.stoppedOrClosed() || nodeConnections.containsKey(discoveryNode) == false) {
                // Nothing to do
                return;
            }

            try {
                // connecting to an already connected node is a noop
                transportService.connectToNode(discoveryNode);
                connectionFailureCount.set(0);
            } catch (Exception e) {
                final int currentConnectionFailureCount = connectionFailureCount.incrementAndGet();
                // log every 6th failure
                if ((currentConnectionFailureCount % 6) == 1) {
                    logger.warn(() -> new ParameterizedMessage(
                        "failed to connect to node {} (tried [{}] times)", discoveryNode, currentConnectionFailureCount), e);
                }
            }
        }

        private void onConnectionCompletion() {
            final List<Runnable> toNotify;
            synchronized (mutex) {
                assert connectionInProgress;

                // Disconnections can occur concurrently with connection attempts. On a disconnection this object is removed from the
                // nodeConnections map. It's possible that this node was added back to the map before we get here, in which case
                // there's another NodeConnection object that's responsible for it.
                if (nodeConnections.containsKey(discoveryNode)) {
                    doDisconnect();
                }

                connectionInProgress = false;
                toNotify = new ArrayList<>(connectionCompletionListeners);
                connectionCompletionListeners.clear();
            }
            toNotify.forEach(Runnable::run);
        }

        private void doDisconnect() {
            assert Thread.holdsLock(mutex) : "doDisconnect should be called under lock";

            try {
                transportService.disconnectFromNode(discoveryNode);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("failed to disconnect from node [{}]", discoveryNode), e);
            }
        }

        void ensureConnected() {
            synchronized (mutex) {
                if (connectionInProgress) {
                    return;
                }
                connectionInProgress = true;
            }

            validateAndConnect();
            onConnectionCompletion();
        }

        public void disconnect(Runnable onCompletion) {
            synchronized (mutex) {
                if (connectionInProgress) {
                    // we check whether to disconnect at the end of each connection attempt
                    connectionCompletionListeners.add(onCompletion);
                    return;
                }

                assert this != nodeConnections.get(discoveryNode) : "should already have been removed from nodeConnections";
                doDisconnect();
            }

            onCompletion.run();
        }
    }
}
