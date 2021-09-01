/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.internal.io.IOUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class manages node connections within a cluster. The connection is opened by the underlying transport.
 * Once the connection is opened, this class manages the connection. This includes closing the connection when
 * the connection manager is closed.
 */
public class ClusterConnectionManager implements ConnectionManager {

    private static final Logger logger = LogManager.getLogger(ClusterConnectionManager.class);

    private final ConcurrentMap<DiscoveryNode, Transport.Connection> connectedNodes = ConcurrentCollections.newConcurrentMap();
    private final ConcurrentMap<DiscoveryNode, ListenableFuture<Transport.Connection>> pendingConnections
        = ConcurrentCollections.newConcurrentMap();
    private final AbstractRefCounted connectingRefCounter = new AbstractRefCounted("connection manager") {
        @Override
        protected void closeInternal() {
            Iterator<Map.Entry<DiscoveryNode, Transport.Connection>> iterator = connectedNodes.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<DiscoveryNode, Transport.Connection> next = iterator.next();
                try {
                    IOUtils.closeWhileHandlingException(next.getValue());
                } finally {
                    iterator.remove();
                }
            }
            closeLatch.countDown();
        }
    };
    private final Transport transport;
    private final ConnectionProfile defaultProfile;
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final CountDownLatch closeLatch = new CountDownLatch(1);
    private final DelegatingNodeConnectionListener connectionListener = new DelegatingNodeConnectionListener();

    public ClusterConnectionManager(Settings settings, Transport transport) {
        this(ConnectionProfile.buildDefaultConnectionProfile(settings), transport);
    }

    public ClusterConnectionManager(ConnectionProfile connectionProfile, Transport transport) {
        this.transport = transport;
        this.defaultProfile = connectionProfile;
    }

    @Override
    public void addListener(TransportConnectionListener listener) {
        this.connectionListener.addListener(listener);
    }

    @Override
    public void removeListener(TransportConnectionListener listener) {
        this.connectionListener.removeListener(listener);
    }

    @Override
    public void openConnection(DiscoveryNode node, ConnectionProfile connectionProfile, ActionListener<Transport.Connection> listener) {
        ConnectionProfile resolvedProfile = ConnectionProfile.resolveConnectionProfile(connectionProfile, defaultProfile);
        internalOpenConnection(node, resolvedProfile, listener);
    }

    /**
     * Connects to the given node, or acquires another reference to an existing connection to the given node if a connection already exists.
     *
     * @param connectionProfile   the profile to use if opening a new connection. Only used in tests, this is {@code null} in production.
     * @param connectionValidator a callback to validate the connection before it is exposed (e.g. to {@link #nodeConnected}).
     * @param listener            completed on the calling thread or the generic thread pool; if successful, completed with a {@link
     *                            Releasable} which will release this connection (and close it if no other references to it are held).
     */
    @Override
    public void connectToNode(
        DiscoveryNode node,
        @Nullable ConnectionProfile connectionProfile,
        ConnectionValidator connectionValidator,
        ActionListener<Releasable> listener
    ) throws ConnectTransportException {
        connectToNodeInternal(node, connectionProfile, connectionValidator, 0, listener);
    }

    /**
     * Connects to the given node, or acquires another reference to an existing connection to the given node if a connection already exists.
     * If a connection already exists but has been completely released (so it's in the process of closing) then this method will wait for
     * the close to complete and then try again (up to 10 times).
     */
    private void connectToNodeInternal(
        DiscoveryNode node,
        @Nullable ConnectionProfile connectionProfile,
        ConnectionValidator connectionValidator,
        int previousFailureCount,
        ActionListener<Releasable> listener
    ) throws ConnectTransportException {

        ConnectionProfile resolvedProfile = ConnectionProfile.resolveConnectionProfile(connectionProfile, defaultProfile);
        if (node == null) {
            listener.onFailure(new ConnectTransportException(null, "can't connect to a null node"));
            return;
        }

        if (connectingRefCounter.tryIncRef() == false) {
            listener.onFailure(new IllegalStateException("connection manager is closed"));
            return;
        }

        final Transport.Connection existingConnection = connectedNodes.get(node);
        if (existingConnection != null) {
            connectingRefCounter.decRef();
            if (existingConnection.tryIncRef()) {
                listener.onResponse(Releasables.releaseOnce(existingConnection::decRef));
                return;
            }

            final int failureCount = previousFailureCount + 1;
            if (failureCount < 10) {
                logger.trace("concurrent connect/disconnect for [{}] ([{}] failures), will try again", node, failureCount);
                existingConnection.addCloseListener(listener.delegateFailure((delegate, ignored) -> connectToNodeInternal(
                    node,
                    connectionProfile,
                    connectionValidator,
                    failureCount,
                    delegate)));
            } else {
                logger.warn("failed to connect to [{}] after [{}] attempts, giving up", node, failureCount);
                listener.onFailure(new ConnectTransportException(
                    node,
                    "concurrently connecting and disconnected even after [" + failureCount + "] attempts"));
            }
            return;
        }

        final ActionListener<Transport.Connection> acquiringListener = listener.delegateFailure((delegate, connection) -> {
            if (connection.tryIncRef()) {
                delegate.onResponse(Releasables.releaseOnce(connection::decRef));
            } else {
                assert false : "connection released before listeners notified";
                delegate.onFailure(new ConnectTransportException(node, "connection released before listeners notified"));
            }
        });

        final ListenableFuture<Transport.Connection> currentListener = new ListenableFuture<>();
        final ListenableFuture<Transport.Connection> existingListener = pendingConnections.putIfAbsent(node, currentListener);
        if (existingListener != null) {
            try {
                // wait on previous entry to complete connection attempt
                existingListener.addListener(acquiringListener);
            } finally {
                connectingRefCounter.decRef();
            }
            return;
        }

        currentListener.addListener(acquiringListener);

        final RunOnce releaseOnce = new RunOnce(connectingRefCounter::decRef);
        internalOpenConnection(node, resolvedProfile, ActionListener.wrap(
            conn -> connectionValidator.validate(conn, resolvedProfile, ActionListener.runAfter(ActionListener.wrap(
                ignored -> {
                    assert Transports.assertNotTransportThread("connection validator success");
                    try {
                        if (connectedNodes.putIfAbsent(node, conn) != null) {
                            logger.debug("existing connection to node [{}], closing new redundant connection", node);
                            IOUtils.closeWhileHandlingException(conn);
                        } else {
                            logger.debug("connected to node [{}]", node);
                            try {
                                connectionListener.onNodeConnected(node, conn);
                            } finally {
                                final Transport.Connection finalConnection = conn;
                                conn.addCloseListener(ActionListener.wrap(() -> {
                                    logger.trace("unregistering {} after connection close and marking as disconnected", node);
                                    connectedNodes.remove(node, finalConnection);
                                    connectionListener.onNodeDisconnected(node, conn);
                                }));
                            }
                        }
                    } finally {
                        ListenableFuture<Transport.Connection> future = pendingConnections.remove(node);
                        assert future == currentListener : "Listener in pending map is different than the expected listener";
                        releaseOnce.run();
                        future.onResponse(conn);
                    }
                }, e -> {
                    assert Transports.assertNotTransportThread("connection validator failure");
                    IOUtils.closeWhileHandlingException(conn);
                    failConnectionListener(node, releaseOnce, e, currentListener);
                }), conn::decRef)),
            e -> {
                assert Transports.assertNotTransportThread("internalOpenConnection failure");
                failConnectionListener(node, releaseOnce, e, currentListener);
            }));
    }

    /**
     * Returns a connection for the given node if the node is connected.
     * Connections returned from this method must not be closed. The lifecycle of this connection is
     * maintained by this connection manager
     *
     * @throws NodeNotConnectedException if the node is not connected
     * @see #connectToNode(DiscoveryNode, ConnectionProfile, ConnectionValidator, ActionListener)
     */
    @Override
    public Transport.Connection getConnection(DiscoveryNode node) {
        Transport.Connection connection = connectedNodes.get(node);
        if (connection == null) {
            throw new NodeNotConnectedException(node, "Node not connected");
        }
        return connection;
    }

    /**
     * Returns {@code true} if the node is connected.
     */
    @Override
    public boolean nodeConnected(DiscoveryNode node) {
        return connectedNodes.containsKey(node);
    }

    /**
     * Disconnected from the given node, if not connected, will do nothing.
     */
    @Override
    public void disconnectFromNode(DiscoveryNode node) {
        Transport.Connection nodeChannels = connectedNodes.remove(node);
        if (nodeChannels != null) {
            // if we found it and removed it we close
            nodeChannels.close();
        }
    }

    /**
     * Returns the number of nodes this manager is connected to.
     */
    @Override
    public int size() {
        return connectedNodes.size();
    }

    @Override
    public Set<DiscoveryNode> getAllConnectedNodes() {
        return Collections.unmodifiableSet(connectedNodes.keySet());
    }

    @Override
    public void close() {
        internalClose(true);
    }

    @Override
    public void closeNoBlock() {
        internalClose(false);
    }

    private void internalClose(boolean waitForPendingConnections) {
        assert Transports.assertNotTransportThread("Closing ConnectionManager");
        if (closing.compareAndSet(false, true)) {
            connectingRefCounter.decRef();
            if (waitForPendingConnections) {
                try {
                    closeLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    private void internalOpenConnection(DiscoveryNode node, ConnectionProfile connectionProfile,
                                        ActionListener<Transport.Connection> listener) {
        transport.openConnection(node, connectionProfile, listener.map(connection -> {
            assert Transports.assertNotTransportThread("internalOpenConnection success");
            try {
                connectionListener.onConnectionOpened(connection);
            } finally {
                connection.addCloseListener(ActionListener.wrap(() -> connectionListener.onConnectionClosed(connection)));
            }
            if (connection.isClosed()) {
                throw new ConnectTransportException(node, "a channel closed while connecting");
            }
            return connection;
        }));
    }

    private void failConnectionListener(
        DiscoveryNode node,
        RunOnce releaseOnce,
        Exception e,
        ListenableFuture<Transport.Connection> expectedListener
    ) {
        ListenableFuture<Transport.Connection> future = pendingConnections.remove(node);
        releaseOnce.run();
        if (future != null) {
            assert future == expectedListener : "Listener in pending map is different than the expected listener";
            future.onFailure(e);
        }
    }

    @Override
    public ConnectionProfile getConnectionProfile() {
        return defaultProfile;
    }

}
