/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

public interface InternalTestCluster {
    void setBootstrapMasterNodeIndex(int bootstrapMasterNodeIndex);

    String getClusterName();

    String[] getNodeNames();

    Collection<Class<? extends Plugin>> getPlugins();

    void ensureAtLeastNumDataNodes(int n);

    void ensureAtMostNumDataNodes(int n) throws IOException;

    Client client();

    Client dataNodeClient();

    Client masterClient();

    Client nonMasterClient();

    Client coordOnlyNodeClient();

    String startCoordinatingOnlyNode(Settings settings);

    Client client(String nodeName);

    Client smartClient();

    void assertConsistentHistoryBetweenTranslogAndLuceneIndex() throws IOException;

    void assertNoInFlightDocsInEngine() throws Exception;

    void assertSeqNos() throws Exception;

    void assertSameDocIdsOnShards() throws Exception;

    void wipePendingDataDirectories();

    void validateClusterFormed();

    void wipeIndices(String... indices);

    ClusterService clusterService();

    ClusterService clusterService(@Nullable String node);

    <T> Iterable<T> getInstances(Class<T> clazz);

    <T> Iterable<T> getDataNodeInstances(Class<T> clazz);

    <T> T getCurrentMasterNodeInstance(Class<T> clazz);

    <T> Iterable<T> getDataOrMasterNodeInstances(Class<T> clazz);

    <T> T getInstance(Class<T> clazz, String nodeName);

    <T> T getInstance(Class<T> clazz, DiscoveryNodeRole role);

    <T> T getDataNodeInstance(Class<T> clazz);

    <T> T getAnyMasterNodeInstance(Class<T> clazz);

    <T> T getInstance(Class<T> clazz);

    Settings dataPathSettings(String node);

    int size();

    InetSocketAddress[] httpAddresses();

    boolean stopRandomDataNode() throws IOException;

    boolean stopNode(String nodeName) throws IOException;

    void stopCurrentMasterNode() throws IOException;

    void stopRandomNonMasterNode() throws IOException;

    Collection<Path> configPaths();

    void restartRandomDataNode() throws Exception;

    void restartRandomDataNode(InternalTestCluster.RestartCallback callback) throws Exception;

    void restartNode(String nodeName) throws Exception;

    void restartNode(String nodeName, InternalTestCluster.RestartCallback callback) throws Exception;

    void fullRestart() throws Exception;

    void rollingRestart(InternalTestCluster.RestartCallback callback) throws Exception;

    void fullRestart(InternalTestCluster.RestartCallback callback) throws Exception;

    String getMasterName();

    String getMasterName(@Nullable String viaNode);

    String getRandomNodeName();

    String getNodeNameThat(Predicate<Settings> predicate);

    Set<String> nodesInclude(String index);

    String startNode();

    String startNode(Settings.Builder settings);

    String startNode(Settings settings);

    List<String> startNodes(int numOfNodes);

    List<String> startNodes(int numOfNodes, Settings settings);

    List<String> startNodes(Settings... extraSettings);

    List<String> startMasterOnlyNodes(int numNodes);

    List<String> startMasterOnlyNodes(int numNodes, Settings settings);

    List<String> startDataOnlyNodes(int numNodes);

    List<String> startDataOnlyNodes(int numNodes, Settings settings);

    String startMasterOnlyNode();

    String startMasterOnlyNode(Settings settings);

    String startDataOnlyNode();

    String startDataOnlyNode(Settings settings);

    void closeNonSharedNodes(boolean wipeData) throws IOException;

    int numDataAndMasterNodes();

    int numMasterNodes();

    int numDataNodes();

    void setDisruptionScheme(ServiceDisruptionScheme scheme);

    void clearDisruptionScheme();

    // synchronized to prevent concurrently modifying the cluster.
    void clearDisruptionScheme(boolean ensureHealthyCluster);

    Iterable<Client> getClients();

    NamedWriteableRegistry getNamedWriteableRegistry();

    Settings getDefaultSettings();

    void assertRequestsFinished();

    /**
     * An abstract class that is called during {@link #rollingRestart(InternalTestCluster.RestartCallback)}
     * and / or {@link #fullRestart(InternalTestCluster.RestartCallback)} to execute actions at certain
     * stages of the restart.
     */
    class RestartCallback {

        /**
         * Executed once the give node name has been stopped.
         */
        public Settings onNodeStopped(String nodeName) throws Exception {
            return Settings.EMPTY;
        }

        public void onAllNodesStopped() throws Exception {}

        /**
         * Executed for each node before the {@code n + 1} node is restarted. The given client is
         * an active client to the node that will be restarted next.
         */
        public void doAfterNodes(int n, Client client) throws Exception {}

        /**
         * If this returns <code>true</code> all data for the node with the given node name will be cleared including
         * gateways and all index data. Returns <code>false</code> by default.
         */
        public boolean clearData(String nodeName) {
            return false;
        }

        /** returns true if the restart should also validate the cluster has reformed */
        public boolean validateClusterForming() {
            return true;
        }
    }
}
