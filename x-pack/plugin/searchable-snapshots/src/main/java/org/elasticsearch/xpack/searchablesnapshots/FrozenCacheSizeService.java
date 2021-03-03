/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.snapshots.SnapshotsService.SNAPSHOT_CACHE_SIZE_SETTING;

/**
 * Keeps track of the nodes in the cluster and whether they do or do not have a frozen-tier shared cache, because we can only allocate
 * frozen-tier shards to such nodes.
 */
public class FrozenCacheSizeService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(FrozenCacheSizeService.class);

    private final Object mutex = new Object();

    /**
     * The known data nodes, along with an indication whether they have a frozen cache or not
     */
    private final Map<DiscoveryNode, NodeStateHolder> nodeStates = new HashMap<>();

    /**
     * Whether this node is currently the master; if not then we stop retrying any failed fetches
     */
    private volatile boolean isElectedMaster;

    public void initialize(ClusterService clusterService) {
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        isElectedMaster = event.localNodeMaster();
        if (isElectedMaster == false) {
            clear();
        }
    }

    public void updateNodes(Client client, Set<DiscoveryNode> nodes, RerouteService rerouteService) {
        if (isElectedMaster == false) {
            return;
        }

        final List<Runnable> runnables;
        synchronized (mutex) {
            // clean up nodes that left the cluster
            nodeStates.keySet().removeIf(n -> nodes.contains(n) == false);

            // skip nodes with known state
            nodes.removeAll(nodeStates.keySet());

            // reach out to any new nodes
            runnables = new ArrayList<>(nodes.size());
            for (DiscoveryNode newNode : nodes) {
                final NodeStateHolder nodeStateHolder = new NodeStateHolder();
                final NodeStateHolder prevState = nodeStates.put(newNode, nodeStateHolder);
                assert prevState == null;

                logger.trace("fetching frozen cache state for {}", newNode);
                runnables.add(new AsyncNodeFetch(client, rerouteService, newNode, nodeStateHolder));
            }
        }

        runnables.forEach(Runnable::run);
    }

    public NodeState getNodeState(DiscoveryNode discoveryNode) {
        final NodeStateHolder nodeStateHolder;
        synchronized (mutex) {
            nodeStateHolder = nodeStates.get(discoveryNode);
        }
        return nodeStateHolder == null ? NodeState.UNKNOWN : nodeStateHolder.nodeState;
    }

    public boolean isFetching() {
        synchronized (mutex) {
            return nodeStates.values().stream().anyMatch(nodeStateHolder -> nodeStateHolder.nodeState == NodeState.FETCHING);
        }
    }

    public void clear() {
        synchronized (mutex) {
            nodeStates.clear();
        }
    }

    private class AsyncNodeFetch extends AbstractRunnable {

        private final Client client;
        private final RerouteService rerouteService;
        private final DiscoveryNode discoveryNode;
        private final NodeStateHolder nodeStateHolder;

        AsyncNodeFetch(Client client, RerouteService rerouteService, DiscoveryNode discoveryNode, NodeStateHolder nodeStateHolder) {
            this.client = client;
            this.rerouteService = rerouteService;
            this.discoveryNode = discoveryNode;
            this.nodeStateHolder = nodeStateHolder;
        }

        @Override
        protected void doRun() {
            client.admin()
                .cluster()
                .prepareNodesInfo(discoveryNode.getId())
                .clear()
                .setSettings(true)
                .execute(new ActionListener<NodesInfoResponse>() {

                    @Override
                    public void onResponse(NodesInfoResponse nodesInfoResponse) {
                        if (nodesInfoResponse.getNodesMap().isEmpty() == false) {
                            assert nodesInfoResponse.hasFailures() == false;
                            assert nodesInfoResponse.getNodes().size() == 1;
                            final NodeInfo nodeInfo = nodesInfoResponse.getNodes().get(0);
                            assert nodeInfo.getNode().getId().equals(discoveryNode.getId());
                            final boolean hasFrozenCache = SNAPSHOT_CACHE_SIZE_SETTING.get(nodeInfo.getSettings()).getBytes() > 0;
                            updateEntry(hasFrozenCache ? NodeState.HAS_CACHE : NodeState.NO_CACHE);
                            rerouteService.reroute("frozen cache state retrieved", Priority.LOW, ActionListener.wrap(() -> {}));
                        } else if (nodesInfoResponse.hasFailures()) {
                            assert nodesInfoResponse.failures().size() == 1;
                            recordFailure(nodesInfoResponse.failures().get(0));
                        } else {
                            recordFailure(new ElasticsearchException("node not found"));
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        recordFailure(e);
                    }
                });
        }

        @Override
        public void onFailure(Exception e) {
            updateEntry(NodeState.FAILED);
        }

        private void recordFailure(Exception e) {
            logger.debug(new ParameterizedMessage("failed to retrieve node settings from node {}", discoveryNode), e);
            final boolean shouldRetry;
            synchronized (mutex) {
                shouldRetry = isElectedMaster && nodeStates.get(discoveryNode) == nodeStateHolder;
            }
            if (shouldRetry) {
                // failure is likely something like a CircuitBreakingException, so there's no sense in an immediate retry
                client.threadPool().scheduleUnlessShuttingDown(TimeValue.timeValueSeconds(1), ThreadPool.Names.SAME, AsyncNodeFetch.this);
            } else {
                updateEntry(NodeState.FAILED);
            }
        }

        private void updateEntry(NodeState nodeState) {
            assert nodeStateHolder.nodeState == NodeState.FETCHING : discoveryNode + " already set to " + nodeStateHolder.nodeState;
            assert nodeState != NodeState.FETCHING : "cannot set " + discoveryNode + " to " + nodeState;
            logger.trace("updating entry for {} to {}", discoveryNode, nodeState);
            nodeStateHolder.nodeState = nodeState;
        }

    }

    /**
     * Records whether a particular data node does/doesn't have a nonzero frozen cache.
     */
    private static class NodeStateHolder {
        volatile NodeState nodeState = NodeState.FETCHING;
    }

    public enum NodeState {
        UNKNOWN,
        FETCHING,
        HAS_CACHE,
        NO_CACHE,
        FAILED,
    }

}
