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
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.snapshots.SnapshotsService.SNAPSHOT_CACHE_SIZE_SETTING;

/**
 * Keeps track of the nodes in the cluster and whether they do or do not have a frozen-tier shared cache, because we can only allocate
 * frozen-tier shards to such nodes.
 */
public class FrozenCacheSizeService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(FrozenCacheSizeService.class);

    private final Object mutex = new Object();
    private final SetOnce<Client> clientRef = new SetOnce<>();
    private final SetOnce<ClusterService> clusterServiceRef = new SetOnce<>();

    /**
     * The known data nodes, along with an indication whether they have a frozen cache or not
     */
    private final Map<DiscoveryNode, NodeStateHolder> nodeStates = new HashMap<>();

    public void initialize(Client client, ClusterService clusterService) {
        clientRef.set(Objects.requireNonNull(client));
        clusterServiceRef.set(Objects.requireNonNull(clusterService));
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        assert clientRef.get() != null;
        final ClusterService clusterService = clusterServiceRef.get();
        assert clusterService != null;

        if (clusterService.localNode().isMasterNode() == false) {
            clusterService.removeListener(this);
            return;
        }

        final Set<DiscoveryNode> nodes = StreamSupport.stream(event.state().nodes().getDataNodes().values().spliterator(), false)
                .map(c -> c.value).collect(Collectors.toSet());

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

                runnables.add(() -> clientRef.get().admin().cluster()
                        .prepareNodesInfo(newNode.getId())
                        .clear()
                        .setSettings(true)
                        .execute(new ActionListener<NodesInfoResponse>() {
                    @Override
                    public void onResponse(NodesInfoResponse nodesInfoResponse) {
                        if (nodesInfoResponse.getNodesMap().isEmpty() == false) {
                            assert nodesInfoResponse.hasFailures() == false;
                            assert nodesInfoResponse.getNodes().size() == 1;
                            final NodeInfo nodeInfo = nodesInfoResponse.getNodes().get(0);
                            assert nodeInfo.getNode().getId().equals(newNode.getId());
                            final boolean hasFrozenCache = SNAPSHOT_CACHE_SIZE_SETTING.get(nodeInfo.getSettings()).getBytes() > 0;
                            logger.trace("updating entry for {} to {}", newNode, hasFrozenCache);
                            updateEntry(hasFrozenCache ? NodeState.HAS_CACHE : NodeState.NO_CACHE);
                            clusterService.getRerouteService().reroute("frozen cache state retrieved", Priority.LOW,
                                    ActionListener.wrap(() -> {}));
                        } else if (nodesInfoResponse.hasFailures()) {
                            assert nodesInfoResponse.failures().size() == 1;
                            removeEntry(nodesInfoResponse.failures().get(0));
                        } else {
                            removeEntry(new ElasticsearchException("node not found"));
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        removeEntry(e);
                    }

                    private void updateEntry(NodeState nodeState) {
                        assert nodeStateHolder.nodeState == NodeState.FETCHING : "already set for " + newNode;
                        assert nodeState != NodeState.FETCHING : "cannot set " + newNode + " to " + nodeState;
                        nodeStateHolder.nodeState = nodeState;
                    }

                    private void removeEntry(Exception e) {
                        assert nodeStateHolder.nodeState == null : "already set for " + newNode;
                        logger.debug(new ParameterizedMessage("failed to retrieve node settings from node {}", newNode), e);
                        synchronized (mutex) {
                            nodeStates.remove(newNode, nodeStateHolder);
                        }
                        // will retry on a subsequent cluster state update
                        // TBD should we retry more enthusiastically? should we stop retrying after a while?
                    }
                }));
            }
        }

        runnables.forEach(Runnable::run);
    }

    @Nullable // if state not known yet
    public Boolean hasFrozenCache(DiscoveryNode discoveryNode) {
        final NodeStateHolder nodeStateHolder;
        synchronized (mutex) {
            nodeStateHolder = nodeStates.get(discoveryNode);
        }


        return nodeStateHolder == null ? null : nodeStateHolder.nodeState == NodeState.HAS_CACHE;
    }

    /**
     * Records whether a particular data node does/doesn't have a nonzero frozen cache.
     */
    private static class NodeStateHolder {
        @Nullable // if not known yet
        volatile NodeState nodeState = NodeState.FETCHING;
    }

    private enum NodeState {
        FETCHING,
        HAS_CACHE,
        NO_CACHE,
        FAILED
    }
}
