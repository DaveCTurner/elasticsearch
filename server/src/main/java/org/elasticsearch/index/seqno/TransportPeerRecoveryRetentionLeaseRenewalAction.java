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
package org.elasticsearch.index.seqno;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.seqno.PeerRecoveryRetentionLeaseRenewalAction.Request;
import org.elasticsearch.index.seqno.PeerRecoveryRetentionLeaseRenewalAction.Response;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class TransportPeerRecoveryRetentionLeaseRenewalAction extends HandledTransportAction<Request, Response> {

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final Logger logger = LogManager.getLogger(TransportPeerRecoveryRetentionLeaseRenewalAction.class);

    @Inject
    public TransportPeerRecoveryRetentionLeaseRenewalAction(
        final TransportService transportService,
        final ActionFilters actionFilters,
        final ClusterService clusterService,
        final IndicesService indicesService) {

        super(PeerRecoveryRetentionLeaseRenewalAction.ACTION_NAME, transportService, actionFilters, Request::new);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final IndexShard indexShard = indicesService.indexServiceSafe(request.shardId.getIndex()).getShard(request.shardId.getId());
        indexShard.renewPeerRecoveryRetentionLeaseForRemote(request.nodeId, request.minimumPeerRecoverySeqNo);
        listener.onResponse(new Response());
    }

    public void renewPeerRecoveryRetentionLease(ShardId shardId, long minimumSeqNoForPeerRecovery) {
        // TODO do we care about cluster blocks? do we care about chasing a relocating primary?

        final ClusterState clusterState = clusterService.state();
        final ShardRouting primaryShardRouting = clusterState.getRoutingNodes().activePrimary(shardId);
        if (primaryShardRouting == null) {
            logger.debug("{} no primary shard when renewing peer-recovery retention lease", shardId);
            return;
        }
        final DiscoveryNode discoveryNode = clusterState.nodes().get(primaryShardRouting.currentNodeId());
        if (discoveryNode == null) {
            logger.debug("{} discovery node not found when renewing peer-recovery retention lease on {}", shardId, primaryShardRouting);
            return;
        }

        transportService.sendRequest(discoveryNode, PeerRecoveryRetentionLeaseRenewalAction.ACTION_NAME,
            new Request(shardId, transportService.getLocalNode().getId(), minimumSeqNoForPeerRecovery),
            new TransportResponseHandler<Response>() {
                @Override
                public void handleResponse(Response response) {
                    // empty response
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.debug(new ParameterizedMessage("{} exception when renewing peer-recovery retention lease on {}",
                        shardId, discoveryNode), exp);
                }

                @Override
                public String executor() {
                    return Names.SAME;
                }

                @Override
                public Response read(StreamInput in) throws IOException {
                    return new Response(in);
                }
            });
    }
}
