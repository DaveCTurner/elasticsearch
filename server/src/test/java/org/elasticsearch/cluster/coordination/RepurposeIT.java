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
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.cluster.routing.UnassignedInfo.Reason.DANGLING_INDEX_IMPORTED;
import static org.elasticsearch.node.Node.INITIAL_STATE_TIMEOUT_SETTING;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0, autoManageMasterNodes = false)
public class RepurposeIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockTransportService.TestPlugin.class);
    }

    public void testTemporaryRepurposeAsDataNodeDoesNotLoseIndexMetadata() throws Exception {
        // Master-ineligible data nodes do not persist the index metadata for indices that have no shards on that node. However we can
        // change the roles of nodes when they are restarted, so must be careful not to allow a situation where a master node seems to have
        // the latest cluster state but really it is incomplete because it was written when the node was a data node. We do not strongly
        // guarantee that the voting configuration only contains master-eligible nodes, and with failures at exactly the right times we
        // risk a node winning an election with an incomplete cluster state.
        //
        // This test engineers such a situation, creating a cluster of three master-eligible data nodes, restarting one of them as a
        // master-ineligible data node and then immediately back as a master-eligible data node again so that there is not time for it to
        // be removed from the voting configuration. It ensures that this node does not publish its cluster state since this state is
        // missing some indices.

        internalCluster().setBootstrapMasterNodeIndex(0);
        final String masterNodeName = internalCluster().startNode();
        final String restartNodeName = internalCluster().startNode();
        final String otherNodeName = internalCluster().startNode();

        assertThat(internalCluster().getMasterName(), is(masterNodeName));

        // do not assign a shard to [restartNodeName] so that when it becomes a data node it does not persist the index metadata
        createIndex("test", Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, between(1, 3))
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, between(0, 1))
            .put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", restartNodeName)
            .build());
        ensureGreen("test");

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        // ensure that we don't allow [restartNodeName] to become the leader with an incomplete cluster state that it then repairs through
        // the magic of dangling indices
        internalCluster().getInstance(ClusterService.class, otherNodeName).addListener(event -> {
            for (ShardRouting shardRouting : event.state().routingTable().shardsWithState(ShardRoutingState.UNASSIGNED)) {
                assertThat(shardRouting.toString(), shardRouting.unassignedInfo().getReason(), not(DANGLING_INDEX_IMPORTED));
            }
        });

        // restart [restartNodeName] as a data-only node but block the master's attempt to remove it from the voting configuration
        logger.info("--> restarting [{}] as a data-only node", restartNodeName);
        internalCluster().restartNode(restartNodeName, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                ensureStableCluster(2);

                assertThat(internalCluster().getMasterName(), is(masterNodeName));

                // block cluster state publications after the restarted node has joined, in particular the followup reconfiguration
                final MockTransportService masterNodeTransportService
                    = (MockTransportService) internalCluster().getMasterNodeInstance(TransportService.class);
                masterNodeTransportService.addSendBehavior(internalCluster().getInstance(TransportService.class, otherNodeName),
                    (connection, requestId, action, request, options) -> {
                        if (action.equals(PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME) && countDownLatch.getCount() == 0) {
                            throw new ElasticsearchException("failing publication");
                        }
                        connection.sendRequest(requestId, action, request, options);
                    });

                internalCluster().getMasterNodeInstance(ClusterService.class).addListener(event -> {
                    if (event.state().nodes().getSize() == 3) {
                        logger.info("--> [{}] has rejoined, blocking publications", restartNodeName);
                        countDownLatch.countDown();
                    }
                });

                return Settings.builder()
                    .put(Node.NODE_MASTER_SETTING.getKey(), false)
                    .build();
            }

            @Override
            public boolean validateClusterForming() {
                return false; // the master is blocked from publishing so the cluster falls apart
            }
        });

        countDownLatch.await();
        // [restartNodeName] has applied a cluster state (i.e. persisted it) but without the index metadata for [test] since as a data node
        // it doesn't have any shards of the [test] index assigned

        // shut down the old master since it might have locally applied an even fresher state than the other two nodes, so we need to
        // ignore it
        logger.info("--> restarting master [{}]", masterNodeName);
        internalCluster().restartNode(masterNodeName, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                // now we can restart [restartNodeName] as a master again; it has persisted a cluster state version fresher than
                // [otherNodeName] but its cluster state is incomplete
                logger.info("--> restarting [{}] as a master-eligible node", restartNodeName);
                internalCluster().restartNode(restartNodeName, new InternalTestCluster.RestartCallback() {
                    @Override
                    public boolean validateClusterForming() {
                        return false; // neither node may win an election
                    }
                });
                return Settings.EMPTY;
            }
        });

        ensureGreen("test");
    }

}
