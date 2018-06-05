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

import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.PublishClusterStateAction;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.NetworkDisruption.Bridge;
import org.elasticsearch.test.disruption.NetworkDisruption.DisruptedLinks;
import org.elasticsearch.test.disruption.NetworkDisruption.NetworkDelay;
import org.elasticsearch.test.disruption.NetworkDisruption.NetworkDisconnect;
import org.elasticsearch.test.disruption.NetworkDisruption.NetworkLinkDisruptionType;
import org.elasticsearch.test.disruption.NetworkDisruption.TwoPartitions;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.disruption.SlowClusterStateProcessing;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

@ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 0, numClientNodes = 0, supportsDedicatedMasters = false)
@TestLogging("org.elasticsearch.cluster:TRACE,org.elasticsearch.discovery:TRACE")
public class IndexingWithMappingUpdateIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
            .put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), TimeValue.ZERO)
            .put(DiscoverySettings.COMMIT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(1)).build();
    }

    /**
     title Replication Sequence, case 1

     Client -> Primary: doc1
     Primary -> Master: Update mapping
     Master -> Primary: mapping update
     Primary -> Master: ack mapping update
     Primary -> Primary: updates local mapping

     Client -> Primary: doc2
     Primary -> Replica: doc2


     Master -> Replica: mapping update
     Replica -> Master: ack mapping update
     Master -> Primary: all nodes acked
     Primary -> Replica: doc1
     */
    public void testRaiseConditionOnSecondDocWithMappingUpdate() throws Exception {

        final String index = "test";
        final String type = "doc";

        // dedicated master + 1 primary + 1 replica
        final String masterNodeName = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(2);

        assertAcked(admin().indices().prepareCreate(index).setSettings(Settings.builder()
                    .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                    .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)));

        ensureGreen();

        final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        final IndexShardRoutingTable indexShardRoutingTable = clusterState.getRoutingTable().index(index).shard(0);
        final String primaryNodeId = indexShardRoutingTable.primaryShard().currentNodeId();
        String primaryNodeName = clusterState.nodes().get(primaryNodeId).getName();
        assertThat(indexShardRoutingTable.replicaShards().size(), is(1));
        final String replicaNodeId = indexShardRoutingTable.replicaShards().get(0).currentNodeId();
        String replicaNodeName = clusterState.nodes().get(replicaNodeId).getName();

        final ServiceDisruptionScheme serviceDisruptionScheme = new SlowClusterStateProcessing(replicaNodeName, random());
        setDisruptionScheme(serviceDisruptionScheme);
        serviceDisruptionScheme.startDisrupting();

        final int threadCount = 10;
        final int docCount = 100;
        final CountDownLatch readyLatch = new CountDownLatch(threadCount);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final AtomicInteger nextDocIndex = new AtomicInteger();
        final List<IndexResponse> indexResponses = new CopyOnWriteArrayList<>();

        final Client client = internalCluster().client(primaryNodeName);

        final Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            final int threadNumber = i;
            threads[i] = new Thread(() -> {
                readyLatch.countDown();
                try {
                    startLatch.await();
                    Thread.sleep(threadNumber * 100);
                } catch (InterruptedException ignored) {
                    // ignore
                }

                int docIndex;
                while ((docIndex = nextDocIndex.incrementAndGet()) < docCount) {
                    logger.info("--> thread {} indexing doc {}", threadNumber, docIndex);
                    IndexResponse indexResponse = client.prepareIndex(index, type, "doc")
                        .setSource("{ \"f\": \"normal\"}", XContentType.JSON).get();
                    logger.info("--> thread {} indexing doc {} done", threadNumber, docIndex);
                    indexResponses.add(indexResponse);
                }
            });
        }

        for (final Thread thread : threads) {
            thread.start();
        }
        readyLatch.await();
        startLatch.countDown();

        for (final Thread thread : threads) {
            thread.join();
        }
        serviceDisruptionScheme.stopDisrupting();

        List<String> failureMessages = indexResponses.stream()
            .map(r -> r.getShardInfo().getFailures())
            .flatMap(Arrays::stream)
            .map(failure -> failure.nodeId() + ": " + failure.reason())
            .collect(Collectors.toList());
        assertThat(failureMessages.toString(), failureMessages, hasSize(0));
    }
}
