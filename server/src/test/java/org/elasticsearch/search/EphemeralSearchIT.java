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
package org.elasticsearch.search;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.nio.file.Path;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class EphemeralSearchIT extends ESIntegTestCase {

    @TestLogging(reason = "nocommit", value = "org.elasticsearch.action.search:TRACE")
    public void testSearchEphemeralShard() throws InterruptedException {

        final String repoName = randomAlphaOfLength(10);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        final Path repo = randomRepoPath();
        assertAcked(client().admin().cluster().preparePutRepository(repoName)
            .setType("fs")
            .setSettings(Settings.builder()
                .put("location", repo)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        createIndex(indexName);
        indexRandom(true, client().prepareIndex(indexName).setSource("foo", "bar"));
        flushAndRefresh(indexName);

        CreateSnapshotResponse createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true).get();
        final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));

        final IndexMetaData indexMetaData = client().admin().cluster().prepareState().get().getState().metaData().index(indexName);

        assertAcked(client().admin().indices().prepareDelete(indexName));

        final AllocationService allocationService = internalCluster().getInstance(AllocationService.class, internalCluster().getMasterName());
        final ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());

        final CountDownLatch indexCreatedLatch = new CountDownLatch(1);
        clusterService.submitStateUpdateTask("test",
            new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return allocationService.reroute(ClusterState.builder(currentState).metaData(MetaData.builder(currentState.metaData())
                        .put(IndexMetaData.builder(indexMetaData).settings(Settings.builder()
                            .put(indexMetaData.getSettings())
                            .put(OperationRouting.EPHEMERAL_INDEX_REPOSITORY_SETTING.getKey(), repoName)
                            .put(OperationRouting.EPHEMERAL_INDEX_SNAPSHOT_SETTING.getKey(), snapshotName)
                            .build()
                        ))).build(), "adding ephemeral index");
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    indexCreatedLatch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    throw new AssertionError(source, e);
                }
            });

        indexCreatedLatch.await();

        internalCluster().client().prepareSearch(indexName).get();

    }
}
