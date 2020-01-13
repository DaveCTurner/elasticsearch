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
package org.elasticsearch.cluster.routing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class AutomaticPrimaryAllocationIT extends ESIntegTestCase {

    private static final String EMPTY_SOURCE_FACTORY_NAME = "empty-source";

    public static class TestPlugin extends Plugin implements ClusterPlugin {

        private static final Logger logger = LogManager.getLogger(TestPlugin.class);

        @Override
        public Map<String, PrimaryRecoverySourceFactory> getPrimaryRecoverySourceFactories() {
            return Collections.singletonMap(EMPTY_SOURCE_FACTORY_NAME,
                (indexMetaData, shardId) -> {
                    logger.info("getting empty primary recovery source for " + shardId);
                    return RecoverySource.EmptyStoreRecoverySource.INSTANCE;
                });
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestPlugin.class);
    }

    public void testPluginCanAllocateEmptyPrimariesOnPrimaryFailure() throws Exception {

        final String dataNode = internalCluster().startDataOnlyNode();

        createIndex("test", Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name", dataNode)
            .put(AllocationService.PRIMARY_RECOVERY_SOURCE_TYPE_SETTING.getKey(), EMPTY_SOURCE_FACTORY_NAME)
            .build());

        ensureGreen("test");

        final int numDocs = randomIntBetween(10, 100);
        List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            indexRequestBuilders.add(client().prepareIndex("test").setSource("num", i));
        }
        indexRandom(true, true, true, indexRequestBuilders);

        {
            final SearchResponse searchResponse = client().prepareSearch("test").setSize(0).setQuery(matchAllQuery()).get();
            assertThat(searchResponse.getFailedShards(), equalTo(0));
            assertThat(searchResponse.getSuccessfulShards(), greaterThan(0));
            assertThat(searchResponse.getHits().getTotalHits().value, greaterThan(0L));
        }

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(dataNode));

        assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder()
            .putNull(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name")));

        ensureGreen("test");

        {
            final SearchResponse searchResponse = client().prepareSearch("test").setSize(0).setQuery(matchAllQuery()).get();
            assertThat(searchResponse.getFailedShards(), equalTo(0));
            assertThat(searchResponse.getSuccessfulShards(), greaterThan(0));
            assertHitCount(searchResponse, 0);
        }
    }

}
