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

package org.elasticsearch.index.engine;

import org.elasticsearch.action.admin.indices.flush.SyncedFlushResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;

import static org.hamcrest.Matchers.equalTo;

@TestLogging("_root:DEBUG,org.elasticsearch.index.engine:TRACE")
public class ReplicationIT extends ESIntegTestCase {
    public void testEnforceSafeAccessOnReplica() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        client().admin().indices().prepareCreate("index")
            .setSettings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put("index.refresh_interval", -1))
            .setWaitForActiveShards(2)
            .get();
        ensureGreen("index");

        // Step 1: prepare
        client().prepareIndex("index", "doc", "1").setSource("{}", XContentType.JSON).get();
        client().prepareDelete("index", "doc", "1").get();

        // Step 2, 3, 4 happen concurrently
        Thread t2 = new Thread(() -> {
            client().prepareDelete("index", "doc", "1").get();
        });

        Thread t3 = new Thread(() -> {
            client().prepareIndex("index", "doc", "1").setSource("{}", XContentType.JSON).get();
        });

        t2.start();
        Thread.sleep(1000);
        t3.start();
        Thread.sleep(1000);

        // Step 4
        client().admin().indices().prepareRefresh("index").get();
        // Index an auto-generated id document: POST /i/_doc?refresh {}
        client().prepareIndex("index", "doc", null).setSource("{}", XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        t2.join();
        t3.join();

        // Step5: refresh and issue synced-flush
        client().admin().indices().prepareRefresh("index").get();
        SyncedFlushResponse syncedFlush = client().admin().indices().prepareSyncedFlush("index").get();
        assertThat(syncedFlush.successfulShards(), equalTo(2)); // Expect a replica out of sync.
    }
}
