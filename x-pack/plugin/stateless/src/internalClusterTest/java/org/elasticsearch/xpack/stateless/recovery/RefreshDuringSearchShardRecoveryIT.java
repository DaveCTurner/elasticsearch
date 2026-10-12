/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Reproduces a search shard missing the commit created by {@code refresh=true}.
 * <p>
 * {@code StatelessCommitService#onCommitCreation} captures the search shards that should receive the new-commit
 * notification before that commit is visible to recovery. The temporary sleep added there opens the window described in
 * <a href="https://github.com/elastic/elasticsearch/issues/159479">elasticsearch#159479</a>: the search shard is allocated
 * during the sleep, recovers the previous (empty) commit, and is absent from the already-captured recipient list. The
 * indexed document stays invisible on that shard, which is the failure reported in elasticsearch-serverless#7911.
 * {@code refresh=true} then blocks on the generation the missed notification was supposed to publish.
 * <p>
 * The sleep is a local reproduction aid and is not meant to be committed.
 */
public class RefreshDuringSearchShardRecoveryIT extends AbstractStatelessPluginIntegTestCase {

    public void testRefreshTrueDocumentNotVisibleWhenSearchShardRecoversDuringCommit() throws Exception {
        startMasterOnlyNode();
        startIndexNode(
            Settings.builder()
                .put(disableIndexingDiskAndMemoryControllersNodeSettings())
                // Keep the missed commit from being re-sent by a later upload notification, which looks up recipients afresh.
                .put(StatelessCommitService.STATELESS_UPLOAD_VBCC_MAX_AGE.getKey(), TimeValue.timeValueHours(1))
                .put(StatelessCommitService.STATELESS_UPLOAD_MONITOR_INTERVAL.getKey(), TimeValue.timeValueHours(1))
                .build()
        );
        startSearchNode();

        // No replicas yet, so index creation does not install the refresh block and the following refresh can enter
        // onCommitCreation immediately, while there is still no search shard to notify.
        final String indexName = "test-refresh-during-recovery";
        assertAcked(
            prepareCreate(indexName).setSettings(
                indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
            )
        );
        ensureGreen(indexName);

        final var indexed = prepareIndex(indexName).setId("1")
            .setSource("field", "value")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .execute();

        try {
            // onCommitCreation sleeps for 30s after snapshotting an empty recipient list and before the new commit is
            // visible to recovery. Allocate the search shard in that window so it recovers the previous commit.
            Thread.sleep(5_000);
            updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
            ensureGreen(indexName);

            assertResponse(
                prepareSearch(indexName).setSize(0),
                response -> assertThat(
                    "search shard started from the pre-refresh commit",
                    response.getHits().getTotalHits().value(),
                    equalTo(0L)
                )
            );

            // Past the end of the production sleep. The notification was sent to the recipient list captured before this
            // search shard existed, so refresh=true is still waiting for a generation that never arrives.
            assertBusy(() -> assertThat("refresh=true acknowledged the write", indexed.isDone(), is(true)), 40, TimeUnit.SECONDS);
            assertThat(indexed.actionGet().getId(), equalTo("1"));
            assertHitCount(prepareSearch(indexName), 1);
        } finally {
            // A later commit reads the routing table again and notifies the search shard, which releases the refresh
            // stuck on the missed generation so the test cluster can shut down.
            if (indexed.isDone() == false) {
                prepareIndex(indexName).setId("unstick")
                    .setSource("field", "unstick")
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .get();
                indexed.actionGet(TimeValue.timeValueSeconds(30));
            }
        }
    }
}
