/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

/**
 * Exercises the elasticsearch-serverless#7911 sequence: {@code refresh=true} is acknowledged while the search shard
 * object does not exist yet, and the new-commit notification is sent to a recipient list captured before that shard
 * was allocated.
 * <p>
 * Two temporary production sleeps open that window. {@code StatelessCommitService#onCommitCreation} holds the new
 * commit invisible to recovery after capturing recipients. {@code IndexService#createShard} holds the search shard out
 * of {@code IndexService} until after the refresh request has been acknowledged, so
 * {@code TransportUnpromotableShardRefreshAction} takes its "shard not yet created, respond OK" path. Recovery then
 * registers and is handed the commit that {@code refresh=true} just created. The sleeps and info logs are a local
 * reproduction aid and are not meant to be committed.
 */
public class RefreshDuringSearchShardRecoveryIT extends AbstractStatelessPluginIntegTestCase {

    public void testRefreshTrueReturnsBeforeSearchCopyCanSeeTheDocument() throws Exception {
        startMasterOnlyNode();
        startIndexNode(
            Settings.builder()
                .put(disableIndexingDiskAndMemoryControllersNodeSettings())
                // Keep a missed commit from being re-sent by a later upload notification, which looks up recipients afresh.
                .put(StatelessCommitService.STATELESS_UPLOAD_VBCC_MAX_AGE.getKey(), TimeValue.timeValueHours(1))
                .put(StatelessCommitService.STATELESS_UPLOAD_MONITOR_INTERVAL.getKey(), TimeValue.timeValueHours(1))
                .build()
        );
        startSearchNode();

        // No replicas yet, so index creation does not install the refresh block and the document commit can be created
        // while there is still no search shard to notify.
        final String indexName = "test-refresh-during-recovery";
        assertAcked(
            prepareCreate(indexName).setSettings(
                indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
            )
        );
        ensureGreen(indexName);

        final long startedNanos = System.nanoTime();
        final var indexed = prepareIndex(indexName).setId("1")
            .setSource("field", "value")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .execute();

        try {
            // The document commit is invisible for 8s and its recipient list is empty. Allocate the search shard in that
            // window. Publication of the shard object is delayed a further 20s, so the refresh request is acknowledged
            // while getShardOrNull is still null.
            Thread.sleep(2_000);
            updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);

            // 7911: the bulk returns. This must not wait for the 20s search-shard publication delay.
            assertThat(indexed.actionGet(TimeValue.timeValueSeconds(15)).getId(), equalTo("1"));
            logger.info("TEMPORARY refresh=true returned after {}ms", (System.nanoTime() - startedNanos) / 1_000_000L);

            assertResponse(prepareSearch(indexName).setSize(0).setAllowPartialSearchResults(true), response -> {
                logSearch("immediately after refresh=true", response);
                assertThat(
                    "refresh=true returned but the document is not searchable: " + searchSummary(response),
                    response.getHits().getTotalHits().value(),
                    equalTo(1L)
                );
            });
        } finally {
            // Let the delayed search shard finish starting so the cluster can shut down, and record whether it ever
            // became able to see the document.
            try {
                ensureGreen(indexName);
                assertHitCount(prepareSearch(indexName), 1);
            } catch (AssertionError | Exception e) {
                logger.info("TEMPORARY search after the search shard started did not see the document", e);
                if (indexed.isDone() == false) {
                    prepareIndex(indexName).setId("unstick")
                        .setSource("field", "unstick")
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                        .get();
                    indexed.actionGet(TimeValue.timeValueSeconds(30));
                }
                throw e;
            }
        }
    }

    private void logSearch(String when, SearchResponse response) {
        logger.info("TEMPORARY search {} {}", when, searchSummary(response));
    }

    private static String searchSummary(SearchResponse response) {
        return "hits="
            + response.getHits().getTotalHits()
            + " total="
            + response.getTotalShards()
            + " successful="
            + response.getSuccessfulShards()
            + " skipped="
            + response.getSkippedShards()
            + " failed="
            + response.getFailedShards();
    }
}
