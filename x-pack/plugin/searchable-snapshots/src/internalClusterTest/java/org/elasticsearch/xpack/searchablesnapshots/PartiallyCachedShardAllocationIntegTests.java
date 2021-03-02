/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;
import org.elasticsearch.xpack.core.DataTier;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsRequest;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.StreamSupport;

import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.getDataTiersPreference;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.SNAPSHOT_DIRECTORY_FACTORY_KEY;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.SNAPSHOT_RECOVERY_STATE_FACTORY_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.Matchers.sameInstance;

public class PartiallyCachedShardAllocationIntegTests extends BaseSearchableSnapshotsIntegTestCase {

    @TestLogging(reason="nocommit", value="org.elasticsearch.xpack.searchablesnapshots.FrozenCacheSizeService:TRACE")
    public void testCreateAndRestorePartialSearchableSnapshot() throws Exception {
        final String fsRepoName = randomAlphaOfLength(10);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String restoredIndexName = randomBoolean() ? indexName : randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        createRepository(
                fsRepoName,
                "fs",
                Settings.builder().put("location", randomRepoPath()).put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
        );

        assertAcked(prepareCreate(indexName, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true)));
        populateIndex(indexName, 10);
        ensureGreen(indexName);
        final SnapshotInfo snapshotInfo = createFullSnapshot(fsRepoName, snapshotName);

        assertAcked(client().admin().indices().prepareDelete(indexName));

        logger.info("--> restoring partial index [{}] with cache enabled", restoredIndexName);

        Settings.Builder indexSettingsBuilder = Settings.builder()
                .put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), true)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), Boolean.FALSE.toString());
        final List<String> nonCachedExtensions;
        if (randomBoolean()) {
            nonCachedExtensions = randomSubsetOf(Arrays.asList("fdt", "fdx", "nvd", "dvd", "tip", "cfs", "dim"));
            indexSettingsBuilder.putList(SearchableSnapshots.SNAPSHOT_CACHE_EXCLUDED_FILE_TYPES_SETTING.getKey(), nonCachedExtensions);
        } else {
            nonCachedExtensions = Collections.emptyList();
        }
        if (randomBoolean()) {
            indexSettingsBuilder.put(
                    SearchableSnapshots.SNAPSHOT_UNCACHED_CHUNK_SIZE_SETTING.getKey(),
                    new ByteSizeValue(randomLongBetween(10, 100_000))
            );
        }
        final int expectedReplicas;
        if (randomBoolean()) {
            expectedReplicas = numberOfReplicas();
            indexSettingsBuilder.put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, expectedReplicas);
        } else {
            expectedReplicas = 0;
        }
        final String expectedDataTiersPreference;
        if (randomBoolean()) {
            expectedDataTiersPreference = String.join(",", randomSubsetOf(DataTier.ALL_DATA_TIERS));
            indexSettingsBuilder.put(DataTierAllocationDecider.INDEX_ROUTING_PREFER, expectedDataTiersPreference);
        } else {
            expectedDataTiersPreference = getDataTiersPreference(MountSearchableSnapshotRequest.Storage.SHARED_CACHE);
        }

        final AtomicBoolean statsWatcherRunning = new AtomicBoolean(true);
        final Thread statsWatcher = new Thread(() -> {
            while (statsWatcherRunning.get()) {
                final IndicesStatsResponse indicesStatsResponse;
                try {
                    indicesStatsResponse = client().admin().indices().prepareStats(restoredIndexName).clear().setStore(true).get();
                } catch (IndexNotFoundException | IndexClosedException e) {
                    continue;
                    // ok
                }

                for (ShardStats shardStats : indicesStatsResponse.getShards()) {
                    assertThat(
                            shardStats.getShardRouting().toString(),
                            shardStats.getStats().getStore().getReservedSize().getBytes(),
                            equalTo(0L)
                    );
                }
            }
        }, "test-stats-watcher");
        statsWatcher.start();

        final MountSearchableSnapshotRequest req = new MountSearchableSnapshotRequest(
                restoredIndexName,
                fsRepoName,
                snapshotInfo.snapshotId().getName(),
                indexName,
                indexSettingsBuilder.build(),
                Strings.EMPTY_ARRAY,
                true,
                MountSearchableSnapshotRequest.Storage.SHARED_CACHE
        );

        final RestoreSnapshotResponse restoreSnapshotResponse = client().execute(MountSearchableSnapshotAction.INSTANCE, req).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));

        statsWatcherRunning.set(false);
        statsWatcher.join();

        final Settings settings = client().admin()
                .indices()
                .prepareGetSettings(restoredIndexName)
                .get()
                .getIndexToSettings()
                .get(restoredIndexName);
        assertThat(SearchableSnapshots.SNAPSHOT_SNAPSHOT_NAME_SETTING.get(settings), equalTo(snapshotName));
        assertThat(IndexModule.INDEX_STORE_TYPE_SETTING.get(settings), equalTo(SNAPSHOT_DIRECTORY_FACTORY_KEY));
        assertThat(IndexModule.INDEX_RECOVERY_TYPE_SETTING.get(settings), equalTo(SNAPSHOT_RECOVERY_STATE_FACTORY_KEY));
        assertTrue(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.get(settings));
        assertTrue(SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING.exists(settings));
        assertTrue(SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING.exists(settings));
        assertThat(IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING.get(settings).toString(), equalTo("false"));
        assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(settings), equalTo(expectedReplicas));
        assertThat(DataTierAllocationDecider.INDEX_ROUTING_PREFER_SETTING.get(settings), equalTo(expectedDataTiersPreference));
        assertTrue(SearchableSnapshots.SNAPSHOT_PARTIAL_SETTING.get(settings));
        assertTrue(DiskThresholdDecider.SETTING_IGNORE_DISK_WATERMARKS.get(settings));

        // TODO: fix
        // assertSearchableSnapshotStats(restoredIndexName, true, nonCachedExtensions);
        ensureGreen(restoredIndexName);

        assertThat(
                client().admin()
                        .cluster()
                        .prepareState()
                        .clear()
                        .setMetadata(true)
                        .setIndices(restoredIndexName)
                        .get()
                        .getState()
                        .metadata()
                        .index(restoredIndexName)
                        .getTimestampRange(),
                sameInstance(IndexLongFieldRange.UNKNOWN)
        );

        if (deletedBeforeMount) {
            assertThat(client().admin().indices().prepareGetAliases(aliasName).get().getAliases().size(), equalTo(0));
            assertAcked(client().admin().indices().prepareAliases().addAlias(restoredIndexName, aliasName));
        } else if (indexName.equals(restoredIndexName) == false) {
            assertThat(client().admin().indices().prepareGetAliases(aliasName).get().getAliases().size(), equalTo(1));
            assertAcked(
                    client().admin()
                            .indices()
                            .prepareAliases()
                            .addAliasAction(IndicesAliasesRequest.AliasActions.remove().index(indexName).alias(aliasName).mustExist(true))
                            .addAlias(restoredIndexName, aliasName)
            );
        }
        assertThat(client().admin().indices().prepareGetAliases(aliasName).get().getAliases().size(), equalTo(1));

        final Decision diskDeciderDecision = client().admin()
                .cluster()
                .prepareAllocationExplain()
                .setIndex(restoredIndexName)
                .setShard(0)
                .setPrimary(true)
                .setIncludeYesDecisions(true)
                .get()
                .getExplanation()
                .getShardAllocationDecision()
                .getMoveDecision()
                .getCanRemainDecision()
                .getDecisions()
                .stream()
                .filter(d -> d.label().equals(DiskThresholdDecider.NAME))
                .findFirst()
                .orElseThrow();
        assertThat(diskDeciderDecision.type(), equalTo(Decision.Type.YES));
        assertThat(
                diskDeciderDecision.getExplanation(),
                oneOf("disk watermarks are ignored on this index", "there is only a single data node present")
        );

        internalCluster().fullRestart();

        // TODO: fix
        // assertSearchableSnapshotStats(restoredIndexName, false, nonCachedExtensions);

        internalCluster().ensureAtLeastNumDataNodes(2);

        final DiscoveryNode dataNode = randomFrom(
                StreamSupport.stream(
                        client().admin().cluster().prepareState().get().getState().nodes().getDataNodes().values().spliterator(),
                        false
                ).map(c -> c.value).toArray(DiscoveryNode[]::new)
        );

        assertAcked(
                client().admin()
                        .indices()
                        .prepareUpdateSettings(restoredIndexName)
                        .setSettings(
                                Settings.builder()
                                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                                        .put(
                                                IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey(),
                                                dataNode.getName()
                                        )
                        )
        );

        assertFalse(
                client().admin()
                        .cluster()
                        .prepareHealth(restoredIndexName)
                        .setWaitForNoRelocatingShards(true)
                        .setWaitForEvents(Priority.LANGUID)
                        .get()
                        .isTimedOut()
        );

        // TODO: fix
        // assertSearchableSnapshotStats(restoredIndexName, false, nonCachedExtensions);

        assertAcked(
                client().admin()
                        .indices()
                        .prepareUpdateSettings(restoredIndexName)
                        .setSettings(
                                Settings.builder()
                                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                                        .putNull(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey())
                        )
        );

        final String clonedIndexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        assertAcked(
                client().admin()
                        .indices()
                        .prepareResizeIndex(restoredIndexName, clonedIndexName)
                        .setResizeType(ResizeType.CLONE)
                        .setSettings(
                                Settings.builder()
                                        .putNull(IndexModule.INDEX_STORE_TYPE_SETTING.getKey())
                                        .putNull(IndexModule.INDEX_RECOVERY_TYPE_SETTING.getKey())
                                        .build()
                        )
        );
        ensureGreen(clonedIndexName);

        final Settings clonedIndexSettings = client().admin()
                .indices()
                .prepareGetSettings(clonedIndexName)
                .get()
                .getIndexToSettings()
                .get(clonedIndexName);
        assertFalse(clonedIndexSettings.hasValue(IndexModule.INDEX_STORE_TYPE_SETTING.getKey()));
        assertFalse(clonedIndexSettings.hasValue(SearchableSnapshots.SNAPSHOT_SNAPSHOT_NAME_SETTING.getKey()));
        assertFalse(clonedIndexSettings.hasValue(SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING.getKey()));
        assertFalse(clonedIndexSettings.hasValue(SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING.getKey()));
        assertFalse(clonedIndexSettings.hasValue(IndexModule.INDEX_RECOVERY_TYPE_SETTING.getKey()));

        assertAcked(client().admin().indices().prepareDelete(restoredIndexName));
        assertThat(client().admin().indices().prepareGetAliases(aliasName).get().getAliases().size(), equalTo(0));
        assertAcked(client().admin().indices().prepareAliases().addAlias(clonedIndexName, aliasName));
    }

}
