/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License, v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndexingMemoryController;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.XContentType;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

/**
 * SDH E-10233: peer recovery of a logsdb / synthetic-recovery-source replica after the replica node
 * restarts. Ops-based recovery is chosen, then the history lock is dropped because a peer-recovery
 * retention lease exists. A flush and force-merge on the primary in that window (as can happen in
 * production) must not drop INDEX ops the replica still needs. {@code LuceneSyntheticSourceChangesSnapshot}
 * skips live docs whose {@code _recovery_source_size} is missing ({@code requiredFullRange=false}).
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SyntheticRecoverySourcePeerRecoveryIT extends ESIntegTestCase {

    @SuppressWarnings("unchecked")
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(
            super.nodePlugins(),
            MockTransportService.TestPlugin.class,
            InternalSettingsPlugin.class
        );
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(IndexingMemoryController.SHARD_INACTIVE_TIME_SETTING.getKey(), TimeValue.timeValueHours(24))
            .build();
    }

    public void testPrunedSyntheticRecoverySourceIsNotReplayedOnReplica() throws Exception {
        internalCluster().startMasterOnlyNode();
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        ensureStableCluster(3);

        final String indexName = randomIndexName();
        assertAcked(
            prepareCreate(indexName).setSettings(
                indexSettings(1, 1).put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.getName())
                    .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC.name())
                    .put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true)
                    .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), 0)
                    .put(IndexSettings.FILE_BASED_RECOVERY_THRESHOLD_SETTING.getKey(), 1.0d)
                    .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), ByteSizeValue.of(1, ByteSizeUnit.PB))
                    .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "24h")
                    .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "24h")
                    .put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1)
                    .put("index.routing.allocation.include._name", String.join(",", dataNodes))
                    .build()
            ).setMapping("@timestamp", "type=date", "message", "type=keyword")
        );
        ensureGreen(indexName);

        final int docsBeforeFailover = between(20, 40);
        indexDocs(indexName, 0, docsBeforeFailover);
        indicesAdmin().prepareFlush(indexName).setForce(true).get();
        ensureGreen(indexName);

        final String replicaNodeName = clusterService().state().nodes().get(replicaRouting(indexName).currentNodeId()).getName();
        final String primaryNodeName = clusterService().state().nodes().get(primaryRouting(indexName).currentNodeId()).getName();

        final SubscribableListener<Void> atPrepareTranslog = new SubscribableListener<>();
        final SubscribableListener<Void> allowPhase2 = new SubscribableListener<>();
        final AtomicBoolean blockedOnce = new AtomicBoolean();
        MockTransportService.getInstance(primaryNodeName).addSendBehavior((connection, requestId, action, request, options) -> {
            if (PeerRecoveryTargetService.Actions.PREPARE_TRANSLOG.equals(action) && blockedOnce.compareAndSet(false, true)) {
                atPrepareTranslog.onResponse(null);
                allowPhase2.andThenAccept(ignored -> connection.sendRequest(requestId, action, request, options));
                return;
            }
            connection.sendRequest(requestId, action, request, options);
        });

        final int docsWhileReplicaDown = between(20, 40);
        internalCluster().restartNode(replicaNodeName, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                indexDocs(indexName, docsBeforeFailover, docsWhileReplicaDown);
                return Settings.EMPTY;
            }
        });

        try {
            safeAwait(atPrepareTranslog, TimeValue.timeValueMinutes(1));
            assertThat(indicesAdmin().prepareFlush(indexName).setForce(true).get().getFailedShards(), equalTo(0));
            assertThat(indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).setFlush(true).get().getFailedShards(), equalTo(0));
            assertPhase2SnapshotEmitsIndexOps(indexName, docsBeforeFailover, docsWhileReplicaDown);
        } finally {
            allowPhase2.onResponse(null);
            MockTransportService.getInstance(primaryNodeName).clearAllRules();
        }

        ensureGreen(indexName);
        final long expectedMaxSeqNo = docsBeforeFailover + docsWhileReplicaDown - 1L;
        assertBusy(() -> {
            int shards = 0;
            for (ShardStats shardStats : indicesAdmin().prepareStats(indexName).get().getShards()) {
                SeqNoStats seqNoStats = shardStats.getSeqNoStats();
                assertThat(seqNoStats.getMaxSeqNo(), equalTo(expectedMaxSeqNo));
                assertThat(seqNoStats.getLocalCheckpoint(), equalTo(expectedMaxSeqNo));
                shards++;
            }
            assertThat(shards, equalTo(2));
        });
        assertHitCount(prepareSearch(indexName).setSize(0).setTrackTotalHits(true), docsBeforeFailover + docsWhileReplicaDown);
    }

    /**
     * Phase2 uses {@code requiredFullRange=false}. Live INDEX ops must still be emitted for seq#s the
     * replica is missing, including after a flush/force-merge that may prune {@code _recovery_source_size}
     * below the peer-recovery retention lease.
     */
    private void assertPhase2SnapshotEmitsIndexOps(String indexName, int fromSeqNo, int expectedIndexOps) throws Exception {
        final AtomicInteger emittedOps = new AtomicInteger();
        internalCluster().forEveryIndexShard(resolveIndex(indexName), shard -> {
            if (shard.routingEntry().primary() == false || shard.routingEntry().active() == false) {
                return;
            }
            try (
                Translog.Snapshot snapshot = shard.newChangesSnapshot(
                    "prune-check",
                    fromSeqNo,
                    Long.MAX_VALUE,
                    false,
                    true,
                    true,
                    1 << 20
                )
            ) {
                Translog.Operation op;
                while ((op = snapshot.next()) != null) {
                    if (op.opType() == Translog.Operation.Type.INDEX) {
                        emittedOps.incrementAndGet();
                    }
                }
            }
        });
        assertThat(
            "phase2 must still emit INDEX ops for live docs after flush/force-merge; "
                + "LuceneSyntheticSourceChangesSnapshot currently skips them when _recovery_source_size is missing (SDH E-10233)",
            emittedOps.get(),
            equalTo(expectedIndexOps)
        );
    }

    private void indexDocs(String indexName, int startId, int count) {
        Instant timestamp = Instant.parse("2026-08-26T16:16:40Z");
        for (int i = 0; i < count; i++) {
            client().prepareIndex(indexName)
                .setId(Integer.toString(startId + i))
                .setSource(
                    "{\"@timestamp\":\"" + timestamp.plusSeconds(i) + "\",\"message\":\"m" + (startId + i) + "\"}",
                    XContentType.JSON
                )
                .get();
        }
    }

    private ShardRouting primaryRouting(String indexName) {
        return clusterService().state().routingTable().index(indexName).shard(0).primaryShard();
    }

    private ShardRouting replicaRouting(String indexName) {
        return clusterService().state().routingTable().index(indexName).shard(0).replicaShards().getFirst();
    }
}
