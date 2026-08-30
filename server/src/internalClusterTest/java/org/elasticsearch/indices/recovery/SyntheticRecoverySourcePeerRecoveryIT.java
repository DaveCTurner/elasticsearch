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
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndexingMemoryController;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.XContentType;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.index.seqno.SequenceNumbersTestUtils.assertMinRetainedSeqNoAdvanced;
import static org.elasticsearch.index.seqno.SequenceNumbersTestUtils.assertRetentionLeasesAdvanced;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * SDH E-10233: end-to-end reproduction of logsdb / synthetic recovery source skipping live INDEX ops
 * in peer-recovery phase2 after {@code _recovery_source_size} is pruned.
 * <p>
 * Recovery chooses ops-based replay (history appears complete while the retention lock freezes
 * {@code min_retained}). The lock is then dropped because a peer-recovery retention lease exists.
 * Before phase2 opens its snapshot we advance retention leases, flush, and force-merge, which
 * strips {@code _recovery_source_size} from live docs. The synthetic recovery snapshot
 * ({@code requiredFullRange=false}) emits nothing for them, so the replica never applies the
 * seq#s indexed while it was down. This test asserts that after that prune, the phase2
 * snapshot still emits INDEX ops for the missing range.
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

        final String indexName = "logsdb-recovery-prune";
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
                safeAwait(allowPhase2);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        final int docsWhileReplicaDown = between(20, 40);
        final Settings replicaDataPathSettings = internalCluster().dataPathSettings(replicaNodeName);
        internalCluster().stopNode(replicaNodeName);
        setReplicaCount(0, indexName);
        ensureGreen(indexName);
        indexDocs(indexName, docsBeforeFailover, docsWhileReplicaDown);

        internalCluster().startDataOnlyNode(replicaDataPathSettings);
        ensureStableCluster(3);
        setReplicaCount(1, indexName);

        try {
            safeAwait(atPrepareTranslog);
            pruneRecoverySourceFromOutside(indexName, docsBeforeFailover, docsWhileReplicaDown);
            // Cancel recovery while still blocked in PREPARE_TRANSLOG. Resuming would hang in
            // markAllocationIdAsInSync because GCP is already at the primary max.
            assertAcked(indicesAdmin().prepareDelete(indexName));
        } finally {
            allowPhase2.onResponse(null);
            MockTransportService.getInstance(primaryNodeName).clearAllRules();
        }
    }

    /**
     * After ops-based recovery is chosen, the history lock is dropped. Advance the recovering
     * replica's peer-recovery retention lease, flush, and force-merge so {@code min_retained}
     * jumps and {@code _recovery_source_size} is stripped.
     */
    private void pruneRecoverySourceFromOutside(String indexName, int docsBeforeFailover, int docsWhileReplicaDown) throws Exception {
        long maxSeqNo = -1L;
        for (ShardStats shardStats : indicesAdmin().prepareStats(indexName).get().getShards()) {
            if (shardStats.getShardRouting().primary()) {
                maxSeqNo = shardStats.getSeqNoStats().getMaxSeqNo();
                break;
            }
        }
        assertThat(maxSeqNo, equalTo((long) docsBeforeFailover + docsWhileReplicaDown - 1L));

        final ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        safeExecute(
            client(),
            RetentionLeaseActions.RENEW,
            new RetentionLeaseActions.RenewRequest(
                shardId,
                ReplicationTracker.getPeerRecoveryRetentionLeaseId(replicaRouting(indexName)),
                maxSeqNo + 1,
                ReplicationTracker.PEER_RECOVERY_RETENTION_LEASE_SOURCE
            )
        );
        assertRetentionLeasesAdvanced(client(), indexName, maxSeqNo + 1);

        assertThat(indicesAdmin().prepareFlush(indexName).setForce(true).get().getFailedShards(), equalTo(0));
        assertMinRetainedSeqNoAdvanced(internalCluster(), indexName, maxSeqNo + 1);
        assertThat(indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).setFlush(true).get().getFailedShards(), equalTo(0));

        final AtomicInteger emittedOps = new AtomicInteger();
        internalCluster().forEveryIndexShard(resolveIndex(indexName), shard -> {
            if (shard.routingEntry().primary() == false || shard.routingEntry().active() == false) {
                return;
            }
            try (
                Translog.Snapshot snapshot = shard.newChangesSnapshot(
                    "prune-check",
                    docsBeforeFailover,
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
            "phase2 must still emit INDEX ops for live docs after _recovery_source_size is pruned; "
                + "LuceneSyntheticSourceChangesSnapshot currently skips them (SDH E-10233)",
            emittedOps.get(),
            equalTo(docsWhileReplicaDown)
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
