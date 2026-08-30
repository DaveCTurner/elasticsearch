/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License, v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * SDH E-10233: end-to-end reproduction of logsdb / synthetic recovery source skipping live INDEX ops
 * in peer-recovery phase2 after {@code _recovery_source_size} is pruned.
 * <p>
 * Recovery chooses ops-based replay (history appears complete while the retention lock freezes
 * {@code min_retained}). The lock is then dropped because a peer-recovery retention lease exists.
 * Before phase2 opens its snapshot we advance GCP / retention leases and force-merge, which
 * strips {@code _recovery_source_size} from live docs. The synthetic recovery snapshot
 * ({@code requiredFullRange=false}) emits nothing for them, so the replica never applies the
 * seq#s indexed while it was down.
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
                    .put("index.routing.allocation.include._name", String.join(",", dataNodes))
                    .build()
            ).setMapping("@timestamp", "type=date", "message", "type=keyword")
        );
        ensureGreen(indexName);

        final int docsBeforeFailover = between(20, 40);
        indexDocs(null, indexName, 0, docsBeforeFailover);
        indicesAdmin().prepareFlush(indexName).setForce(true).get();
        ensureGreen(indexName);

        final String replicaNodeName = clusterService().state().nodes().get(replicaRouting(indexName).currentNodeId()).getName();
        final String primaryNodeName = clusterService().state().nodes().get(primaryRouting(indexName).currentNodeId()).getName();

        final CountDownLatch atPrepareTranslog = new CountDownLatch(1);
        final CountDownLatch allowPhase2 = new CountDownLatch(1);
        final AtomicBoolean blockedOnce = new AtomicBoolean();
        MockTransportService.getInstance(primaryNodeName).addSendBehavior((connection, requestId, action, request, options) -> {
            if (PeerRecoveryTargetService.Actions.PREPARE_TRANSLOG.equals(action) && blockedOnce.compareAndSet(false, true)) {
                atPrepareTranslog.countDown();
                try {
                    if (allowPhase2.await(2, TimeUnit.MINUTES) == false) {
                        throw new AssertionError("timed out waiting to resume phase2");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        final int docsWhileReplicaDown = between(20, 40);
        internalCluster().restartNode(replicaNodeName, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                indexDocs(primaryNodeName, indexName, docsBeforeFailover, docsWhileReplicaDown);
                return Settings.EMPTY;
            }
        });

        try {
            assertTrue("recovery never reached prepare_translog", atPrepareTranslog.await(1, TimeUnit.MINUTES));
            pruneRecoverySourceOnPrimary(indexName, primaryNodeName, docsBeforeFailover);

            allowPhase2.countDown();

            final long expectedStuckLcp = docsBeforeFailover - 1L;
            final long expectedPrimaryMax = docsBeforeFailover + docsWhileReplicaDown - 1L;
            assertBusy(() -> {
                IndexShard replica = shardOnNode(indexName, replicaNodeName);
                IndexShard primary = shardOnNode(indexName, primaryNodeName);
                assertNotNull(replica);
                SeqNoStats replicaSeqNo = replica.seqNoStats();
                SeqNoStats primarySeqNo = primary.seqNoStats();
                assertThat(primarySeqNo.getMaxSeqNo(), equalTo(expectedPrimaryMax));
                assertThat(primarySeqNo.getLocalCheckpoint(), equalTo(expectedPrimaryMax));
                assertThat(replicaSeqNo.getLocalCheckpoint(), equalTo(expectedStuckLcp));
                assertThat(replica.docStats().getCount(), equalTo((long) docsBeforeFailover));
                assertThat(replica.docStats().getCount(), lessThan(primary.docStats().getCount()));
            }, 60, TimeUnit.SECONDS);
        } finally {
            allowPhase2.countDown();
            MockTransportService.getInstance(primaryNodeName).clearAllRules();
            assertAcked(indicesAdmin().prepareDelete(indexName));
        }
    }

    /**
     * After ops-based recovery is chosen, the history lock is dropped. Advance GCP and peer-recovery
     * retention leases so {@code min_retained} jumps, then force-merge to strip {@code _recovery_source_size}.
     */
    private void pruneRecoverySourceOnPrimary(String indexName, String primaryNodeName, int docsBeforeFailover) throws Exception {
        final IndexShard primary = shardOnNode(indexName, primaryNodeName);
        final long maxSeqNo = primary.seqNoStats().getMaxSeqNo();
        // The in-sync copy that left with the replica node still pins GCP at docsBeforeFailover-1.
        // Its allocation id may differ from the initializing replica's. Bump every non-primary
        // in-sync copy plus the recovering replica so lastSynced GCP and PRRLs can advance.
        final String primaryAllocationId = primary.routingEntry().allocationId().getId();
        for (String allocationId : clusterService().state().metadata().getProject().index(indexName).inSyncAllocationIds(0)) {
            if (allocationId.equals(primaryAllocationId) == false) {
                primary.updateLocalCheckpointForShard(allocationId, maxSeqNo);
                primary.updateGlobalCheckpointForShard(allocationId, maxSeqNo);
            }
        }
        final String recoveringReplicaAllocationId = replicaRouting(indexName).allocationId().getId();
        primary.updateLocalCheckpointForShard(recoveringReplicaAllocationId, maxSeqNo);
        primary.updateGlobalCheckpointForShard(recoveringReplicaAllocationId, maxSeqNo);
        primary.sync();
        primary.flush(new FlushRequest().force(true).waitIfOngoing(true));
        final long retainFrom = maxSeqNo + 1L;
        primary.renewRetentionLease(
            ReplicationTracker.getPeerRecoveryRetentionLeaseId(primary.routingEntry()),
            retainFrom,
            ReplicationTracker.PEER_RECOVERY_RETENTION_LEASE_SOURCE
        );
        primary.renewRetentionLease(
            ReplicationTracker.getPeerRecoveryRetentionLeaseId(replicaRouting(indexName)),
            retainFrom,
            ReplicationTracker.PEER_RECOVERY_RETENTION_LEASE_SOURCE
        );
        primary.syncRetentionLeases();
        assertBusy(() -> assertThat(primary.getMinRetainedSeqNo(), greaterThan((long) docsBeforeFailover)));
        primary.forceMerge(new ForceMergeRequest().maxNumSegments(1).flush(true));
        int emittedOps = 0;
        try (
            Translog.Snapshot snapshot = primary.newChangesSnapshot(
                "prune-check",
                docsBeforeFailover,
                Long.MAX_VALUE,
                false,
                true,
                true,
                1 << 20
            )
        ) {
            while (snapshot.next() != null) {
                emittedOps++;
            }
        }
        assertThat(
            "force-merge should strip _recovery_source_size so the synthetic snapshot emits no INDEX ops",
            emittedOps,
            equalTo(0)
        );
    }

    private void indexDocs(String nodeName, String indexName, int startId, int count) {
        Instant timestamp = Instant.parse("2026-08-26T16:16:40Z");
        var indexClient = nodeName == null ? client() : internalCluster().client(nodeName);
        for (int i = 0; i < count; i++) {
            indexClient.prepareIndex(indexName)
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

    private IndexShard shardOnNode(String indexName, String nodeName) {
        Index index = resolveIndex(indexName);
        return internalCluster().getInstance(IndicesService.class, nodeName).getShardOrNull(new ShardId(index, 0));
    }
}
