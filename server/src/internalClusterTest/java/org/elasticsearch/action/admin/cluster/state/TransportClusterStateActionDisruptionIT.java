/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.coordination.ClusterBootstrapService;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class TransportClusterStateActionDisruptionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockTransportService.TestPlugin.class);
    }

    public void testLocalRequestAlwaysSucceeds() throws Exception {
        runRepeatedlyWhileChangingMaster(() -> {
            final String node = randomFrom(internalCluster().getNodeNames());
            final DiscoveryNodes discoveryNodes = client(node).admin()
                .cluster()
                .prepareState(TimeValue.timeValueMillis(100))
                .clear()
                .setNodes(true)
                .setBlocks(true)
                .get()
                .getState()
                .nodes();
            for (DiscoveryNode discoveryNode : discoveryNodes) {
                if (discoveryNode.getName().equals(node)) {
                    return;
                }
            }
            fail("nodes did not contain [" + node + "]: " + discoveryNodes);
        });
    }

    public void testLocalRequestWaitsForMetadata() throws Exception {
        runRepeatedlyWhileChangingMaster(() -> {
            final String node = randomFrom(internalCluster().getNodeNames());
            final long metadataVersion = internalCluster().getInstance(ClusterService.class, node)
                .getClusterApplierService()
                .state()
                .metadata()
                .version();
            final long waitForMetadataVersion = randomLongBetween(Math.max(1, metadataVersion - 3), metadataVersion + 5);
            final ClusterStateResponse clusterStateResponse = client(node).admin()
                .cluster()
                .prepareState(TimeValue.timeValueMillis(100))
                .clear()
                .setMetadata(true)
                .setBlocks(true)
                .setWaitForMetadataVersion(waitForMetadataVersion)
                .setWaitForTimeOut(TimeValue.timeValueMillis(100))
                .get();
            if (clusterStateResponse.isWaitForTimedOut() == false) {
                final Metadata metadata = clusterStateResponse.getState().metadata();
                assertThat(
                    "waited for metadata version " + waitForMetadataVersion + " with node " + node,
                    metadata.version(),
                    greaterThanOrEqualTo(waitForMetadataVersion)
                );
            }
        });
    }

    public void runRepeatedlyWhileChangingMaster(Runnable runnable) throws Exception {
        internalCluster().startNodes(3);

        assertBusy(
            () -> assertThat(
                clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
                    .clear()
                    .setMetadata(true)
                    .setBlocks(true)
                    .get()
                    .getState()
                    .getLastCommittedConfiguration()
                    .getNodeIds()
                    .stream()
                    .filter(n -> ClusterBootstrapService.isBootstrapPlaceholder(n) == false)
                    .collect(Collectors.toSet()),
                hasSize(3)
            )
        );

        final String masterName = internalCluster().getMasterName();

        final AtomicBoolean shutdown = new AtomicBoolean();
        final Thread assertingThread = new Thread(() -> {
            while (shutdown.get() == false) {
                runnable.run();
            }
        }, "asserting thread");

        final Thread updatingThread = new Thread(() -> {
            String value = "none";
            while (shutdown.get() == false) {
                value = "none".equals(value) ? "all" : "none";
                final String nonMasterNode = randomValueOtherThan(masterName, () -> randomFrom(internalCluster().getNodeNames()));
                assertAcked(
                    client(nonMasterNode).admin()
                        .cluster()
                        .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                        .setPersistentSettings(Settings.builder().put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), value))
                );
            }
        }, "updating thread");

        final List<MockTransportService> mockTransportServices = StreamSupport.stream(
            internalCluster().getInstances(TransportService.class).spliterator(),
            false
        ).map(ts -> (MockTransportService) ts).toList();

        assertingThread.start();
        updatingThread.start();

        final var masterTransportService = MockTransportService.getInstance(masterName);

        for (final var mockTransportService : mockTransportServices) {
            if (masterTransportService != mockTransportService) {
                masterTransportService.addFailToSendNoConnectRule(mockTransportService);
                mockTransportService.addFailToSendNoConnectRule(masterTransportService);
            }
        }

        final String nonMasterNode = randomValueOtherThan(masterName, () -> randomFrom(internalCluster().getNodeNames()));
        awaitClusterState(
            logger,
            nonMasterNode,
            state -> Optional.ofNullable(state.nodes().getMasterNode()).map(m -> m.getName().equals(masterName) == false).orElse(false)
        );

        shutdown.set(true);
        assertingThread.join();
        updatingThread.join();
        internalCluster().close();
    }

    public void testFailsWithBlockExceptionIfBlockedAndBlocksNotRequested() {
        internalCluster().startMasterOnlyNode(Settings.builder().put(GatewayService.RECOVER_AFTER_DATA_NODES_SETTING.getKey(), 1).build());
        final var state = safeGet(clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).clear().setBlocks(true).execute()).getState();
        assertTrue(state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK));

        assertThat(
            safeAwaitFailure(
                SubscribableListener.<ClusterStateResponse>newForked(
                    l -> clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).clear().execute(l)
                )
            ),
            instanceOf(ClusterBlockException.class)
        );

        internalCluster().startDataOnlyNode();

        final var recoveredState = safeGet(clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).clear().setBlocks(randomBoolean()).execute())
            .getState();
        assertFalse(recoveredState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK));
    }

}
