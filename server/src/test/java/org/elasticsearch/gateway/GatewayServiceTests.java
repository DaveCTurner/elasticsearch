/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class GatewayServiceTests extends ESTestCase {

    private static class TestHarness {

        private final DeterministicTaskQueue deterministicTaskQueue;
        private final ClusterService clusterService;
        private long recoveryTimeMillis = Long.MIN_VALUE;

        TestHarness(Settings settings) {
            deterministicTaskQueue = new DeterministicTaskQueue();
            clusterService = ClusterServiceUtils.createSingleThreadedClusterService(deterministicTaskQueue);

            final var localNode = new DiscoveryNode("local", "local", buildNewFakeTransportAddress(), Map.of(),
                Set.of(DiscoveryNodeRole.MASTER_ROLE), Version.CURRENT);
            applyState("initialize", ClusterState.builder(clusterService.state())
                .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).build())
                .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build())
                .build());

            final var gatewayService = new GatewayService(settings, clusterService);
            gatewayService.start();

            clusterService.addListener(new ClusterStateListener() {
                @Override
                public void clusterChanged(ClusterChangedEvent event) {
                    if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) == false) {
                        recoveryTimeMillis = deterministicTaskQueue.getCurrentTimeMillis();
                        clusterService.removeListener(this);
                    }
                }
            });
        }

        void scheduleAfter(long delayMillis, Runnable task) {
            deterministicTaskQueue.scheduleAt(deterministicTaskQueue.getCurrentTimeMillis() + delayMillis, task);
        }

        void run() {
            deterministicTaskQueue.runAllTasksInTimeOrder();
        }

        void assertRecoveredAt(long expectedRecoveryTimeMillis) {
            assertFalse(clusterService.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK));
            assertThat(recoveryTimeMillis, equalTo(expectedRecoveryTimeMillis));
        }

        void becomeMaster() {
            final var clusterState = clusterService.state();
            final var discoveryNodes = clusterState.nodes();
            applyState("become-master", ClusterState.builder(clusterState)
                .nodes(DiscoveryNodes.builder(discoveryNodes).masterNodeId(discoveryNodes.getLocalNodeId()))
                .build());
        }

        void addDataNode() {
            final var clusterState = clusterService.state();
            final var discoveryNodes = clusterState.nodes();
            final var nodeId = "data-node-" + discoveryNodes.size();
            applyState("add-data-node", ClusterState.builder(clusterState)
                .nodes(DiscoveryNodes.builder(discoveryNodes)
                    .add(new DiscoveryNode(nodeId, nodeId, buildNewFakeTransportAddress(), Map.of(),
                        Set.copyOf(randomValueOtherThanMany(roles -> roles.stream().noneMatch(DiscoveryNodeRole::canContainData),
                            () -> randomSubsetOf(DiscoveryNodeRole.roles()))),
                        Version.CURRENT)).build())
                .build());
        }

        private void applyState(String source, ClusterState newState) {
            final var future = new PlainActionFuture<Void>();
            clusterService.getClusterApplierService()
                .onNewClusterState(
                    source,
                    () -> newState,
                    future
                );
            assertTrue(future.isDone());
            assertNull(future.actionGet());
        }

    }

    public void testRecoversImmediatelyByDefault() {
        final var testHarness = new TestHarness(Settings.EMPTY);
        testHarness.becomeMaster();

        if (randomBoolean()) {
            testHarness.scheduleAfter(between(0, 10), testHarness::addDataNode);
        }

        testHarness.run();
        testHarness.assertRecoveredAt(0L);
    }

    public void testWaitsForEnoughDataNodesIfRecoverAfterSet() {
        final var recoverAfterNodes = between(1, 10);

        final var testHarness = new TestHarness(Settings.builder().put(GatewayService.RECOVER_AFTER_DATA_NODES_SETTING.getKey(),
            recoverAfterNodes).build());
        testHarness.becomeMaster();

        final var joinTimes = new long[recoverAfterNodes + between(0, 3)];
        for (int i = 0; i < joinTimes.length; i++) {
            joinTimes[i] = randomLongBetween(0, TimeValue.timeValueHours(1).millis());
            testHarness.scheduleAfter(joinTimes[i], testHarness::addDataNode);
        }
        Arrays.sort(joinTimes);

        testHarness.run();
        testHarness.assertRecoveredAt(joinTimes[recoverAfterNodes - 1]);
    }

    public void testWaitsForEnoughDataNodesIfExpectedNodesSet() {
        final var expectedDataNodes = between(1, 10);

        final var settings = Settings.builder().put(GatewayService.EXPECTED_DATA_NODES_SETTING.getKey(), expectedDataNodes);
        final long timeoutMillis;
        if (randomBoolean()) {
            timeoutMillis = GatewayService.RECOVER_AFTER_TIME_SETTING.get(Settings.EMPTY).millis();
        } else {
            timeoutMillis = randomLongBetween(2, TimeValue.timeValueMinutes(10).millis());
            settings.put(GatewayService.RECOVER_AFTER_TIME_SETTING.getKey(), TimeValue.timeValueMillis(timeoutMillis));
        }

        final var testHarness = new TestHarness(settings.build());
        testHarness.becomeMaster();

        final var joinTimes = new long[expectedDataNodes + between(0, 3)];
        for (int i = 0; i < joinTimes.length; i++) {
            joinTimes[i] = randomLongBetween(0, timeoutMillis - 1);
            testHarness.scheduleAfter(joinTimes[i], testHarness::addDataNode);
        }
        Arrays.sort(joinTimes);

        testHarness.run();
        testHarness.assertRecoveredAt(joinTimes[expectedDataNodes - 1]);
    }

    public void testWaitsForTimeoutIfExpectedNodesSet() {
        final var expectedDataNodes = between(1, 10);

        final var settings = Settings.builder().put(GatewayService.EXPECTED_DATA_NODES_SETTING.getKey(), expectedDataNodes);
        final long timeoutMillis;
        if (randomBoolean()) {
            timeoutMillis = GatewayService.RECOVER_AFTER_TIME_SETTING.get(Settings.EMPTY).millis();
        } else {
            timeoutMillis = randomLongBetween(2, TimeValue.timeValueMinutes(10).millis());
            settings.put(GatewayService.RECOVER_AFTER_TIME_SETTING.getKey(), TimeValue.timeValueMillis(timeoutMillis));
        }

        final var testHarness = new TestHarness(settings.build());
        testHarness.becomeMaster();

        final var earlyNodes = between(0, expectedDataNodes -1);
        for (int i = 0; i < earlyNodes; i++) {
            testHarness.scheduleAfter(randomLongBetween(0, timeoutMillis - 1), testHarness::addDataNode);
        }

        final var laterNodes = between(0, 2);
        for (int i = 0; i < laterNodes; i++) {
            testHarness.scheduleAfter(randomLongBetween(timeoutMillis + 1, TimeValue.timeValueHours(1).millis()), testHarness::addDataNode);
        }

        testHarness.run();
        testHarness.assertRecoveredAt(timeoutMillis);
    }

}
