/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class GatewayServiceTests extends ESTestCase {

    public void testRecoversImmediatelyByDefault() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var clusterService = ClusterServiceUtils.createSingleThreadedClusterService(deterministicTaskQueue);
        final var gatewayService = new GatewayService(Settings.EMPTY, clusterService);
        gatewayService.start();

        final var recoveryTimeFuture = new PlainActionFuture<Long>();
        clusterService.addListener(event -> {
            if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) == false) {
                recoveryTimeFuture.onResponse(deterministicTaskQueue.getCurrentTimeMillis());
            }
        });

        clusterService.getClusterApplierService()
            .onNewClusterState(
                "test",
                () -> ClusterState.builder(clusterService.state())
                    .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build())
                    .build(),
                ActionListener.noop()
            );

        deterministicTaskQueue.runAllTasksInTimeOrder();

        assertFalse(clusterService.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK));
        assertTrue(recoveryTimeFuture.isDone());
        assertThat(recoveryTimeFuture.actionGet(), equalTo(0L));
    }

    public void testWaitsForEnoughDataNodesIfRecoverAfterSet() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var clusterService = ClusterServiceUtils.createSingleThreadedClusterService(deterministicTaskQueue);
        final var gatewayService = new GatewayService(
            Settings.builder().put(GatewayService.RECOVER_AFTER_DATA_NODES_SETTING.getKey(), 2).build(),
            clusterService
        );
        gatewayService.start();

        final var recoveryTimeFuture = new PlainActionFuture<Long>();
        clusterService.addListener(event -> {
            if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) == false) {
                recoveryTimeFuture.onResponse(deterministicTaskQueue.getCurrentTimeMillis());
            }
        });

        clusterService.getClusterApplierService()
            .onNewClusterState(
                "test",
                () -> ClusterState.builder(clusterService.state())
                    .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build())
                    .build(),
                ActionListener.noop()
            );

        final var joinTime = randomLongBetween(TimeValue.timeValueMinutes(1).millis() + 1, TimeValue.timeValueMinutes(10).millis());
        deterministicTaskQueue.scheduleAt(
            joinTime,
            () -> clusterService.getClusterApplierService()
                .onNewClusterState(
                    "test",
                    () -> ClusterState.builder(clusterService.state())
                        .nodes(
                            DiscoveryNodes.builder(clusterService.state().nodes())
                                .add(new DiscoveryNode("new-node", buildNewFakeTransportAddress(), Version.CURRENT))
                        )
                        .build(),
                    ActionListener.noop()
                )
        );

        deterministicTaskQueue.runAllTasksInTimeOrder();

        assertFalse(clusterService.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK));
        assertTrue(recoveryTimeFuture.isDone());
        assertThat(recoveryTimeFuture.actionGet(), equalTo(joinTime));
    }

    public void testWaitsForEnoughDataNodesIfExpectedNodesSet() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var clusterService = ClusterServiceUtils.createSingleThreadedClusterService(deterministicTaskQueue);

        final var settings = Settings.builder().put(GatewayService.EXPECTED_DATA_NODES_SETTING.getKey(), 2);
        final long timeoutMillis;
        if (randomBoolean()) {
            timeoutMillis = GatewayService.RECOVER_AFTER_TIME_SETTING.get(Settings.EMPTY).millis();
        } else {
            timeoutMillis = randomLongBetween(2, TimeValue.timeValueMinutes(10).millis());
            settings.put(GatewayService.RECOVER_AFTER_TIME_SETTING.getKey(), TimeValue.timeValueMillis(timeoutMillis));
        }
        final var gatewayService = new GatewayService(settings.build(), clusterService);

        gatewayService.start();

        final var recoveryTimeFuture = new PlainActionFuture<Long>();
        clusterService.addListener(event -> {
            if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) == false) {
                recoveryTimeFuture.onResponse(deterministicTaskQueue.getCurrentTimeMillis());
            }
        });

        clusterService.getClusterApplierService()
            .onNewClusterState(
                "test",
                () -> ClusterState.builder(clusterService.state())
                    .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build())
                    .build(),
                ActionListener.noop()
            );

        final var joinTime = randomLongBetween(1, timeoutMillis - 1);
        deterministicTaskQueue.scheduleAt(
            joinTime,
            () -> clusterService.getClusterApplierService()
                .onNewClusterState(
                    "test",
                    () -> ClusterState.builder(clusterService.state())
                        .nodes(
                            DiscoveryNodes.builder(clusterService.state().nodes())
                                .add(new DiscoveryNode("new-node", buildNewFakeTransportAddress(), Version.CURRENT))
                        )
                        .build(),
                    ActionListener.noop()
                )
        );

        deterministicTaskQueue.runAllTasksInTimeOrder();

        assertFalse(clusterService.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK));
        assertTrue(recoveryTimeFuture.isDone());
        assertThat(recoveryTimeFuture.actionGet(), equalTo(joinTime));
    }

    public void testWaitsForTimeoutIfExpectedNodesSet() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var clusterService = ClusterServiceUtils.createSingleThreadedClusterService(deterministicTaskQueue);

        final var settings = Settings.builder().put(GatewayService.EXPECTED_DATA_NODES_SETTING.getKey(), 2);
        final long timeoutMillis;
        if (randomBoolean()) {
            timeoutMillis = GatewayService.RECOVER_AFTER_TIME_SETTING.get(Settings.EMPTY).millis();
        } else {
            timeoutMillis = randomLongBetween(2, TimeValue.timeValueMinutes(10).millis());
            settings.put(GatewayService.RECOVER_AFTER_TIME_SETTING.getKey(), TimeValue.timeValueMillis(timeoutMillis));
        }
        final var gatewayService = new GatewayService(settings.build(), clusterService);

        gatewayService.start();

        final var recoveryTimeFuture = new PlainActionFuture<Long>();
        clusterService.addListener(event -> {
            if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) == false) {
                recoveryTimeFuture.onResponse(deterministicTaskQueue.getCurrentTimeMillis());
            }
        });

        clusterService.getClusterApplierService()
            .onNewClusterState(
                "test",
                () -> ClusterState.builder(clusterService.state())
                    .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build())
                    .build(),
                ActionListener.noop()
            );

        deterministicTaskQueue.runAllTasksInTimeOrder();

        assertFalse(clusterService.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK));
        assertTrue(recoveryTimeFuture.isDone());
        assertThat(recoveryTimeFuture.actionGet(), equalTo(timeoutMillis));
    }

}
