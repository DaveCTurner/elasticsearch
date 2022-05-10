/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class GatewayService extends AbstractLifecycleComponent implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(GatewayService.class);

    public static final Setting<Integer> RECOVER_AFTER_DATA_NODES_SETTING = Setting.intSetting(
        "gateway.recover_after_data_nodes",
        -1,
        -1,
        Property.NodeScope
    );

    public static final Setting<Integer> EXPECTED_DATA_NODES_SETTING = Setting.intSetting(
        "gateway.expected_data_nodes",
        -1,
        -1,
        Property.NodeScope
    );

    public static final Setting<TimeValue> RECOVER_AFTER_TIME_SETTING = Setting.positiveTimeSetting(
        "gateway.recover_after_time",
        TimeValue.timeValueMinutes(5),
        Property.NodeScope
    );

    public static final Setting<TimeValue> DELAYED_RECOVERY_WARNING_INTERVAL = Setting.timeSetting(
        "gateway.delayed_recovery_warning_interval",
        TimeValue.timeValueMinutes(1),
        Property.NodeScope
    );

    public static final ClusterBlock STATE_NOT_RECOVERED_BLOCK = new ClusterBlock(
        1,
        "state not recovered / initialized",
        true,
        true,
        false,
        RestStatus.SERVICE_UNAVAILABLE,
        ClusterBlockLevel.ALL
    );

    private final ThreadPool threadPool;

    private final ClusterService clusterService;
    private final RecoverStateExecutor executor = new RecoverStateExecutor();

    // hard limit, we do not recover until this many data nodes exist
    private final int recoverAfterDataNodes;

    // soft limit, once recoverAfterDataNodes is satisfied we wait for either this many data nodes _or_ the timeout
    private final int expectedDataNodes;
    private final TimeValue recoverAfterTime;

    private final TimeValue delayedRecoveryWarningInterval;

    private final AtomicReference<AbstractRunnable> currentDelayedRecovery = new AtomicReference<>();
    private final AtomicReference<AbstractRunnable> currentWarningEmitter = new AtomicReference<>();

    @Inject
    public GatewayService(final Settings settings, final ClusterService clusterService, final ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;

        this.recoverAfterDataNodes = RECOVER_AFTER_DATA_NODES_SETTING.get(settings);
        this.expectedDataNodes = EXPECTED_DATA_NODES_SETTING.get(settings);
        this.recoverAfterTime = RECOVER_AFTER_TIME_SETTING.get(settings);
        this.delayedRecoveryWarningInterval = DELAYED_RECOVERY_WARNING_INTERVAL.get(settings);
    }

    @Override
    protected void doStart() {
        if (DiscoveryNode.isMasterNode(clusterService.getSettings())) {
            // use post applied so that the state will be visible to the background recovery thread we spawn in performStateRecovery
            clusterService.addListener(this);
        }
    }

    @Override
    protected void doStop() {
        clusterService.removeListener(this);
    }

    @Override
    protected void doClose() {}

    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        if (lifecycle.stoppedOrClosed()) {
            currentDelayedRecovery.set(null);
            currentWarningEmitter.set(null);
            return;
        }

        final var clusterState = event.state();

        if (clusterState.nodes().isLocalNodeElectedMaster() == false) {
            // not our job to recover
            currentDelayedRecovery.set(null);
            currentWarningEmitter.set(null);
            return;
        }

        if (clusterState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false) {
            // already recovered
            currentDelayedRecovery.set(null);
            currentWarningEmitter.set(null);
            return;
        }

        scheduleIfNew(new WarningEmitter(), delayedRecoveryWarningInterval, currentWarningEmitter);

        final var dataNodeCount = clusterState.nodes().getDataNodes().size();
        if (satisfiesTarget(recoverAfterDataNodes, dataNodeCount)) {
            if (satisfiesTarget(expectedDataNodes, dataNodeCount)) {
                currentDelayedRecovery.set(null);
                runRecovery();
            } else {
                scheduleIfNew(new DelayedRecovery(), recoverAfterTime, currentDelayedRecovery);
            }
        } else {
            // recover_after_data_nodes not satisfied yet
            logger.debug(
                "not recovering from gateway, nodes_size (data) [{}] < recover_after_data_nodes [{}]",
                dataNodeCount,
                recoverAfterDataNodes
            );
            currentDelayedRecovery.set(null);
        }
    }

    private static boolean satisfiesTarget(int targetCount, int dataNodeCount) {
        return targetCount == -1 || targetCount <= dataNodeCount;
    }

    private void scheduleIfNew(AbstractRunnable command, TimeValue delay, AtomicReference<AbstractRunnable> holder) {
        if (holder.compareAndSet(null, command)) {
            threadPool.schedule(command, delay, ThreadPool.Names.SAME);
        }
    }

    private static final String TASK_SOURCE = "local-gateway-elected-state";

    private class RecoverStateExecutor implements ClusterStateTaskExecutor<RecoverStateUpdateTask> {
        @Override
        public ClusterState execute(ClusterState currentState, List<TaskContext<RecoverStateUpdateTask>> taskContexts) throws Exception {
            if (currentState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false) {
                logger.debug("cluster is already recovered");
                for (final var taskContext : taskContexts) {
                    taskContext.success(ActionListener.noop());
                }
                return currentState;
            } else {
                var first = true;
                for (final var taskContext : taskContexts) {
                    taskContext.success(first ? new ActionListener<>() {
                        @Override
                        public void onResponse(ClusterState clusterState) {
                            logger.info("recovered [{}] indices into cluster_state", currentState.metadata().indices().size());
                            clusterService.getRerouteService().reroute("state recovered", Priority.NORMAL, ActionListener.noop());
                        }

                        @Override
                        public void onFailure(Exception e) {
                            taskContext.getTask().onFailure(e);
                        }
                    } : ActionListener.noop());
                    first = false;
                }
                return ClusterStateUpdaters.removeStateNotRecoveredBlock(ClusterStateUpdaters.updateRoutingTable(currentState));
            }
        }
    }

    private static class RecoverStateUpdateTask implements ClusterStateTaskListener {
        @Override
        public void clusterStateProcessed(final ClusterState oldState, final ClusterState newState) {
            assert false : "not called";
        }

        @Override
        public void onFailure(final Exception e) {
            logger.log(
                MasterService.isPublishFailureException(e) ? Level.DEBUG : Level.INFO,
                () -> new ParameterizedMessage("unexpected failure during [{}]", TASK_SOURCE),
                e
            );
        }
    }

    private class DelayedRecovery extends AbstractRunnable {
        @Override
        public void onFailure(Exception e) {
            assert e instanceof EsRejectedExecutionException esre && esre.isExecutorShutdown();
            currentDelayedRecovery.compareAndSet(this, null);
        }

        @Override
        protected void doRun() throws Exception {
            if (currentDelayedRecovery.compareAndSet(this, null)) {
                runRecovery();
            }
        }
    }

    private class WarningEmitter extends AbstractRunnable {
        @Override
        public void onFailure(Exception e) {
            assert e instanceof EsRejectedExecutionException esre && esre.isExecutorShutdown();
            currentWarningEmitter.compareAndSet(this, null);
        }

        @Override
        protected void doRun() throws Exception {
            if (currentWarningEmitter.get() == this) {
                logger.info(
                    "state recovery is delayed until the cluster has [{}] data nodes; it currently has [{}]",
                    Math.max(recoverAfterDataNodes, expectedDataNodes),
                    clusterService.state().nodes().getDataNodes().size()
                );
                threadPool.schedule(this, delayedRecoveryWarningInterval, ThreadPool.Names.SAME);
            }
        }
    }

    private void runRecovery() {
        clusterService.submitStateUpdateTask(
            TASK_SOURCE,
            new RecoverStateUpdateTask(),
            ClusterStateTaskConfig.build(Priority.NORMAL),
            executor
        );
    }
}
