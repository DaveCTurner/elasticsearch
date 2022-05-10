/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.Throwables;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStatePublicationEvent;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.ClusterStatePublisher;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static junit.framework.TestCase.fail;

public class ClusterServiceUtils {

    public static void setState(ClusterApplierService executor, ClusterState clusterState) {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exception = new AtomicReference<>();
        executor.onNewClusterState(
            "test setting state",
            () -> ClusterState.builder(clusterState).version(clusterState.version() + 1).build(),
            new ActionListener<>() {
                @Override
                public void onResponse(Void ignored) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    exception.set(e);
                    latch.countDown();
                }
            }
        );
        try {
            latch.await();
            if (exception.get() != null) {
                Throwables.rethrow(exception.get());
            }
        } catch (InterruptedException e) {
            throw new ElasticsearchException("unexpected exception", e);
        }
    }

    public static void setState(MasterService executor, ClusterState clusterState) {
        CountDownLatch latch = new CountDownLatch(1);
        executor.submitUnbatchedStateUpdateTask("test setting state", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                // make sure we increment versions as listener may depend on it for change
                return ClusterState.builder(clusterState).build();
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected exception" + e);
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new ElasticsearchException("unexpected interruption", e);
        }
    }

    public static ClusterService createClusterService(ThreadPool threadPool) {
        DiscoveryNode discoveryNode = new DiscoveryNode(
            "node",
            ESTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.roles(),
            Version.CURRENT
        );
        return createClusterService(threadPool, discoveryNode);
    }

    public static ClusterService createClusterService(ThreadPool threadPool, DiscoveryNode localNode) {
        return createClusterService(threadPool, localNode, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
    }

    public static ClusterService createClusterService(ThreadPool threadPool, DiscoveryNode localNode, ClusterSettings clusterSettings) {
        Settings settings = Settings.builder().put("node.name", "test").put("cluster.name", "ClusterServiceTests").build();
        ClusterService clusterService = new ClusterService(settings, clusterSettings, threadPool);
        clusterService.setNodeConnectionsService(createNoOpNodeConnectionsService());
        ClusterState initialClusterState = ClusterState.builder(new ClusterName(ClusterServiceUtils.class.getSimpleName()))
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()))
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();
        clusterService.getClusterApplierService().setInitialState(initialClusterState);
        clusterService.getMasterService().setClusterStatePublisher(createClusterStatePublisher(clusterService.getClusterApplierService()));
        clusterService.getMasterService().setClusterStateSupplier(clusterService.getClusterApplierService()::state);
        clusterService.start();
        return clusterService;
    }

    public static ClusterService createSingleThreadedClusterService() {
        final var threadPool = new DeterministicTaskQueue().getThreadPool();
        final var clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final var localNode = new DiscoveryNode(
            "node",
            ESTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.roles(),
            Version.CURRENT
        );
        // run tasks inline, no need to fork to a separate thread
        final var directExecutor = new PrioritizedEsThreadPoolExecutor(
            "direct",
            1,
            1,
            1,
            TimeUnit.SECONDS,
            r -> { throw new AssertionError("should not create new threads"); },
            null,
            null,
            PrioritizedEsThreadPoolExecutor.StarvationWatcher.NOOP_STARVATION_WATCHER
        ) {
            @Override
            public void execute(Runnable command, final TimeValue timeout, final Runnable timeoutCallback) {
                command.run();
            }

            @Override
            public void execute(Runnable command) {
                command.run();
            }
        };

        final var clusterApplierService = new ClusterApplierService("test", Settings.EMPTY, clusterSettings, threadPool) {
            @Override
            protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
                return directExecutor;
            }
        };
        clusterApplierService.setInitialState(
            ClusterState.builder(new ClusterName(ClusterServiceUtils.class.getSimpleName()))
                .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()))
                .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
                .build()
        );

        final var masterService = new MasterService(Settings.EMPTY, clusterSettings, threadPool) {
            @Override
            protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
                return directExecutor;
            }
        };
        masterService.setClusterStatePublisher(createClusterStatePublisher(clusterApplierService));
        masterService.setClusterStateSupplier(clusterApplierService::state);

        final var clusterService = new ClusterService(Settings.EMPTY, clusterSettings, masterService, clusterApplierService);
        clusterService.setNodeConnectionsService(createNoOpNodeConnectionsService());
        clusterService.setRerouteService((s, p, r) -> r.onResponse(clusterApplierService.state()));
        clusterService.start();
        return clusterService;
    }

    public static NodeConnectionsService createNoOpNodeConnectionsService() {
        return new NodeConnectionsService(Settings.EMPTY, null, null) {
            @Override
            public void connectToNodes(DiscoveryNodes discoveryNodes, Runnable onCompletion) {
                // don't do anything
                onCompletion.run();
            }

            @Override
            public void disconnectFromNodesExcept(DiscoveryNodes nodesToKeep) {
                // don't do anything
            }
        };
    }

    public static ClusterStatePublisher createClusterStatePublisher(ClusterApplier clusterApplier) {
        return (clusterStatePublicationEvent, publishListener, ackListener) -> {
            setAllElapsedMillis(clusterStatePublicationEvent);
            clusterApplier.onNewClusterState(
                "mock_publish_to_self[" + clusterStatePublicationEvent.getSummary() + "]",
                clusterStatePublicationEvent::getNewState,
                publishListener
            );
        };
    }

    public static ClusterService createClusterService(ClusterState initialState, ThreadPool threadPool) {
        ClusterService clusterService = createClusterService(threadPool);
        setState(clusterService, initialState);
        return clusterService;
    }

    public static void setState(ClusterService clusterService, ClusterState.Builder clusterStateBuilder) {
        setState(clusterService, clusterStateBuilder.build());
    }

    /**
     * Sets the state on the cluster applier service
     */
    public static void setState(ClusterService clusterService, ClusterState clusterState) {
        setState(clusterService.getClusterApplierService(), clusterState);
    }

    public static void setAllElapsedMillis(ClusterStatePublicationEvent clusterStatePublicationEvent) {
        clusterStatePublicationEvent.setPublicationContextConstructionElapsedMillis(0L);
        clusterStatePublicationEvent.setPublicationCommitElapsedMillis(0L);
        clusterStatePublicationEvent.setPublicationCompletionElapsedMillis(0L);
        clusterStatePublicationEvent.setMasterApplyElapsedMillis(0L);
    }

    public static void awaitClusterState(Logger logger, Predicate<ClusterState> statePredicate, ClusterService clusterService)
        throws Exception {
        final ClusterStateObserver observer = new ClusterStateObserver(
            clusterService,
            null,
            logger,
            clusterService.getClusterApplierService().threadPool().getThreadContext()
        );
        if (statePredicate.test(observer.setAndGetObservedState()) == false) {
            final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    future.onResponse(null);
                }

                @Override
                public void onClusterServiceClose() {
                    future.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    assert false : "onTimeout called with no timeout set";
                }
            }, statePredicate);
            future.get(30L, TimeUnit.SECONDS);
        }
    }
}
