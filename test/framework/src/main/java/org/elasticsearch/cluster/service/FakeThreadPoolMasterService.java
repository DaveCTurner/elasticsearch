/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStatePublicationEvent;
import org.elasticsearch.cluster.coordination.ClusterStatePublisher.AckListener;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.lucene.tests.util.LuceneTestCase.random;

public class FakeThreadPoolMasterService extends MasterService {

    private final String name;
    private final Consumer<Runnable> taskExecutor;
    private final ThreadContext threadContext;

    public FakeThreadPoolMasterService(String nodeName, String serviceName, ThreadPool threadPool, Consumer<Runnable> taskExecutor) {
        this(
            Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), nodeName).build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool,
            serviceName,
            taskExecutor
        );
    }

    private FakeThreadPoolMasterService(
        Settings settings,
        ClusterSettings clusterSettings,
        ThreadPool threadPool,
        String serviceName,
        Consumer<Runnable> taskExecutor
    ) {
        super(settings, clusterSettings, threadPool, new TaskManager(settings, threadPool, Set.of()));
        this.name = serviceName;
        this.taskExecutor = taskExecutor;
        this.threadContext = threadPool.getThreadContext();
    }

    @Override
    protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
        return new PrioritizedEsThreadPoolExecutor(
            name,
            1,
            1,
            1,
            TimeUnit.SECONDS,
            r -> { throw new AssertionError("should not create new threads"); },
            null,
            null
        ) {
            @Override
            public void execute(Runnable command, final TimeValue timeout, final Runnable timeoutCallback) {
                execute(command);
            }

            @Override
            public void execute(Runnable command) {
                taskExecutor.accept(threadContext.preserveContext(command));
            }
        };
    }

    @Override
    public ClusterState.Builder incrementVersion(ClusterState clusterState) {
        // generate cluster UUID deterministically for repeatable tests
        return ClusterState.builder(clusterState).incrementVersion().stateUUID(UUIDs.randomBase64UUID(random()));
    }

    @Override
    protected void publish(
        ClusterStatePublicationEvent clusterStatePublicationEvent,
        AckListener ackListener,
        ActionListener<Void> publicationListener
    ) {
        threadPool.generic().execute(threadPool.getThreadContext().preserveContext(new Runnable() {
            @Override
            public void run() {
                FakeThreadPoolMasterService.super.publish(clusterStatePublicationEvent, wrapAckListener(ackListener), publicationListener);
            }

            @Override
            public String toString() {
                return "publish change of cluster state from version ["
                    + clusterStatePublicationEvent.getOldState().version()
                    + "] in term ["
                    + clusterStatePublicationEvent.getOldState().term()
                    + "] to version ["
                    + clusterStatePublicationEvent.getNewState().version()
                    + "] in term ["
                    + clusterStatePublicationEvent.getNewState().term()
                    + "]";
            }
        }));
    }

    protected AckListener wrapAckListener(AckListener ackListener) {
        return ackListener;
    }
}
