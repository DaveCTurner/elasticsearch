/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.TaskTracerPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.hamcrest.Matchers;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class TaskTracerIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), CapturingTaskTracerPlugin.class);
    }

    public void testTaskTracer() throws Exception {
        ensureAllTasksCompleted();

        createIndex("test");
        ensureGreen("test");

        logger.info("--> indexing doc 1");
        index("test", null, "{}");
        logger.info("--> indexing doc 1 done");


        logger.info("--> indexing doc 2");
        index("test", null, "{}");
        logger.info("--> indexing doc 2 done");

        logger.info("--> searching index");
        client().prepareSearch("test").get();
        logger.info("--> done searching index");

    }

    private static void ensureAllTasksCompleted() throws Exception {
        assertBusy(() -> {
            for (CapturingTaskTracer capturingTaskTracer : internalCluster().getInstances(CapturingTaskTracer.class)) {
                capturingTaskTracer.assertAllTasksComplete();
            }
        });
    }

    public static class CapturingTaskTracerPlugin extends Plugin implements TaskTracerPlugin {

        private final SetOnce<CapturingTaskTracer> taskTracerRef = new SetOnce<>();

        @Override
        public Collection<Object> createComponents(
                Client client,
                ClusterService clusterService,
                ThreadPool threadPool,
                ResourceWatcherService resourceWatcherService,
                ScriptService scriptService,
                NamedXContentRegistry xContentRegistry,
                Environment environment,
                NodeEnvironment nodeEnvironment,
                NamedWriteableRegistry namedWriteableRegistry,
                IndexNameExpressionResolver indexNameExpressionResolver,
                Supplier<RepositoriesService> repositoriesServiceSupplier) {
            taskTracerRef.set(new CapturingTaskTracer(clusterService::localNode));
            clusterService.addLifecycleListener(new LifecycleListener() {
                @Override
                public void afterStart() {
                    taskTracerRef.get().localNode();
                }
            });
            return Collections.singletonList(taskTracerRef.get());
        }

        @Override
        public List<TaskTracingListener> getListeners() {
            assert taskTracerRef.get() != null;
            return Collections.singletonList(taskTracerRef.get());
        }

    }

    private static class CapturingTaskTracer implements TaskTracingListener {

        private static final Logger logger = LogManager.getLogger(CapturingTaskTracer.class);

        private final Set<Long> registeredTaskIds = ConcurrentCollections.newConcurrentSet();
        private final Set<Long> childRequestIds = ConcurrentCollections.newConcurrentSet();
        private final Supplier<DiscoveryNode> localNodeSupplier;

        public CapturingTaskTracer(Supplier<DiscoveryNode> localNodeSupplier) {
            this.localNodeSupplier = localNodeSupplier;
        }

        public void assertAllTasksComplete() {
            assertThat("no outstanding child requests on [" + localNode().getName() + "]", childRequestIds, Matchers.empty());
            assertThat("no registered tasks on [" + localNode().getName() + "]", registeredTaskIds, Matchers.empty());
        }

        @Override
        public void onTaskRegistered(Task task) {
            logger.info("-->   registered [{}][{}] with parent [{}]", new TaskId(localNode().getId(), task.getId()), task.getAction(), task.getParentTaskId());
            assertTrue(registeredTaskIds.add(task.getId()));
        }

        @Override
        public void onTaskUnregistered(Task task) {
            logger.info("--> unregistered [{}][{}]", new TaskId(localNode().getId(), task.getId()), task.getAction());
            assertTrue(registeredTaskIds.remove(task.getId()));
        }

        @Override
        public void onChildRequestStart(DiscoveryNode node, long requestId, String action, TaskId parentTask) {
            assert parentTask.getNodeId().equals(localNode().getId())
                    : parentTask + " running on node " + localNode();
            logger.info("-->   child request [{}][{}] of [{}] sent to node [{}]",
                    requestId, action, parentTask, node.getId());
            assertTrue(childRequestIds.add(requestId));
        }

        @Override
        public void onRequestComplete(long requestId) {
            logger.info("-->   child request [{}] on node [{}] completed",
                    requestId, localNode().getId());
            childRequestIds.remove(requestId); // may not be a child request, so no assertTrue here
        }

        DiscoveryNode localNode() {
            return localNodeSupplier.get();
        }

    }

}
