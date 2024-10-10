/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.persistent;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class PersistentTaskInitializationFailureIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(FailingInitializationPersistentTasksPlugin.class);
    }

    public void testPersistentTasksThatFailDuringInitializationAreRemovedFromClusterState() throws Exception {
        final var persistentTasksService = internalCluster().getInstance(PersistentTasksService.class);
        final var clusterService = internalCluster().getInstance(ClusterService.class);

        final var taskCreatedAndRemovedListener = ClusterServiceUtils
            // listen for the task creation
            .addTemporaryStateListener(
                clusterService,
                clusterState -> findTasks(clusterState, FailingInitializationPersistentTaskExecutor.TASK_NAME).isEmpty() == false
            )
            // then listen for its removal
            .<Void>andThen(
                l -> ClusterServiceUtils.addTemporaryStateListener(
                    clusterService,
                    clusterState -> findTasks(clusterState, FailingInitializationPersistentTaskExecutor.TASK_NAME).isEmpty()
                ).addListener(l)
            );

        final var startRequestFuture = new PlainActionFuture<>();
        persistentTasksService.sendStartRequest(
            UUIDs.base64UUID(),
            FailingInitializationPersistentTaskExecutor.TASK_NAME,
            new FailingInitializationTaskParams(),
            null,
            startRequestFuture.map(ignored -> null)
        );
        safeAwait(taskCreatedAndRemovedListener);
        startRequestFuture.result(); // shouldn't throw
    }

    public static class FailingInitializationPersistentTasksPlugin extends Plugin implements PersistentTaskPlugin {
        @Override
        public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
            ClusterService clusterService,
            ThreadPool threadPool,
            Client client,
            SettingsModule settingsModule,
            IndexNameExpressionResolver expressionResolver
        ) {
            return List.of(new FailingInitializationPersistentTaskExecutor());
        }

        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return List.of(
                new NamedWriteableRegistry.Entry(
                    PersistentTaskParams.class,
                    FailingInitializationPersistentTaskExecutor.TASK_NAME,
                    FailingInitializationTaskParams::new
                )
            );
        }

        @Override
        public List<NamedXContentRegistry.Entry> getNamedXContent() {
            return List.of(
                new NamedXContentRegistry.Entry(
                    PersistentTaskParams.class,
                    new ParseField(FailingInitializationPersistentTaskExecutor.TASK_NAME),
                    p -> {
                        p.skipChildren();
                        return new FailingInitializationTaskParams();
                    }
                )
            );
        }
    }

    public static class FailingInitializationTaskParams implements PersistentTaskParams {
        public FailingInitializationTaskParams() {}

        public FailingInitializationTaskParams(StreamInput in) {}

        @Override
        public String getWriteableName() {
            return FailingInitializationPersistentTaskExecutor.TASK_NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        public void writeTo(StreamOutput out) {}

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().endObject();
        }
    }

    static class FailingInitializationPersistentTaskExecutor extends PersistentTasksExecutor<FailingInitializationTaskParams> {
        static final String TASK_NAME = "cluster:admin/persistent/test_init_failure";

        FailingInitializationPersistentTaskExecutor() {
            super(TASK_NAME, r -> fail("execution is unexpected"));
        }

        @Override
        protected AllocatedPersistentTask createTask(
            long id,
            String type,
            String action,
            TaskId parentTaskId,
            PersistentTasksCustomMetadata.PersistentTask<FailingInitializationTaskParams> taskInProgress,
            Map<String, String> headers
        ) {
            return new AllocatedPersistentTask(id, type, action, "", parentTaskId, headers) {
                @Override
                protected void init(
                    PersistentTasksService persistentTasksService,
                    TaskManager taskManager,
                    String persistentTaskId,
                    long allocationId
                ) {
                    throw new RuntimeException("simulated exception from task init");
                }
            };
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask task, FailingInitializationTaskParams params, PersistentTaskState state) {
            fail("execution is unexpected");
        }
    }
}
