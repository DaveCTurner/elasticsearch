/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;

import java.util.Map;

@UpdateForV9 // this logging can be removed in v9
public class LoggingTaskListenerTests extends ESTestCase {

    private static Task createTask() {
        return new Task(randomNonNegativeLong(), "test", "test:action", "", TaskId.EMPTY_TASK_ID, Map.of());
    }

    private static SubscribableListener<Void> runTask(Task task, Settings empty) {
        final var listeners = new SubscribableListener<Void>();
        assertSame(task, LoggingTaskListener.<Void>runWithLoggingTaskListener(empty, l -> {
            listeners.addListener(l);
            return task;
        }));
        return listeners;
    }

    private void assertDeprecationWarning() {
        assertWarnings(Strings.format("""
            Logging the completion of a task using [org.elasticsearch.tasks.LoggingTaskListener] is deprecated and will be removed \
            in a future version. Instead, use the task management API [%s] to monitor long-running tasks for completion. To suppress \
            this warning and opt-in to the future behaviour now, set [tasks.logging_task_listener.enabled] to [false] on every \
            node.""", ReferenceDocs.TASK_MANAGEMENT_API));
    }

    public void testLogSuccess() {
        final var task = createTask();
        final var listeners = runTask(task, Settings.EMPTY);

        assertDeprecationWarning();
        MockLogAppender.assertThatLogger(
            () -> listeners.onResponse(null),
            LoggingTaskListener.class,
            new MockLogAppender.SeenEventExpectation(
                "completion",
                LoggingTaskListener.class.getCanonicalName(),
                Level.INFO,
                task.getId() + " finished with response null"
            )
        );
    }

    public void testLogFailure() {
        final var task = createTask();
        final var listeners = runTask(task, Settings.EMPTY);

        assertDeprecationWarning();
        MockLogAppender.assertThatLogger(
            () -> listeners.onFailure(new ElasticsearchException("simulated")),
            LoggingTaskListener.class,
            new MockLogAppender.SeenEventExpectation(
                "completion",
                LoggingTaskListener.class.getCanonicalName(),
                Level.WARN,
                task.getId() + " failed with exception"
            )
        );
    }

    private static final Settings NO_LOG_SETTINGS = Settings.builder()
        .put(LoggingTaskListener.LOGGING_TASK_LISTENER_ENABLED_SETTING.getKey(), false)
        .build();

    public void testNoLogSuccess() {
        final var task = createTask();
        final var listeners = runTask(task, NO_LOG_SETTINGS);

        MockLogAppender.assertThatLogger(
            () -> listeners.onResponse(null),
            LoggingTaskListener.class,
            new MockLogAppender.LoggingExpectation() {
                @Override
                public void match(LogEvent event) {
                    fail("should see no log messages");
                }

                @Override
                public void assertMatched() {}
            }
        );
    }

    public void testNoLogFailure() {
        final var task = createTask();
        final var listeners = runTask(task, NO_LOG_SETTINGS);

        MockLogAppender.assertThatLogger(
            () -> listeners.onFailure(new ElasticsearchException("simulated")),
            LoggingTaskListener.class,
            new MockLogAppender.LoggingExpectation() {
                @Override
                public void match(LogEvent event) {
                    fail("should see no log messages");
                }

                @Override
                public void assertMatched() {}
            }
        );
    }
}
