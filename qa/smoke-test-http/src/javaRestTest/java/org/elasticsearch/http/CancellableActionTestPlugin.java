/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.junit.Assert;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.ExceptionsHelper.unwrapCause;
import static org.elasticsearch.test.ESIntegTestCase.internalCluster;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.safeAwait;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Utility plugin that captures the invocation of an action on a node, cancels it (e.g. by closing the client connection), verifies that
 * the corresponding task is cancelled, then lets the action execution proceed and finally verifies that the action completes exceptionally
 * with a {@link TaskCancelledException}.
 */
public class CancellableActionTestPlugin extends Plugin implements ActionPlugin {

    interface BlockedAction extends Releasable {
        void captureAndCancel(Runnable doCancel);
    }

    static BlockedAction capturingActionOnNode(String actionName, String nodeName) {
        final var plugins = internalCluster().getInstance(PluginsService.class, nodeName)
            .filterPlugins(CancellableActionTestPlugin.class)
            .toList();

        final var captureListener = new SubscribableListener<Captured>();
        plugins.forEach(plugin -> {
            plugin.capturedActionName = actionName;
            plugin.capturedRef.set(captureListener);
        });

        return new BlockedAction() {
            @Override
            public void captureAndCancel(Runnable doCancel) {
                final var completionLatch = new CountDownLatch(1);
                captureListener.onResponse(new Captured(doCancel, completionLatch));
                safeAwait(completionLatch);
            }

            @Override
            public void close() {
                plugins.forEach(plugin -> Assert.assertNull(plugin.capturedRef.get()));
            }
        };
    }

    private volatile String capturedActionName;
    private final AtomicReference<SubscribableListener<Captured>> capturedRef = new AtomicReference<>();

    private record Captured(Runnable doCancel, CountDownLatch countDownLatch) {}

    @Override
    public List<ActionFilter> getActionFilters() {
        return List.of(new ActionFilter() {

            private final int order = randomInt();

            @Override
            public int order() {
                return order;
            }

            @Override
            public <FilterRequest extends ActionRequest, FilterResponse extends ActionResponse> void apply(
                Task task,
                String action,
                FilterRequest request,
                ActionListener<FilterResponse> listener,
                ActionFilterChain<FilterRequest, FilterResponse> chain
            ) {
                if (action.equals(capturedActionName)) {
                    final var capturingListener = capturedRef.getAndSet(null);
                    if (capturingListener != null) {
                        final var cancellableTask = ESTestCase.asInstanceOf(CancellableTask.class, task);
                        capturingListener.addListener(ActionTestUtils.assertNoFailureListener(captured -> {
                            cancellableTask.addListener(() -> chain.proceed(task, action, request, new ActionListener<>() {
                                @Override
                                public void onResponse(FilterResponse filterResponse) {
                                    fail("cancelled action should not succeed, but got " + filterResponse);
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    assertThat(unwrapCause(e), instanceOf(TaskCancelledException.class));
                                    listener.onFailure(e);
                                    captured.countDownLatch().countDown();
                                }
                            }));
                            assertFalse(cancellableTask.isCancelled());
                            captured.doCancel().run();
                        }));
                        return;
                    }
                }

                chain.proceed(task, action, request, listener);
            }
        });
    }
}
