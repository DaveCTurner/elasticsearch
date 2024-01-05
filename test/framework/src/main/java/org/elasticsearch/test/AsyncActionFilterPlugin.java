/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomInt;

/**
 * Test plugin which sometimes runs actions asynchronously, similarly to certain paths through the security action filter, to ensure we
 * are not relying on synchronous execution of actions anywhere.
 */
public class AsyncActionFilterPlugin extends Plugin implements ActionPlugin {

    private static final String THREAD_NAME_PREFIX = AsyncActionFilterPlugin.class.getCanonicalName();

    private AsyncActionFilter asyncActionFilter;

    @Override
    public Collection<?> createComponents(PluginServices services) {
        asyncActionFilter = new AsyncActionFilter(
            NODE_NAME_SETTING.get(services.clusterService().getSettings()),
            services.threadPool().getThreadContext()
        );
        return List.of(asyncActionFilter);
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        return List.of(Objects.requireNonNull(asyncActionFilter));
    }

    private static class AsyncActionFilter extends AbstractLifecycleComponent implements ActionFilter {

        private final int order = randomInt();
        private final String nodeName;
        private final ThreadContext threadContext;
        private ExecutorService executorService;

        AsyncActionFilter(String nodeName, ThreadContext threadContext) {
            this.nodeName = nodeName;
            this.threadContext = threadContext;
        }

        @Override
        public int order() {
            return order;
        }

        @Override
        protected void doStart() {
            executorService = EsExecutors.newScaling(
                THREAD_NAME_PREFIX,
                0,
                EsExecutors.allocatedProcessors(Settings.EMPTY),
                60,
                TimeUnit.SECONDS,
                true,
                EsExecutors.daemonThreadFactory(nodeName, THREAD_NAME_PREFIX),
                threadContext
            );
        }

        @Override
        protected void doStop() {
            ThreadPool.terminate(executorService, 10, TimeUnit.SECONDS);
        }

        @Override
        protected void doClose() {}

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> void apply(
            Task task,
            String action,
            Request request,
            ActionListener<Response> listener,
            ActionFilterChain<Request, Response> chain
        ) {
            if (false /* TODO */ && randomBoolean()) {
                chain.proceed(task, action, request, listener);
            } else {
                request.mustIncRef();
                executorService.execute(new AbstractRunnable() {
                    @Override
                    protected void doRun() {
                        chain.proceed(task, action, request, listener);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }

                    @Override
                    public void onAfter() {
                        request.decRef();
                    }
                });
            }
        }
    }
}
