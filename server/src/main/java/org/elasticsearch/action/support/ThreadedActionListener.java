/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.ExecutorService;

/**
 * A listener wrapper which dispatches the completion of the delegate listener to a separate executor.
 */
public final class ThreadedActionListener<Response> extends ActionListener.Delegating<Response, Response> {

    private static final Logger logger = LogManager.getLogger(ThreadedActionListener.class);

    private final ExecutorService executor;
    private final boolean forceExecution;

    /**
     * Marker constant for places that maybe should capture the caller's thread context, but don't today. TODO remove this.
     */
    public static final ThreadContext TODO_SHOULD_THIS_CAPTURE_CONTEXT = null;

    /**
     * When this {@link ThreadedActionListener} completes, it dispatches a task to complete {@code listener} on the specified executor,
     * preserving the thread context in which this constructor was called. If the execution is rejected then {@code listener} is completed
     * with an exception on the calling thread.
     */
    public ThreadedActionListener(ThreadPool threadPool, String executor, ActionListener<Response> listener, boolean forceExecution) {
        this(threadPool.executor(executor), threadPool.getThreadContext(), listener, forceExecution);
    }

    /**
     * When this {@link ThreadedActionListener} completes, it dispatches a task to complete {@code listener} on the specified executor. If
     * the execution is rejected then {@code listener} is completed with an exception on the calling thread.
     *
     * @param threadContext If null, {@code listener} is completed in the context of the thread that completed this listener; if non-null,
     *                      {@code listener} is completed in the context in which this {@link ThreadedActionListener} was constructed.
     */
    public ThreadedActionListener(
        ExecutorService executor,
        @Nullable ThreadContext threadContext,
        ActionListener<Response> listener,
        boolean forceExecution
    ) {
        super(wrapListener(listener, threadContext));
        this.executor = executor;
        this.forceExecution = forceExecution;
    }

    private static <Response> ActionListener<Response> wrapListener(ActionListener<Response> listener, ThreadContext threadContext) {
        if (threadContext == null) {
            return listener;
        } else {
            return ContextPreservingActionListener.wrapPreservingContext(listener, threadContext);
        }
    }

    @Override
    public void onResponse(final Response response) {
        executor.execute(new ActionRunnable<>(delegate) {
            @Override
            public boolean isForceExecution() {
                return forceExecution;
            }

            @Override
            protected void doRun() {
                listener.onResponse(response);
            }

            @Override
            public String toString() {
                return ThreadedActionListener.this + "/onResponse";
            }
        });
    }

    @Override
    public void onFailure(final Exception e) {
        executor.execute(new AbstractRunnable() {
            @Override
            public boolean isForceExecution() {
                return forceExecution;
            }

            @Override
            protected void doRun() {
                delegate.onFailure(e);
            }

            @Override
            public void onRejection(Exception e2) {
                e.addSuppressed(e2);
                try {
                    delegate.onFailure(e);
                } catch (Exception e3) {
                    e.addSuppressed(e3);
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(() -> "failed to execute failure callback: [" + ThreadedActionListener.this + "]", e);
                assert false : e;
            }

            @Override
            public String toString() {
                return ThreadedActionListener.this + "/onFailure";
            }
        });
    }

    @Override
    public String toString() {
        return "ThreadedActionListener[" + executor + "/" + delegate + "]";
    }
}
