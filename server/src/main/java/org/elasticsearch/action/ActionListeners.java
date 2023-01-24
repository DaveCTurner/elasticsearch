/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class ActionListeners {
    private ActionListeners() {
        // no instances
    }

    public static <T> ActionListenerBuilder<T> noop() {
        return new ActionListenerBuilder<>(ActionListener.noop());
    }

    public static <T> ActionListenerBuilder<T> builder(ActionListener<T> listener) {
        return new ActionListenerBuilder<>(listener);
    }

    public static <T> ActionListenerBuilder<T> builder(Consumer<T> onResponse, Consumer<Exception> onFailure) {
        return new ActionListenerBuilder<>(new ActionListener<T>() {
            @Override
            public void onResponse(T t) {
                onResponse.accept(t);
            }

            @Override
            public void onFailure(Exception e) {
                onFailure.accept(e);
            }
        });
    }

    public static <T> ActionListenerBuilder<T> running(Runnable runnable) {
        return new ActionListenerBuilder<>(ActionListener.wrap(runnable));
    }

    public static <T> ActionListenerBuilder<T> releasing(Releasable releasable) {
        return new ActionListenerBuilder<>(ActionListener.releasing(releasable));
    }

    public static <T> ActionListenerBuilder<T> catching(CheckedConsumer<T, ? extends Exception> onResponse, Consumer<Exception> onFailure) {
        return new ActionListenerBuilder<>(ActionListener.wrap(onResponse, onFailure));
    }

    public static <T extends TransportResponse> TransportResponseHandler<T> responseHandler(
        String executor,
        Writeable.Reader<T> reader,
        ActionListenerBuilder<T> builder
    ) {
        return new ActionListenerResponseHandler<>(builder.build(), reader, executor);
    }

    public static class ActionListenerBuilder<T> {

        private ActionListener<?> listener;

        ActionListenerBuilder(ActionListener<T> listener) {
            this.listener = listener;
        }

        private boolean assertOpen() {
            assert listener != null : "already built";
            return true;
        }

        @SuppressWarnings("unchecked")
        public ActionListener<T> build() {
            assert assertOpen();
            try {
                return (ActionListener<T>) listener;
            } finally {
                listener = null;
            }
        }

        @SuppressWarnings("unchecked")
        public <U> ActionListenerBuilder<U> map(CheckedFunction<U, T, Exception> mapper) {
            assert assertOpen();
            listener = ((ActionListener<T>) listener).map(mapper);
            return (ActionListenerBuilder<U>) this;
        }

        @SuppressWarnings("unchecked")
        public <U> ActionListenerBuilder<U> onResponse(BiConsumer<ActionListener<T>, U> bc) {
            assert assertOpen();
            listener = ((ActionListener<T>) listener).delegateFailure(bc);
            return (ActionListenerBuilder<U>) this;
        }

        @SuppressWarnings("unchecked")
        public <U> ActionListenerBuilder<U> onFailure(BiConsumer<ActionListener<T>, Exception> bc) {
            assert assertOpen();
            listener = ((ActionListener<T>) listener).delegateResponse(bc);
            return (ActionListenerBuilder<U>) this;
        }

        public ActionListenerBuilder<T> feedbackFailure() {
            assert assertOpen();
            listener = ActionListener.wrap(listener);
            return this;
        }

        @SuppressWarnings("unchecked")
        public <U extends T> ActionListenerBuilder<U> cast() {
            assert assertOpen();
            return (ActionListenerBuilder<U>) this;
        }

        public ActionListenerBuilder<T> runAfter(Runnable runnable) {
            assert assertOpen();
            listener = ActionListener.runAfter(listener, runnable);
            return this;
        }

        public ActionListenerBuilder<T> releaseAfter(Releasable releasable) {
            assert assertOpen();
            listener = ActionListener.releaseAfter(listener, releasable);
            return this;
        }

        public ActionListenerBuilder<T> runBefore(CheckedRunnable<?> runnable) {
            assert assertOpen();
            listener = ActionListener.runBefore(listener, runnable);
            return this;
        }

        public ActionListenerBuilder<T> once() {
            assert assertOpen();
            listener = ActionListener.notifyOnce(listener);
            return this;
        }

        public ActionListenerBuilder<T> assertOnce() {
            assert assertOpen();
            listener = ActionListener.assertOnce(listener);
            return this;
        }

        public ActionListenerBuilder<T> withContext(ThreadContext threadContext) {
            assert assertOpen();
            listener = ContextPreservingActionListener.wrapPreservingContext(listener, threadContext);
            return this;
        }

        public ActionListenerBuilder<T> named(Supplier<String> nameSupplier) {
            assert assertOpen();
            @SuppressWarnings("unchecked")
            final var innerListener = (ActionListener<T>) listener;
            listener = new ActionListener<T>() {
                @Override
                public void onResponse(T t) {
                    innerListener.onResponse(t);
                }

                @Override
                public void onFailure(Exception e) {
                    innerListener.onFailure(e);
                }

                @Override
                public String toString() {
                    return nameSupplier.get();
                }
            };
            return this;
        }

        public ActionListenerBuilder<T> dispatching(ThreadPool threadPool, String executor) {
            assert assertOpen();
            listener = new ThreadedActionListener<>(null, threadPool, executor, listener, false);
            return this;
        }

        public ActionRunnable<T> suppliedBy(CheckedSupplier<T, Exception> supplier) {
            assert assertOpen();
            return ActionRunnable.supply(build(), supplier);
        }

        public ActionRunnable<T> executing(CheckedConsumer<ActionListener<T>, Exception> consumer) {
            assert assertOpen();
            return ActionRunnable.wrap(build(), consumer);
        }

    }
}
