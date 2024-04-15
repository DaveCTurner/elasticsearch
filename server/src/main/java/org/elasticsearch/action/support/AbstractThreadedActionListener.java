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
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.util.concurrent.Executor;

/**
 * Base class for action listeners that wrap another action listener and dispatch its completion to an executor.
 */
public abstract class AbstractThreadedActionListener<Response> implements ActionListener<Response> {

    private static final Logger logger = LogManager.getLogger(AbstractThreadedActionListener.class);

    protected final Executor executor;
    protected final ActionListener<Response> delegate;
    protected final boolean forceExecution;

    protected AbstractThreadedActionListener(Executor executor, boolean forceExecution, ActionListener<Response> delegate) {
        this.forceExecution = forceExecution;
        this.executor = executor;
        this.delegate = delegate;
    }

    @Override
    public final void onFailure(final Exception e) {
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
            public void onRejection(Exception rejectionException) {
                if (rejectionException != e) {
                    rejectionException.addSuppressed(e);
                }
                executor.execute(new RejectionRunnable(rejectionException));
            }

            @Override
            public void onFailure(Exception e) {
                onUnhandledFailure(e);
            }

            @Override
            public String toString() {
                return AbstractThreadedActionListener.this + "/onFailure";
            }
        });
    }

    private void onUnhandledFailure(Exception e) {
        logger.error(() -> "failed to execute failure callback on [" + this + "]", e);
        assert false : e;
    }

    @Override
    public final String toString() {
        return getClass().getSimpleName() + "[" + executor + "/" + delegate + "]";
    }

    protected void handleRejection(Exception rejectionException) {
        final var rejectionRunnable = new RejectionRunnable(rejectionException);
        if (rejectionException instanceof EsRejectedExecutionException esre && esre.isExecutorShutdown()) {
            // shutting down so no point in trying to use the executor any more - better to complete the listener on the calling thread (and
            // risk a stack overflow) rather than just dropping it
            rejectionRunnable.run();
        } else {
            executor.execute(rejectionRunnable);
        }
    }

    private class RejectionRunnable extends AbstractRunnable {
        private final Exception rejectionException;

        protected RejectionRunnable(Exception rejectionException) {
            this.rejectionException = rejectionException;
        }

        @Override
        protected void doRun() {
            delegate.onFailure(rejectionException);
        }

        @Override
        public boolean isForceExecution() {
            return true;
        }

        @Override
        public void onFailure(Exception e) {
            if (rejectionException != e) {
                rejectionException.addSuppressed(e);
            }
            onUnhandledFailure(rejectionException);
        }

        @Override
        public void onRejection(Exception e) {
            assert e instanceof EsRejectedExecutionException esre && esre.isExecutorShutdown() : e;
            // our isForceExecution() returns true, so a rejection means we must be shutting down - better to complete the listener on the
            // calling thread (and risk a stack overflow) rather than just dropping it
            if (rejectionException != e) {
                rejectionException.addSuppressed(e);
            }
            RejectionRunnable.this.run();
        }
    }
}
