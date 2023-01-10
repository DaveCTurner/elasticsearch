/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Releasable;

import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A mechanism to complete a listener on the completion of some (dynamic) collection of other actions. Basic usage is as follows:
 *
 * <pre>
 * try (var refs = new RefCountingListener(10, finalListener)) {
 *     for (var item : collection) {
 *         runAsyncAction(item, refs.acquire()); // completes the acquired listener on completion
 *     }
 * }
 * </pre>
 *
 * The delegate listener is completed when execution leaves the try-with-resources block and every acquired reference is released. Unlike a
 * {@link GroupedActionListener} there is no need to declare the number of subsidiary listeners up front: listeners can be acquired
 * dynamically as needed. Moreover even outside the try-with-resources block you can continue to acquire additional listeners, even in a
 * separate thread, as long as there's at least one listener outstanding:
 *
 * <pre>
 * try (var refs = new RefCountingListener(10, finalListener)) {
 *     for (var item : collection) {
 *         if (condition(item)) {
 *             runAsyncAction(item, refs.acquire());
 *         }
 *     }
 *     if (flag) {
 *         runOneOffAsyncAction(refs.acquire());
 *         return;
 *     }
 *     for (var item : otherCollection) {
 *         var itemRef = refs.acquire(); // delays completion while the background action is pending
 *         executorService.execute(() -> {
 *             try (var ignored = itemRef) {
 *                 if (condition(item)) {
 *                     runOtherAsyncAction(item, refs.acquire());
 *                 }
 *             }
 *         });
 *     }
 * }
 * </pre>
 *
 * In particular (and also unlike a {@link GroupedActionListener}) this works even if you don't acquire any extra refs at all: in that case,
 * the delegate listener is completed at the end of the try-with-resources block.
 */
public final class RefCountingListener implements Releasable {

    private final ActionListener<Void> delegate;
    private final RefCountingRunnable refs = new RefCountingRunnable(this::finish);

    private final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
    private final Semaphore exceptionPermits;
    private final AtomicInteger droppedExceptionsRef = new AtomicInteger();

    /**
     * Construct a {@link RefCountingListener} which completes {@code delegate} when all refs are released.
     * @param delegate The listener to complete when all refs are released. This listener must not throw any exception on completion. If all
     *                 the acquired listeners completed successfully then so is the delegate. If any of the acquired listeners completed
     *                 with failure then the delegate is completed with the first exception received, with other exceptions added to its
     *                 collection of suppressed exceptions.
     * @deprecated This imposes no limit on the number of exceptions accumulated, which could cause substantial memory use. Prefer to limit
     *             the number of accumulated exceptions with {@link #RefCountingListener(int, ActionListener)} instead.
     */
    @Deprecated
    public RefCountingListener(ActionListener<Void> delegate) {
        this(Integer.MAX_VALUE, delegate);
    }

    /**
     * Construct a {@link RefCountingListener} which completes {@code delegate} when all refs are released.
     * @param delegate The listener to complete when all refs are released. This listener must not throw any exception on completion. If all
     *                 the acquired listeners completed successfully then so is the delegate. If any of the acquired listeners completed
     *                 with failure then the delegate is completed with the first exception received, with other exceptions added to its
     *                 collection of suppressed exceptions.
     * @param maxExceptions The maximum number of exceptions to accumulate on failure.
     */
    public RefCountingListener(int maxExceptions, ActionListener<Void> delegate) {
        if (maxExceptions <= 0) {
            assert false : maxExceptions;
            throw new IllegalArgumentException("maxExceptions must be positive");
        }
        this.delegate = Objects.requireNonNull(delegate);
        this.exceptionPermits = new Semaphore(maxExceptions);
    }

    @Override
    public void close() {
        refs.close();
    }

    private void finish() {
        var exception = exceptionRef.get();
        if (exception == null) {
            delegate.onResponse(null);
        } else {
            final var droppedExceptions = droppedExceptionsRef.getAndSet(0);
            if (droppedExceptions > 0) {
                exception.addSuppressed(new ElasticsearchException(droppedExceptions + " further exceptions were dropped"));
            }
            delegate.onFailure(exception);
        }
    }

    public ActionListener<Void> acquire() {
        return new ActionListener<>() {
            private final Releasable ref = refs.acquire();

            @Override
            public void onResponse(Void unused) {
                ref.close();
            }

            @Override
            public void onFailure(Exception e) {
                if (exceptionPermits.tryAcquire()) {
                    final var firstException = exceptionRef.compareAndExchange(null, e);
                    if (firstException != null && firstException != e) {
                        firstException.addSuppressed(e);
                    }
                } else {
                    droppedExceptionsRef.incrementAndGet();
                }
                ref.close();
            }

            @Override
            public String toString() {
                return "refCounted[" + delegate + "]";
            }
        };
    }

    @Override
    public String toString() {
        return "refCounting[" + delegate + "]";
    }
}
