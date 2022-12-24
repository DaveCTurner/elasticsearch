/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A mechanism to fan-in a collection of listeners into a single final listener. Basic usage is as follows:
 *
 * <pre>
 * try (var listeners = new RefCountingListeners(finalListener)) {
 *     for (var item : collection) {
 *         runAsyncAction(item, listeners.acquire());
 *     }
 * }
 * </pre>
 *
 * The final listener is completed when execution leaves the try-with-resources block and every acquired listener is completed. Unlike a
 * {@link GroupedActionListener} there is no need to declare the number of listeners up front, they can be acquired dynamically as needed.
 * Moreover even outside the try-with-resources block you can continue to acquire additional listeners, even in a separate thread, as long
 * as there's at least one listener outstanding:
 *
 * <pre>
 * try (var listeners = new RefCountingListeners(finalListener)) {
 *     for (var item : collection) {
 *         if (condition(item)) {
 *             runAsyncAction(item, listeners.acquire());
 *         }
 *     }
 *     if (flag) {
 *         runOneOffAsyncAction(listeners.acquire());
 *         return;
 *     }
 *     for (var item : otherCollection) {
 *         var itemListener = listeners.acquire(); // delays completion until the background action completes
 *         executorService.execute(() -> {
 *             if (condition(item)) {
 *                 runOtherAsyncAction(item, listeners.acquire());
 *             }
 *             itemListener.onResponse(null);
 *         });
 *     }
 * }
 * </pre>
 *
 * In particular this works even if you don't acquire any listeners at all: in that case, the final listener is completed at the end of the
 * try-with-resources block.
 */
public final class RefCountingListeners implements Releasable {

    private final ActionListener<Void> finalListener;
    private final AtomicReference<Exception> failure = new AtomicReference<>();
    private final ActionListener<Void> acquiredListener = new DecRefListener();
    private final RefCounted refs = AbstractRefCounted.of(this::onCompletion);

    /**
     * @param finalListener The listener to be notified when this {@link RefCountingListeners} is closed and all acquired listeners are
     *                      completed. If there are still pending listeners when the {@link RefCountingListeners} is closed then the final
     *                      listener is completed on the thread that last completed an acquired listener. If all acquired listeners
     *                      completed successfully then so is this listener; if any acquired listener failed then this listener is also
     *                      failed with the first exception captured, with all other exceptions suppressed. If this listener's
     *                      {@link ActionListener#onResponse} throws an exception then the thrown exception is passed to its
     *                      {@link ActionListener#onFailure} method; its {@link ActionListener#onFailure} method must not throw anything.
     */
    public RefCountingListeners(ActionListener<Void> finalListener) {
        this.finalListener = Objects.requireNonNull(finalListener);
    }

    public ActionListener<Void> acquire() {
        if (refs.tryIncRef()) {
            return acquiredListener;
        }
        assert false : "already closed, cannot acquire another listener";
        throw new AlreadyClosedException("already closed, cannot acquire another listener");
    }

    @Override
    public void close() {
        refs.decRef();
    }

    private void onCompletion() {
        final var finalFailure = failure.get();
        if (finalFailure == null) {
            try {
                finalListener.onResponse(null);
            } catch (RuntimeException e) {
                completeWithFailure(e);
            }
        } else {
            completeWithFailure(finalFailure);
        }
    }

    private void completeWithFailure(Exception e) {
        try {
            finalListener.onFailure(e);
        } catch (RuntimeException ex) {
            if (ex != e) {
                ex.addSuppressed(e);
            }
            assert false : new AssertionError("listener.onFailure failed", ex);
            throw ex;
        }
    }

    private class DecRefListener implements ActionListener<Void> {
        @Override
        public void onResponse(Void unused) {
            refs.decRef();
        }

        @Override
        public void onFailure(Exception e) {
            if (failure.compareAndSet(null, e) == false) {
                failure.accumulateAndGet(e, (current, update) -> {
                    // we have to avoid self-suppression!
                    if (update != current) {
                        // TODO maybe limit the number of suppressions?
                        current.addSuppressed(update);
                    }
                    return current;
                });
            }
            refs.decRef();
        }
    }
}
