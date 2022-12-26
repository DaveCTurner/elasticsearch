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
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;

/**
 * A mechanism to trigger an action on the completion of some (dynamic) collection of other actions. Basic usage is as follows:
 *
 * <pre>
 * try (var refs = new RefCountedRunnable(finalRunnable)) {
 *     for (var item : collection) {
 *         runAsyncAction(item, refs.acquire());
 *     }
 * }
 * </pre>
 *
 * The final action is completed when execution leaves the try-with-resources block and every acquired reference is released. Unlike a
 * {@link CountDown} there is no need to declare the number of subsidiary actions up front (refs can be acquired dynamically as needed) nor
 * does the caller need to check for completion each time a reference is released. Moreover even outside the try-with-resources block you
 * can continue to acquire additional listeners, even in a separate thread, as long as there's at least one listener outstanding:
 *
 * <pre>
 * try (var refs = new RefCountedRunnable(finalRunnable)) {
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
 * In particular (and also unlike a {@link CountDown}) this works even if you don't acquire any refs at all: in that case, the final action
 * executes at the end of the try-with-resources block.
 */
public final class RefCountedRunnable implements Releasable {

    private final RefCounted refs;
    private final Releasable acquired = new DecRefReleasable();

    public RefCountedRunnable(Runnable delegate) {
        this.refs = AbstractRefCounted.of(delegate);
    }

    private static final String ALREADY_CLOSED_MESSAGE = "already closed, cannot acquire another ref";

    public Releasable acquire() {
        if (refs.tryIncRef()) {
            return acquired;
        }
        assert false : ALREADY_CLOSED_MESSAGE;
        throw new AlreadyClosedException(ALREADY_CLOSED_MESSAGE);
    }

    public ActionListener<Void> acquireListener() {
        return ActionListener.releasing(acquire());
    }

    @Override
    public void close() {
        refs.decRef();
    }

    private class DecRefReleasable implements Releasable {
        @Override
        public void close() {
            refs.decRef();
        }

        @Override
        public String toString() {
            return refs.toString();
        }
    }

    @Override
    public String toString() {
        return refs.toString();
    }
}
