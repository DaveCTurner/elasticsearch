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
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.Releasable;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;

import static org.elasticsearch.core.AbstractRefCounted.wrapToString;

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
 * The delegate action is completed when execution leaves the try-with-resources block and every acquired reference is released. Unlike a
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
 * In particular (and also unlike a {@link CountDown}) this works even if you don't acquire any extra refs at all: in that case, the
 * delegate action executes at the end of the try-with-resources block.
 */
public final class RefCountedRunnable implements Releasable {

    private static final Logger logger = LogManager.getLogger(RefCountedRunnable.class);

    private final Runnable delegate;
    private final AtomicInteger refCount = new AtomicInteger(1);

    /**
     * Construct a {@link RefCountedRunnable} which executes {@code delegate} when all refs are released.
     * @param delegate The action to execute when all refs are released. This action must not throw any exception.
     */
    public RefCountedRunnable(Runnable delegate) {
        this.delegate = delegate;
    }

    private static final IntUnaryOperator INC_REF = i -> i > 0 ? i + 1 : 0;
    private static final IntUnaryOperator DEC_REF = i -> i > 0 ? i - 1 : 0;

    /**
     * Acquire a reference to this object and return an action which releases it. The delegate {@link Runnable} is called when all its
     * references have been released.
     */
    public Releasable acquire() {
        ensureValidCount(refCount.getAndUpdate(INC_REF));
        // closing ourselves releases a ref, so we can just return 'this' and avoid any allocation; callers only see a Releasable
        return this;
    }

    /**
     * Acquire a reference to this object and return a listener which releases it when notified. The delegate {@link Runnable} is called
     * when all its references have been released.
     */
    public ActionListener<Void> acquireListener() {
        return ActionListener.releasing(acquire());
    }

    /**
     * Release a reference to this object, and execute the delegate {@link Runnable} if there are no other references.
     */
    @Override
    public void close() {
        if (ensureValidCount(refCount.getAndUpdate(DEC_REF)) == 1) {
            try {
                delegate.run();
            } catch (Exception e) {
                logger.error("exception in delegate", e);
                assert false : e;
            }
        }
    }

    @Override
    public String toString() {
        return wrapToString(delegate.toString());
    }

    static final String ALREADY_CLOSED_MESSAGE = "already closed, cannot acquire or release any further refs";

    private static int ensureValidCount(int count) {
        assert 0 < count : ALREADY_CLOSED_MESSAGE;
        return count;
    }
}
