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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

public final class FanOutListener<T> implements ActionListener<T> {

    private static final Logger logger = LogManager.getLogger(FanOutListener.class);
    private static final Object EMPTY = new Object();

    /**
     *  If we are incomplete, {@code ref} may refer to one of the following depending on how many waiting subscribers there are:
     *  <ul>
     *  <li>If there are no subscribers yet, {@code ref} refers to {@link #EMPTY}.
     *  <li>If there is one subscriber, {@code ref} refers to it directly.
     *  <li>If there are more than one subscriber, {@code ref} refers to the head of a linked list of subscribers in reverse order of
     *  their subscriptions.
     *  </ul>
     *  If we are complete, {@code ref} refers to a {@code Consumer<ActionListener<T>>} which will complete any subsequent subscribers.
     */
    private final AtomicReference<Object> ref = new AtomicReference<>(EMPTY);

    private static class Cell {
        final ActionListener<?> listener;
        Cell next;

        Cell(ActionListener<?> listener, Cell next) {
            this.listener = listener;
            this.next = next;
        }
    }

    /**
     * Add a listener to this listener's collection of subscribers. If this listener is complete, this method completes the subscribing
     * listener immediately with the result with which this listener was completed. Otherwise, the subscribing listener is retained and
     * completed when this listener is completed.
     *
     * Listeners added strictly before this listener is completed will themselves be completed in the order in which their subscriptions
     * were received. However, there are no guarantees about the ordering of the completions of listeners which are added concurrently with
     * (or after) the completion of this listener.
     *
     * @param threadContext If not {@code null}, and the subscribing listener is not completed immediately, then it will be completed in
     *                      the given thread context.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void addListener(ThreadContext threadContext, ActionListener<T> listener) {
        final var wrappedListener = threadContext == null
            ? listener
            : ContextPreservingActionListener.wrapPreservingContext(listener, threadContext);
        var currentValue = ref.compareAndExchange(EMPTY, wrappedListener);
        if (currentValue == EMPTY) {
            return;
        }
        Cell newCell = null;
        while (true) {
            if (currentValue instanceof Consumer completer) {
                completer.accept(listener);
                return;
            }
            if (currentValue instanceof ActionListener firstListener) {
                final var tail = new Cell(firstListener, null);
                currentValue = ref.compareAndExchange(firstListener, tail);
                if (currentValue == firstListener) {
                    currentValue = tail;
                }
                continue;
            }
            if (currentValue instanceof Cell head) {
                if (newCell == null) {
                    newCell = new Cell(wrappedListener, head);
                } else {
                    newCell.next = head;
                }
                currentValue = ref.compareAndExchange(head, newCell);
                if (currentValue == head) {
                    return;
                }
            } else {
                assert false : "unexpected witness: " + currentValue;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void setConsumer(Consumer<ActionListener<T>> consumer) {
        var currentValue = ref.get();
        while (true) {
            if (currentValue instanceof Consumer) {
                // already complete - nothing to do
                return;
            }
            final var witness = ref.compareAndExchange(currentValue, consumer);
            if (witness != currentValue) {
                // we lost a race with another setConsumer call - retry
                currentValue = witness;
                continue;
            }

            if (currentValue == EMPTY) {
                // no subscribers yet - nothing to do
                return;
            }
            if (currentValue instanceof ActionListener<?> listener) {
                // unique subscriber - complete it
                // noinspection unchecked
                consumer.accept((ActionListener<T>) listener);
                return;
            }
            if (currentValue instanceof Cell currCell) {
                // multiple subscribers, but they are currently in reverse order of subscription so reverse them back
                Cell prevCell = null;
                while (true) {
                    final var nextCell = currCell.next;
                    currCell.next = prevCell;
                    if (nextCell == null) {
                        break;
                    }
                    prevCell = currCell;
                    currCell = nextCell;
                }
                // now they are in subscription order, complete them
                while (currCell != null) {
                    // noinspection unchecked
                    consumer.accept((ActionListener<T>) currCell.listener);
                    currCell = currCell.next;
                }
            } else {
                assert false : "unexpected witness: " + currentValue;
            }
        }
    }

    private void safeOnResponse(ActionListener<T> listener, T response) {
        try {
            listener.onResponse(response);
        } catch (Exception exception) {
            safeOnFailure(listener, exception);
        }
    }

    private void safeOnFailure(ActionListener<T> listener, Exception exception) {
        try {
            listener.onFailure(exception);
        } catch (Exception innerException) {
            if (exception != innerException) {
                exception.addSuppressed(innerException);
            }
            logger.error(Strings.format("exception thrown while handling another exception in listener [%s]", listener), exception);
            assert false : exception;
            // nothing more can be done here
        }
    }

    @Override
    public void onResponse(T response) {
        setConsumer(l -> safeOnResponse(l, response));
    }

    @Override
    public void onFailure(Exception exception) {
        setConsumer(l -> safeOnFailure(l, exception));
    }

    /**
     * @return {@code true} if and only if this listener has been completed (either successfully or exceptionally).
     */
    public boolean isDone() {
        return ref.get() instanceof Consumer;
    }

    /**
     * @return the result with which this listener completed successfully, or throw the exception with which it failed.
     * @throws AssertionError if this listener is not complete yet and assertions are enabled.
     * @throws IllegalStateException if this listener is not complete yet and assertions are disabled.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public T result() {
        if (ref.get()instanceof Consumer consumer) {
            class ResultListener implements ActionListener<T> {
                Supplier<T> resultSupplier;

                @Override
                public void onResponse(T t) {
                    assert resultSupplier == null;
                    resultSupplier = () -> t;
                }

                @Override
                public void onFailure(Exception e) {
                    assert resultSupplier == null;
                    resultSupplier = () -> { throw e instanceof RuntimeException re ? re : new ElasticsearchException(e); };
                }

                T result() {
                    assert resultSupplier != null;
                    return resultSupplier.get();
                }
            }

            var resultListener = new ResultListener();
            // noinspection unchecked
            consumer.accept(resultListener);
            return resultListener.result();
        } else {
            assert false : "not done";
            throw new IllegalStateException("listener is not done, cannot get result yet");
        }
    }
}
