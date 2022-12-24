/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.util.concurrent.EsExecutors.DIRECT_EXECUTOR_SERVICE;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class RefCountingListenersTests extends ESTestCase {

    public void testNotifications() throws Exception {
        AtomicBoolean called = new AtomicBoolean(false);
        ActionListener<Void> result = new ActionListener<>() {
            @Override
            public void onResponse(Void ignored) {
                assertTrue(called.compareAndSet(false, true));
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        };
        final var groupSize = between(10, 1000);
        Thread[] threads = new Thread[randomIntBetween(1, 5)];
        CyclicBarrier barrier = new CyclicBarrier(threads.length + 1);
        try (var listeners = new RefCountingListeners(result)) {
            final var queue = ConcurrentCollections.<ActionListener<Void>>newQueue();
            for (int i = 0; i < groupSize; i++) {
                queue.add(listeners.acquire());
            }

            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(() -> {
                    try {
                        barrier.await(10, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                    while (true) {
                        var listener = queue.poll();
                        if (listener == null) {
                            return;
                        }
                        listener.onResponse(null);
                    }
                });
                threads[i].start();
            }
        }
        barrier.await(10, TimeUnit.SECONDS);
        for (Thread t : threads) {
            t.join();
        }
        assertTrue(called.get());
    }

    public void testFailed() {
        AtomicReference<Exception> excRef = new AtomicReference<>();

        ActionListener<Void> result = new ActionListener<>() {
            @Override
            public void onResponse(Void ignored) {
                fail("unexpected success");
            }

            @Override
            public void onFailure(Exception e) {
                assertNotNull(e);
                assertTrue(excRef.compareAndSet(null, e));
            }
        };
        IOException ioException = new IOException();
        RuntimeException rtException = new RuntimeException();
        try (var listeners = new RefCountingListeners(result)) {
            listeners.acquire().onResponse(null);
            listeners.acquire().onFailure(rtException);
            listeners.acquire().onFailure(ioException);
            if (randomBoolean()) {
                listeners.acquire().onResponse(null);
            }
        }

        assertNotNull(excRef.get());
        assertEquals(rtException, excRef.get());
        assertEquals(1, excRef.get().getSuppressed().length);
        assertEquals(ioException, excRef.get().getSuppressed()[0]);
    }

    public void testValidation() throws InterruptedException {
        AtomicBoolean called = new AtomicBoolean(false);
        ActionListener<Void> result = new ActionListener<>() {
            @Override
            public void onResponse(Void ignored) {
                assertTrue(called.compareAndSet(false, true));
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(called.compareAndSet(false, true));
            }
        };

        // noinspection resource
        expectThrows(NullPointerException.class, () -> new RefCountingListeners(null));

        final int overage = randomIntBetween(0, 10);
        AtomicInteger assertionsTriggered = new AtomicInteger();
        final int groupSize = randomIntBetween(10, 1000);
        AtomicInteger count = new AtomicInteger();
        Thread[] threads = new Thread[randomIntBetween(2, 5)];

        try (var listeners = new RefCountingListeners(result)) {
            final var childListeners = new ArrayList<ActionListener<Void>>(groupSize);
            for (int i = 0; i < groupSize; i++) {
                childListeners.add(listeners.acquire());
            }

            CyclicBarrier barrier = new CyclicBarrier(threads.length);
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(() -> {
                    try {
                        barrier.await(10, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                    int c;
                    while ((c = count.incrementAndGet()) <= groupSize + overage) {
                        var listener = randomFrom(childListeners);
                        try {
                            if (c % 10 == 1) { // a mix of failures and non-failures
                                listener.onFailure(new RuntimeException());
                            } else {
                                listener.onResponse(null);
                            }
                        } catch (AssertionError e) {
                            assertionsTriggered.incrementAndGet();
                        }
                    }
                });
                threads[i].start();
            }
        }
        for (Thread t : threads) {
            t.join();
        }
        assertTrue(called.get());
        assertEquals(overage, assertionsTriggered.get());
    }

    public void testConcurrentFailures() throws InterruptedException {
        AtomicReference<Exception> finalException = new AtomicReference<>();
        int numGroups = randomIntBetween(10, 100);
        try (var listeners = new RefCountingListeners(ActionListener.wrap(r -> fail("unexpected success"), finalException::set))) {
            ExecutorService executorService = Executors.newFixedThreadPool(numGroups);
            for (int i = 0; i < numGroups; i++) {
                var listener = listeners.acquire();
                executorService.submit(() -> listener.onFailure(new IOException()));
            }

            executorService.shutdown();
            assertTrue(executorService.awaitTermination(10, TimeUnit.SECONDS));
        }

        Exception exception = finalException.get();
        assertNotNull(exception);
        assertThat(exception, instanceOf(IOException.class));
        assertEquals(numGroups - 1, exception.getSuppressed().length);
    }

    /*
     * It can happen that the same exception causes a grouped listener to be notified of the failure multiple times. Since we suppress
     * additional exceptions into the first exception, we have to guard against suppressing into the same exception, which could occur if we
     * are notified of with the same failure multiple times. This test verifies that the guard against self-suppression remains.
     */
    public void testRepeatNotificationForTheSameException() {
        final AtomicReference<Exception> finalExceptionHolder = new AtomicReference<>();
        final Exception expectedException = new Exception();
        try (var listeners = new RefCountingListeners(ActionListener.wrap(r -> fail("unexpected success"), finalExceptionHolder::set))) {
            // repeat notification for the same exception
            listeners.acquire().onFailure(expectedException);
            listeners.acquire().onFailure(expectedException);
        }

        final var finalException = finalExceptionHolder.get();
        assertThat(finalException, not(nullValue()));
        assertThat(finalException, equalTo(expectedException));
        assertEquals(0, finalException.getSuppressed().length);
    }

    public void testJavaDocExample() {
        final var finalListener = new PlainActionFuture<Void>();
        runExample(finalListener);
        assertTrue(finalListener.isDone());
    }

    private void runExample(PlainActionFuture<Void> finalListener) {
        final var collection = randomList(10, Object::new);
        final var otherCollection = randomList(10, Object::new);
        final var flag = randomBoolean();
        @SuppressWarnings("UnnecessaryLocalVariable")
        final var executorService = DIRECT_EXECUTOR_SERVICE;

        try (var listeners = new RefCountingListeners(finalListener)) {
            for (var item : collection) {
                if (condition(item)) {
                    runAsyncAction(item, listeners.acquire());
                }
            }
            if (flag) {
                runOneOffAsyncAction(listeners.acquire());
                return;
            }
            for (var item : otherCollection) {
                var itemListener = listeners.acquire(); // delays completion until the background action completes
                executorService.execute(() -> {
                    if (condition(item)) {
                        runOtherAsyncAction(item, listeners.acquire());
                    }
                    itemListener.onResponse(null);
                });
            }
        }
    }

    @SuppressWarnings("unused")
    private boolean condition(Object item) {
        return randomBoolean();
    }

    @SuppressWarnings("unused")
    private void runAsyncAction(Object item, ActionListener<Void> listener) {
        listener.onResponse(null);
    }

    @SuppressWarnings("unused")
    private void runOtherAsyncAction(Object item, ActionListener<Void> listener) {
        listener.onResponse(null);
    }

    private void runOneOffAsyncAction(ActionListener<Void> listener) {
        listener.onResponse(null);
    }
}
