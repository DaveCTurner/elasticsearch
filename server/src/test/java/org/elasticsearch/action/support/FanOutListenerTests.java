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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.TestBarrier;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;

public class FanOutListenerTests extends ESTestCase {

    private static class OrderAssertingRunnable implements Runnable {
        private final int index;
        private final AtomicInteger order;

        OrderAssertingRunnable(int index, AtomicInteger order) {
            this.index = index;
            this.order = order;
        }

        @Override
        public void run() {
            assertTrue(order.compareAndSet(index, index + 1));
        }
    }

    public void testSubscriptionOrder() {
        var listener = new FanOutListener<>();
        var order = new AtomicInteger();

        var subscriberCount = between(0, 4);
        for (int i = 0; i < subscriberCount; i++) {
            listener.addListener(null, ActionListener.wrap(new OrderAssertingRunnable(i, order)));
        }

        assertEquals(0, order.get());

        if (randomBoolean()) {
            listener.onResponse(new Object());
        } else {
            listener.onFailure(new ElasticsearchException("test"));
        }

        assertEquals(subscriberCount, order.get());
        listener.addListener(null, ActionListener.wrap(new OrderAssertingRunnable(subscriberCount, order)));
        assertEquals(subscriberCount + 1, order.get());
        listener.addListener(null, ActionListener.wrap(new OrderAssertingRunnable(subscriberCount + 1, order)));
        assertEquals(subscriberCount + 2, order.get());
    }

    public void testOnResponse() {
        var listener = new FanOutListener<>();
        var order = new AtomicInteger();
        var expectedResponse = new Object();

        IntFunction<ActionListener<Object>> listenerFactory = i -> ActionListener.runAfter(
            ActionListener.wrap(o -> assertSame(o, expectedResponse), e -> fail()),
            new OrderAssertingRunnable(i, order)
        );

        var subscriberCount = between(0, 4);
        for (int i = 0; i < subscriberCount; i++) {
            listener.addListener(null, listenerFactory.apply(i));
        }

        assertEquals(0, order.get());
        listener.onResponse(expectedResponse);
        assertEquals(subscriberCount, order.get());

        assertEquals(subscriberCount, order.get());
        listener.addListener(null, ActionListener.wrap(new OrderAssertingRunnable(subscriberCount, order)));
        assertEquals(subscriberCount + 1, order.get());

        if (randomBoolean()) {
            listener.onResponse(new Object());
        } else {
            listener.onFailure(new ElasticsearchException("test"));
        }

        listener.addListener(null, ActionListener.wrap(new OrderAssertingRunnable(subscriberCount + 1, order)));
        assertEquals(subscriberCount + 2, order.get());
    }

    public void testOnFailure() {
        var listener = new FanOutListener<>();
        var order = new AtomicInteger();
        var expectedException = new ElasticsearchException("test");

        IntFunction<ActionListener<Object>> listenerFactory = i -> ActionListener.runAfter(
            ActionListener.wrap(o -> fail(), e -> assertSame(e, expectedException)),
            new OrderAssertingRunnable(i, order)
        );

        var subscriberCount = between(0, 4);
        for (int i = 0; i < subscriberCount; i++) {
            listener.addListener(null, listenerFactory.apply(i));
        }

        assertEquals(0, order.get());
        listener.onFailure(expectedException);
        assertEquals(subscriberCount, order.get());

        assertEquals(subscriberCount, order.get());
        listener.addListener(null, ActionListener.wrap(new OrderAssertingRunnable(subscriberCount, order)));
        assertEquals(subscriberCount + 1, order.get());

        if (randomBoolean()) {
            listener.onResponse(new Object());
        } else {
            listener.onFailure(new ElasticsearchException("test"));
        }

        listener.addListener(null, ActionListener.wrap(new OrderAssertingRunnable(subscriberCount + 1, order)));
        assertEquals(subscriberCount + 2, order.get());
    }

    public void testThreadContext() {
        final var listener = new FanOutListener<>();
        final var threadContext = new ThreadContext(Settings.EMPTY);
        final var subscriberCount = between(1, 5);
        final var completedListeners = new AtomicInteger();

        for (int i = 0; i < subscriberCount; i++) {
            try (var ignored = threadContext.stashContext()) {
                final var headerValue = randomAlphaOfLength(5);
                threadContext.putHeader("test-header", headerValue);
                listener.addListener(threadContext, ActionListener.wrap(() -> {
                    assertEquals(headerValue, threadContext.getHeader("test-header"));
                    completedListeners.incrementAndGet();
                }));
            }
        }

        assertEquals(0, completedListeners.get());
        listener.onResponse(null);
        assertEquals(subscriberCount, completedListeners.get());
    }

    public void testConcurrency() throws InterruptedException {
        final var threadContext = new ThreadContext(Settings.EMPTY);
        final var listener = new FanOutListener<>();
        final var subscriberThreads = between(0, 10);
        final var completerThreads = between(1, 10);
        final var barrier = new TestBarrier(subscriberThreads + completerThreads);

        final var completerThread = new AtomicReference<String>();
        final var winningValue = new AtomicReference<>();
        final var threads = new ArrayList<Thread>();
        final var responses = new HashMap<String, Object>();
        for (int i = 0; i < subscriberThreads; i++) {
            final var threadName = "subscriber-" + i;
            threads.add(new Thread(() -> {
                barrier.await(10, TimeUnit.SECONDS);
                try (var ignored = threadContext.stashContext()) {
                    final var headerValue = randomAlphaOfLength(5);
                    threadContext.putHeader("test-header", headerValue);
                    listener.addListener(threadContext, new ActionListener<>() {
                        @Override
                        public void onResponse(Object o) {
                            assertEquals(headerValue, threadContext.getHeader("test-header"));

                            winningValue.compareAndSet(null, o);
                            assertSame(winningValue.get(), o);

                            var currentThreadName = Thread.currentThread().getName();
                            if (currentThreadName.equals(threadName) == false) {
                                completerThread.compareAndSet(null, currentThreadName);
                                assertEquals(completerThread.get(), currentThreadName);
                                assertSame(responses.get(currentThreadName), o);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            onResponse(e);
                        }
                    });
                }
            }, threadName));
        }
        for (int i = 0; i < completerThreads; i++) {
            final var threadName = "completer-" + i;
            final var thisResponse = randomFrom(new Object(), new ElasticsearchException("test"));
            responses.put(threadName, thisResponse);
            threads.add(new Thread(() -> {
                barrier.await(10, TimeUnit.SECONDS);
                if (thisResponse instanceof Exception e) {
                    listener.onFailure(e);
                } else {
                    listener.onResponse(thisResponse);
                }
            }, threadName));
        }

        for (final var thread : threads) {
            thread.start();
        }
        for (final var thread : threads) {
            thread.join();
        }
    }

}
