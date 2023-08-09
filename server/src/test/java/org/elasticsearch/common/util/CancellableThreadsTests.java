/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.util;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.common.util.CancellableThreads.Interruptible;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.elasticsearch.common.util.CancellableThreads.ExecutionCancelledException;
import static org.hamcrest.Matchers.equalTo;

public class CancellableThreadsTests extends ESTestCase {
    public static class CustomException extends RuntimeException {
        public CustomException(String msg) {
            super(msg);
        }
    }

    public static class IOCustomException extends IOException {
        public IOCustomException(String msg) {
            super(msg);
        }
    }

    static class ThrowOnCancelException extends RuntimeException {}

    private class TestPlan {
        public final int id;
        public final boolean busySpin;
        public final boolean exceptBeforeCancel;
        public final boolean exitBeforeCancel;
        public final boolean exceptAfterCancel;
        public final boolean presetInterrupt;
        public final boolean ioOp;

        private TestPlan(int id) {
            this.id = id;
            this.busySpin = randomBoolean();
            this.exceptBeforeCancel = randomBoolean();
            this.exitBeforeCancel = randomBoolean();
            this.exceptAfterCancel = randomBoolean();
            this.presetInterrupt = randomBoolean();
            this.ioOp = randomBoolean();
        }
    }

    static class TestRunnable implements Interruptible {
        final TestPlan plan;
        final CountDownLatch readyForCancel;

        TestRunnable(TestPlan plan, CountDownLatch readyForCancel) {
            this.plan = plan;
            this.readyForCancel = readyForCancel;
        }

        @Override
        public void run() throws InterruptedException {
            assertFalse("interrupt thread should have been clear", Thread.currentThread().isInterrupted());
            if (plan.exceptBeforeCancel) {
                throw new CustomException("thread [" + plan.id + "] pre-cancel exception");
            } else if (plan.exitBeforeCancel) {
                return;
            }
            readyForCancel.countDown();
            try {
                if (plan.busySpin) {
                    while (Thread.currentThread().isInterrupted() == false) {
                    }
                } else {
                    Thread.sleep(50000);
                }
            } finally {
                if (plan.exceptAfterCancel) {
                    throw new CustomException("thread [" + plan.id + "] post-cancel exception");
                }
            }
        }
    }

    public void testCancellableThreads() throws InterruptedException {
        Thread[] threads = new Thread[randomIntBetween(3, 10)];
        final TestPlan[] plans = new TestPlan[threads.length];
        final Exception[] exceptions = new Exception[threads.length];
        final boolean[] interrupted = new boolean[threads.length];
        final CancellableThreads cancellableThreads = new CancellableThreads();
        final CountDownLatch readyForCancel = new CountDownLatch(threads.length);
        for (int i = 0; i < threads.length; i++) {
            final TestPlan plan = new TestPlan(i);
            plans[i] = plan;
            threads[i] = new Thread(() -> {
                try {
                    if (plan.presetInterrupt) {
                        Thread.currentThread().interrupt();
                    }
                    cancellableThreads.execute(new TestRunnable(plan, readyForCancel));
                } catch (Exception e) {
                    exceptions[plan.id] = e;
                }
                if (plan.exceptBeforeCancel || plan.exitBeforeCancel) {
                    // we have to mark we're ready now (actually done).
                    readyForCancel.countDown();
                }
                interrupted[plan.id] = Thread.currentThread().isInterrupted();
            });
            threads[i].setDaemon(true);
            threads[i].start();
        }

        readyForCancel.await();
        final boolean throwInOnCancel = randomBoolean();
        final AtomicInteger invokeTimes = new AtomicInteger();
        cancellableThreads.setOnCancel((reason, beforeCancelException) -> {
            invokeTimes.getAndIncrement();
            if (throwInOnCancel) {
                ThrowOnCancelException e = new ThrowOnCancelException();
                if (beforeCancelException != null) {
                    e.addSuppressed(beforeCancelException);
                }
                throw e;
            }
        });

        cancellableThreads.cancel("test");
        for (Thread thread : threads) {
            thread.join(20000);
            assertFalse(thread.isAlive());
        }
        for (int i = 0; i < threads.length; i++) {
            TestPlan plan = plans[i];
            if (plan.exceptBeforeCancel) {
                assertThat(exceptions[i], Matchers.instanceOf(CustomException.class));
            } else if (plan.exitBeforeCancel) {
                assertNull(exceptions[i]);
            } else {
                // in all other cases, we expect a cancellation exception.
                if (throwInOnCancel) {
                    assertThat(exceptions[i], Matchers.instanceOf(ThrowOnCancelException.class));
                } else {
                    assertThat(exceptions[i], Matchers.instanceOf(ExecutionCancelledException.class));
                }
                if (plan.exceptAfterCancel) {
                    assertThat(exceptions[i].getSuppressed(), Matchers.arrayContaining(Matchers.instanceOf(CustomException.class)));
                } else {
                    assertThat(exceptions[i].getSuppressed(), Matchers.emptyArray());
                }
            }
            assertThat(interrupted[plan.id], equalTo(plan.presetInterrupt));
        }
        assertThat(
            invokeTimes.longValue(),
            equalTo(Arrays.stream(plans).filter(p -> p.exceptBeforeCancel == false && p.exitBeforeCancel == false).count())
        );
        if (throwInOnCancel) {
            expectThrows(ThrowOnCancelException.class, cancellableThreads::checkForCancel);
        } else {
            expectThrows(ExecutionCancelledException.class, cancellableThreads::checkForCancel);
        }
        assertThat(
            invokeTimes.longValue(),
            equalTo(Arrays.stream(plans).filter(p -> p.exceptBeforeCancel == false && p.exitBeforeCancel == false).count() + 1)
        );
    }

    public void testGetUninterruptibly() throws Exception {
        final var future = new PlainActionFuture<Integer>();
        final var counter = new AtomicInteger();
        final var cancellableThreads = new CancellableThreads();
        final var workers = new Thread[between(1, 5)];
        final var completionBlock = new CountDownLatch(1);

        try (var refs = new RefCountingRunnable(() -> future.onResponse(counter.get()))) {
            for (int i = 0; i < workers.length; i++) {
                final var barrier = new CyclicBarrier(2);
                final var ref = refs.acquire();
                workers[i] = new Thread(
                    () -> assertEquals(
                        "operation was cancelled reason [interrupted during getUninterruptibly]",
                        expectThrows(ExecutionCancelledException.class, () -> {
                            try (ref) {
                                cancellableThreads.execute(() -> {
                                    safeAwait(barrier);
                                    try {
                                        if (randomBoolean()) {
                                            Thread.sleep(Long.MAX_VALUE); // throws InterruptedException
                                        } else {
                                            LockSupport.parkNanos(Long.MAX_VALUE); // just continues on interrupt
                                        }
                                    } finally {
                                        counter.incrementAndGet();
                                        safeAwait(completionBlock);
                                    }
                                });
                            }
                        }).getMessage()
                    )
                );
                workers[i].start();
                safeAwait(barrier);
            }
        }

        final var waitFinished = new AtomicBoolean();
        final var waiter = new Thread(() -> {
            try {
                assertEquals(workers.length, cancellableThreads.getUninterruptibly(future).intValue());
                assertTrue(Thread.currentThread().isInterrupted());
                waitFinished.set(true);
            } catch (ExecutionException e) {
                throw new AssertionError("unexpected", e);
            }
        });
        waiter.start();

        for (int i = between(1, 3); i > 0; i--) {
            LockSupport.parkNanos(TimeValue.timeValueMillis(50).nanos());
            waiter.interrupt();
        }

        assertFalse(future.isDone());
        assertFalse(waitFinished.get());

        completionBlock.countDown();
        waiter.join();
        assertTrue(future.isDone());
        assertTrue(waitFinished.get());

        for (final var worker : workers) {
            worker.join();
        }
    }
}
