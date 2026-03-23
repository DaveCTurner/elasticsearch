/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsExecutors.TaskTrackingConfig;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThreadedActionListenerTests extends ESTestCase {

    public void testRejectionHandling() throws InterruptedException {
        final var listenerCount = between(1, 1000);
        final var startLatch = new CountDownLatch(between(1, listenerCount));
        final var finishLatch = new CountDownLatch(listenerCount);
        final var threadPool = new TestThreadPool(
            "test",
            Settings.EMPTY,
            new FixedExecutorBuilder(
                Settings.EMPTY,
                "fixed-bounded-queue",
                between(1, 3),
                10,
                "fbq",
                randomFrom(TaskTrackingConfig.DEFAULT, TaskTrackingConfig.DO_NOT_TRACK)
            ),
            new FixedExecutorBuilder(
                Settings.EMPTY,
                "fixed-unbounded-queue",
                between(1, 3),
                -1,
                "fnq",
                randomFrom(TaskTrackingConfig.DEFAULT, TaskTrackingConfig.DO_NOT_TRACK)
            ),
            new ScalingExecutorBuilder("scaling-drop-if-shutdown", between(1, 3), between(3, 5), TimeValue.timeValueSeconds(1), false),
            new ScalingExecutorBuilder("scaling-reject-if-shutdown", between(1, 3), between(3, 5), TimeValue.timeValueSeconds(1), true)
        );
        final var closeFlag = new AtomicBoolean();
        try {
            final var pools = randomNonEmptySubsetOf(
                List.of("fixed-bounded-queue", "fixed-unbounded-queue", "scaling-drop-if-shutdown", "scaling-reject-if-shutdown")
            );
            final var shutdownUnsafePools = Set.of("fixed-bounded-queue", "scaling-drop-if-shutdown");

            threadPool.generic().execute(() -> {
                try {
                    logger.info("--> using [{}] listeners", listenerCount);
                    for (int i = 0; i < listenerCount; i++) {
                        final var pool = randomFrom(pools);
                        final var forceExecution = (pool.equals("fixed-bounded-queue") || pool.startsWith("scaling")) && rarely();
                        final var listenerDescription = Strings.format(
                            "listener [%04d] on pool [%s] with forceExecution=%s",
                            i,
                            pool,
                            forceExecution
                        );
                        final var listener = new ThreadedActionListener<Void>(
                            threadPool.executor(pool),
                            forceExecution,
                            ActionListener.runAfter(new ActionListener<>() {
                                @Override
                                public void onResponse(Void ignored) {
                                    logger.info("--> OUTCOME [{}] completed successfully", listenerDescription);
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    logger.info("--> OUTCOME [{}] failed: {}", listenerDescription, e.getMessage());
                                    assertNull(e.getCause());
                                    if (e instanceof EsRejectedExecutionException esRejectedExecutionException) {
                                        assertTrue(esRejectedExecutionException.isExecutorShutdown());
                                        if (e.getSuppressed().length == 0) {
                                            return;
                                        }
                                        assertEquals(1, e.getSuppressed().length);
                                        if (e.getSuppressed()[0] instanceof ElasticsearchException elasticsearchException) {
                                            e = elasticsearchException;
                                            assertNull(e.getCause());
                                        } else {
                                            fail(e);
                                        }
                                    }

                                    if (e instanceof ElasticsearchException) {
                                        assertEquals("simulated", e.getMessage());
                                        assertEquals(0, e.getSuppressed().length);
                                    } else {
                                        fail(e);
                                    }

                                }
                            }, finishLatch::countDown)
                        );
                        startLatch.countDown();
                        Thread.yield();
                        synchronized (closeFlag) {
                            if (closeFlag.get() && shutdownUnsafePools.contains(pool)) {
                                // closing, so tasks submitted to this pool may just be dropped
                                logger.info("--> OUTCOME [{}] dropping as unsafe at shutdown", listenerDescription);
                                finishLatch.countDown();
                            } else if (randomBoolean()) {
                                logger.info("--> [{}] completing successfully", listenerDescription);
                                listener.onResponse(null);
                            } else {
                                logger.info("--> [{}] failing", listenerDescription);
                                listener.onFailure(new ElasticsearchException("simulated"));
                            }
                        }
                        Thread.yield();
                    }
                } catch (Exception e) {
                    logger.error("boom", e);
                    throw e;
                }
            });
        } finally {
            if (startLatch.await(10, TimeUnit.SECONDS) == false) {
                HotThreads.logLocalCurrentThreads(logger, Level.INFO, "thread dump on start latch failure");
                fail("start latch failed");
            }
            logger.info("--> startLatch released");
            synchronized (closeFlag) {
                logger.info("--> closing threadpool");
                assertTrue(closeFlag.compareAndSet(false, true));
                threadPool.shutdown();
                logger.info("--> closed threadpool");
            }
            assertTrue(threadPool.awaitTermination(10, TimeUnit.SECONDS));
            logger.info("--> threadpool terminated");
        }
        if (finishLatch.await(10, TimeUnit.SECONDS) == false) {
            HotThreads.logLocalCurrentThreads(logger, Level.INFO, "thread dump on finish latch failure");
            fail("finish latch failed");
        }
        logger.info("--> latch complete");
    }

    public void testToString() {
        var deterministicTaskQueue = new DeterministicTaskQueue();

        assertEquals(
            "ThreadedActionListener[DeterministicTaskQueue/forkingExecutor/NoopActionListener]",
            new ThreadedActionListener<Void>(deterministicTaskQueue.getThreadPool().generic(), randomBoolean(), ActionListener.noop())
                .toString()
        );

        assertEquals(
            "ThreadedActionListener[DeterministicTaskQueue/forkingExecutor/NoopActionListener]/onResponse",
            safeAwait(listener -> new ThreadedActionListener<Void>(deterministicTaskQueue.getThreadPool(s -> {
                listener.onResponse(s.toString());
                return s;
            }).generic(), randomBoolean(), ActionListener.noop()).onResponse(null))
        );

        assertEquals(
            "ThreadedActionListener[DeterministicTaskQueue/forkingExecutor/NoopActionListener]/onFailure",
            safeAwait(listener -> new ThreadedActionListener<Void>(deterministicTaskQueue.getThreadPool(s -> {
                listener.onResponse(s.toString());
                return s;
            }).generic(), randomBoolean(), ActionListener.noop()).onFailure(new ElasticsearchException("test")))
        );
    }

}
