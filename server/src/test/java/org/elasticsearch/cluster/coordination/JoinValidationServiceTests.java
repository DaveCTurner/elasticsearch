/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.CloseableConnection;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class JoinValidationServiceTests extends ESTestCase {
    public void testConcurrentBehaviour() throws Exception {
        final var releasables = new ArrayList<Releasable>();
        try {
            final var settingsBuilder = Settings.builder();
            settingsBuilder.put(
                JoinValidationService.JOIN_VALIDATION_CACHE_TIMEOUT_SETTING.getKey(),
                TimeValue.timeValueMillis(between(1, 1000))
            );
            if (randomBoolean()) {
                settingsBuilder.put("thread_pool.cluster_coordination.size", between(1, 5));
            }
            final var settings = settingsBuilder.build();

            final var threadPool = new TestThreadPool("test", settings);
            releasables.add(() -> ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));

            final var transport = new MockTransport() {
                @Override
                public Connection createConnection(DiscoveryNode node) {
                    return new CloseableConnection() {
                        @Override
                        public DiscoveryNode getNode() {
                            return node;
                        }

                        @Override
                        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                            throws TransportException {
                            final var executor = threadPool.executor(
                                randomFrom(ThreadPool.Names.SAME, ThreadPool.Names.GENERIC, ThreadPool.Names.CLUSTER_COORDINATION)
                            );
                            executor.execute(() -> handleResponse(requestId, switch (action) {
                                case JoinHelper.JOIN_VALIDATE_CLUSTER_STATE_ACTION_NAME -> TransportResponse.Empty.INSTANCE;
                                case TransportService.HANDSHAKE_ACTION_NAME -> new TransportService.HandshakeResponse(
                                    Version.CURRENT,
                                    Build.CURRENT.hash(),
                                    node,
                                    ClusterName.DEFAULT
                                );
                                default -> throw new AssertionError("unexpected action: " + action);
                            }));
                        }
                    };
                }
            };

            final var localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);

            final var transportService = new TransportService(
                settings,
                transport,
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                ignored -> localNode,
                null,
                Set.of()
            );
            releasables.add(transportService);

            final var clusterState = ClusterState.EMPTY_STATE;

            final var joinValidationService = new JoinValidationService(settings, transportService, () -> clusterState);

            transportService.start();
            releasables.add(() -> {
                if (transportService.lifecycleState() == Lifecycle.State.STARTED) {
                    transportService.stop();
                }
            });

            transportService.acceptIncomingRequests();

            final var otherNodes = new DiscoveryNode[between(1, 10)];
            for (int i = 0; i < otherNodes.length; i++) {
                otherNodes[i] = new DiscoveryNode("other-" + i, buildNewFakeTransportAddress(), Version.CURRENT);
                final var connectionListener = new PlainActionFuture<Releasable>();
                transportService.connectToNode(otherNodes[i], connectionListener);
                releasables.add(connectionListener.get(10, TimeUnit.SECONDS));
            }

            final var threads = new Thread[between(1, 5)];
            final var startBarrier = new CyclicBarrier(threads.length + 1);
            final var permitCount = 1000;
            final var validationPermits = new Semaphore(permitCount);
            final var expectFailures = new AtomicBoolean(false);
            final var keepGoing = new AtomicBoolean(true);
            for (int i = 0; i < threads.length; i++) {
                final var seed = randomLong();
                threads[i] = new Thread(() -> {
                    final var random = new Random(seed);
                    try {
                        startBarrier.await(10, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }

                    while (keepGoing.get()) {
                        Thread.yield();
                        if (validationPermits.tryAcquire()) {
                            joinValidationService.validateJoin(
                                randomFrom(random, otherNodes),
                                ActionListener.notifyOnce(new ActionListener<>() {
                                    @Override
                                    public void onResponse(TransportResponse.Empty empty) {
                                        validationPermits.release();
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        validationPermits.release();
                                        assert expectFailures.get() : e;
                                    }
                                })
                            );
                        }
                    }
                }, "join-validating-thread-" + i);
                threads[i].start();
            }

            startBarrier.await(10, TimeUnit.SECONDS);
            Thread.yield();

            if (randomBoolean()) {
                logger.info("--> stopping join validation service");
                expectFailures.set(true);
                joinValidationService.stop();

                if (randomBoolean()) {
                    logger.info("--> stopping transport service");
                    expectFailures.set(true);
                    transportService.stop();
                    if (randomBoolean()) {
                        logger.info("--> closing transport service");
                        expectFailures.set(true);
                        transportService.close();
                    }
                }
                if (randomBoolean()) {
                    logger.info("--> terminating thread pool");
                    expectFailures.set(true);
                    ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
                }
            }

            logger.info("--> joining threads");
            keepGoing.set(false);
            for (final var thread : threads) {
                thread.join();
            }

            logger.info("--> awaiting completion of all listeners");
            assertTrue(validationPermits.tryAcquire(permitCount, 10, TimeUnit.SECONDS));
            logger.info("--> awaiting cleanup");
            assertBusy(() -> assertTrue(joinValidationService.isIdle()));
            logger.info("--> done");
        } finally {
            Collections.reverse(releasables);
            Releasables.close(releasables);
        }
    }
}
