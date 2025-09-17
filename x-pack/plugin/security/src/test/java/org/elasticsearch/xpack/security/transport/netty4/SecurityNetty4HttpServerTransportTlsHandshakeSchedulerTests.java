/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport.netty4;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleUserEventChannelHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.http.AbstractHttpServerTransportTestCase;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.MetricRecorder;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.Instrument;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.netty4.Netty4Plugin;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.elasticsearch.transport.netty4.TLSConfig;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.hamcrest.Matchers;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.elasticsearch.telemetry.InstrumentType.LONG_ASYNC_COUNTER;
import static org.elasticsearch.telemetry.InstrumentType.LONG_GAUGE;
import static org.elasticsearch.test.SecuritySettingsSource.addSSLSettingsForNodePEMFiles;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@TestLogging(reason = "nocommit", value = "org.elasticsearch.http.netty4:DEBUG")
public class SecurityNetty4HttpServerTransportTlsHandshakeSchedulerTests extends AbstractHttpServerTransportTestCase {

    /*
     * Idea: use io.netty.handler.ssl.SslClientHelloHandler to wait until we've received a full `ClientHello` message, then schedule
     * the handshake work, either passing the handshake on directly or putting it in a LIFO queue.
     *
     * But this is going to make testing a bit tricky: we can set up clients that withhold e.g. the last byte of their `ClientHello`
     * (and indeed we want to test that even if we have loads of clients waiting at this point then nothing is throttled).
     *
     * But also we want to test that the work is scheduled appropriately, so ideally we need some way to inject some kind of barrier on the
     * server side.
     */

    /**
     * Setup {@link Netty4HttpServerTransport} with SSL enabled and self-signed certificate.
     * All HTTP requests accumulate in the dispatcher reqQueue.
     * The server will not reply to request automatically, to send response poll the queue.
     */
    private Netty4HttpServerTransport createServerTransport(
        ThreadPool threadPool,
        SharedGroupFactory sharedGroupFactory,
        int maxConcurrentTlsHandshakes,
        int maxDelayedTlsHandshakes,
        Queue<ActionListener<Void>> handshakeBlockPromiseQueue,
        MeterRegistry meterRegistry
    ) {
        final Settings.Builder builder = Settings.builder();
        addSSLSettingsForNodePEMFiles(builder, "xpack.security.http.", randomBoolean());
        final var settings = builder.put("xpack.security.http.ssl.enabled", true)
            .put("path.home", createTempDir())
            .put(Netty4Plugin.SETTING_HTTP_NETTY_TLS_HANDSHAKES_MAX_CONCURRENT.getKey(), maxConcurrentTlsHandshakes)
            .put(Netty4Plugin.SETTING_HTTP_NETTY_TLS_HANDSHAKES_MAX_DELAYED.getKey(), maxDelayedTlsHandshakes)
            .build();
        final var env = TestEnvironment.newEnvironment(settings);
        final var sslService = new SSLService(env);
        final var tlsConfig = new TLSConfig(sslService.profile(XPackSettings.HTTP_SSL_PREFIX)::engine);

        final var inflightHandshakesByEventLoop = ConcurrentCollections.<Thread, AtomicInteger>newConcurrentMap();

        final List<Setting<?>> settingsSet = new ArrayList<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingsSet.add(Netty4Plugin.SETTING_HTTP_NETTY_TLS_HANDSHAKES_MAX_CONCURRENT);
        settingsSet.add(Netty4Plugin.SETTING_HTTP_NETTY_TLS_HANDSHAKES_MAX_DELAYED);

        final var telemetryProvider = meterRegistry == MeterRegistry.NOOP ? TelemetryProvider.NOOP : new TelemetryProvider() {
            @Override
            public Tracer getTracer() {
                return Tracer.NOOP;
            }

            @Override
            public MeterRegistry getMeterRegistry() {
                return meterRegistry;
            }
        };

        final var server = new Netty4HttpServerTransport(
            settings,
            new NetworkService(Collections.emptyList()),
            threadPool,
            xContentRegistry(),
            NEVER_CALLED_DISPATCHER,
            new ClusterSettings(settings, Set.copyOf(settingsSet)),
            sharedGroupFactory,
            telemetryProvider,
            tlsConfig,
            null,
            null
        ) {
            @Override
            public ChannelHandler configureServerChannelHandler() {
                return new HttpChannelHandler(this, handlingSettings, tlsConfig, null, null) {
                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        super.initChannel(ch);

                        final var workerThread = Thread.currentThread();
                        final var handshakeCounter = inflightHandshakesByEventLoop.computeIfAbsent(
                            workerThread,
                            ignored -> new AtomicInteger()
                        );
                        final var handshakeCompletePromise = new SubscribableListener<>();
                        final var handshakeBlockPromise = new SubscribableListener<Void>();

                        final var handshakeStartRecorder = new RunOnce(() -> {
                            logger.info("--> handshake start detected on [{}]", ch);
                            assertThat(handshakeCounter.incrementAndGet(), Matchers.lessThanOrEqualTo(maxConcurrentTlsHandshakes));
                            handshakeCompletePromise.addListener(ActionListener.running(handshakeCounter::decrementAndGet));
                        });

                        final var handshakeBlockPromiseEnqueuer = new RunOnce(() -> {
                            final var threadedActionListener = new ThreadedActionListener<>(ch.eventLoop(), handshakeBlockPromise);
                            handshakeBlockPromiseQueue.add(new ActionListener<>() {
                                @Override
                                public void onResponse(Void v) {
                                    threadedActionListener.onResponse(v);
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    threadedActionListener.onFailure(e);
                                }

                                @Override
                                public String toString() {
                                    return "handshake block promise for " + ch;
                                }
                            });
                        });

                        ch.pipeline().addBefore("ssl", "handshake-start-detector", new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                handshakeStartRecorder.run();
                                assertSame(workerThread, Thread.currentThread());
                                // ownership transfer of msg to handshakeBlockPromise - no refcounting needed
                                handshakeBlockPromise.addListener(
                                    ActionListener.running(() -> ctx.fireChannelRead(msg)),
                                    ctx.executor(),
                                    null
                                );
                                handshakeBlockPromiseEnqueuer.run();
                            }

                            @Override
                            public void channelReadComplete(ChannelHandlerContext ctx) {
                                handshakeBlockPromise.addListener(
                                    ActionListener.running(ctx::fireChannelReadComplete),
                                    ctx.executor(),
                                    null
                                );
                            }
                        }).addAfter("ssl", "handshake-complete-detector", new SimpleUserEventChannelHandler<SslHandshakeCompletionEvent>() {
                            @Override
                            protected void eventReceived(ChannelHandlerContext ctx, SslHandshakeCompletionEvent evt) {
                                handshakeCompletePromise.onResponse(null);
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                // TODO NOCOMMIT must cover in test the case where we don't receive a full handshake
                                // TODO NOCOMMIT must cover in test the case where the channel closes before the handshake is complete
                                handshakeCompletePromise.onResponse(null);
                                super.channelInactive(ctx);
                            }
                        });
                    }
                };
            }
        };
        server.start();
        return server;
    }

    /**
     * Set up a Netty HTTPs client and connect to server.
     * Configured with self-signed certificate trust.
     */
    private Bootstrap setupClientBootstrap(
        EventLoopGroup eventLoopGroup,
        TransportAddress serverAddress,
        final SubscribableListener<Void> handshakeCompletePromise
    ) {
        var sslContext = newClientSslContext();
        return new Bootstrap().group(eventLoopGroup)
            .channel(NioSocketChannel.class)
            .remoteAddress(serverAddress.getAddress(), serverAddress.getPort())
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    final var sslHandler = sslContext.newHandler(ch.alloc());
                    ch.pipeline().addLast(sslHandler);
                    sslHandler.handshakeFuture().addListener(future -> {
                        if (future.isSuccess()) {
                            handshakeCompletePromise.onResponse(null);
                        } else {
                            ExceptionsHelper.maybeDieOnAnotherThread(future.cause());
                            if (future.cause() instanceof ClosedChannelException closedChannelException) {
                                handshakeCompletePromise.onFailure(
                                    new NodeDisconnectedException(
                                        null,
                                        "disconnected before handshake complete",
                                        null,
                                        closedChannelException
                                    )
                                );
                            } else {
                                handshakeCompletePromise.onFailure(new ElasticsearchException("handshake failed", future.cause()));
                            }
                        }
                    });

                    ch.closeFuture()
                        .addListener(
                            ignored -> handshakeCompletePromise.onFailure(
                                new NodeDisconnectedException(null, "disconnected before handshake complete", null, null)
                            )
                        );

                    handshakeCompletePromise.addListener(ActionListener.running(ch::close));
                }
            });
    }

    private static SslContext newClientSslContext() {
        try {
            return SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /**
     * This test ensures that we permit up to the max number of concurrent handshakes without any throttling
     */
    @AwaitsFix(bugUrl = "NOCOMMIT")
    public void testNoThrottling() {
        final List<Releasable> releasables = new ArrayList<>();
        try {
            // connection-to-event-loop assignment is round-robin so this will never hit the limit
            final var eventLoopCount = between(1, 5);
            final var maxConcurrentTlsHandshakes = between(1, 5);
            final var clientCount = between(1, eventLoopCount * maxConcurrentTlsHandshakes);
            final var maxDelayedTlsHandshakes = between(0, 100);

            final var threadPool = new TestThreadPool(getTestName());
            releasables.add(() -> ThreadPool.terminate(threadPool, SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS));

            final var sharedGroupFactory = new SharedGroupFactory(
                Settings.builder().put(Netty4Plugin.WORKER_COUNT.getKey(), eventLoopCount).build()
            );
            final var queue = ConcurrentCollections.<ActionListener<Void>>newBlockingQueue();

            final var meterRegistry = new RecordingMeterRegistry();
            final var metricRecorder = meterRegistry.getRecorder();

            final var serverTransport = createServerTransport(
                threadPool,
                sharedGroupFactory,
                maxConcurrentTlsHandshakes,
                maxDelayedTlsHandshakes,
                queue,
                meterRegistry
            );
            releasables.add(serverTransport);

            final var handshakeCompletePromises = startClientsAndGetHandshakeCompletePromises(
                clientCount,
                randomFrom(serverTransport.boundAddress().boundAddresses()),
                releasables
            );

            final var handshakeBlockPromises = IntStream.range(0, clientCount).mapToObj(ignored -> {
                try {
                    return Objects.requireNonNull(
                        queue.poll(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS),
                        "timed out waiting for handshake block"
                    );
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }).toList();

            logger.info("--> all handshakes blocked");

            metricRecorder.collect();
            assertLongMetric(metricRecorder, LONG_GAUGE, CURRENT_IN_FLIGHT_METRIC, clientCount);
            assertLongMetric(metricRecorder, LONG_GAUGE, CURRENT_DELAYED_METRIC, 0);
            assertLongMetric(metricRecorder, LONG_ASYNC_COUNTER, TOTAL_DELAYED_METRIC, 0);
            assertLongMetric(metricRecorder, LONG_ASYNC_COUNTER, TOTAL_DROPPED_METRIC, 0);
            metricRecorder.resetCalls();

            handshakeBlockPromises.forEach(l -> l.onResponse(null));
            handshakeCompletePromises.forEach(ESTestCase::safeAwait);

            assertFinalStats(serverTransport, metricRecorder, 0, 0);
        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            Collections.reverse(releasables);
            Releasables.close(releasables);
        }
    }

    /**
     * This test ensures that if we send more than the permitted number of TLS handshakes at once then the excess are throttled
     */
    @AwaitsFix(bugUrl = "NOCOMMIT")
    public void testThrottleConcurrentHandshakes() {
        final List<Releasable> releasables = new ArrayList<>();
        try {
            // connection-to-event-loop assignment is round-robin so this will always use all available slots before queueing
            final var eventLoopCount = between(2, 2);
            final var maxConcurrentTlsHandshakes = between(2, 2);
            final var expectedDelayedHandshakes = between(2, 2);
            final var clientCount = eventLoopCount * maxConcurrentTlsHandshakes + expectedDelayedHandshakes;
            final var maxDelayedTlsHandshakes = clientCount + between(0, 100);

            final var threadPool = new TestThreadPool(getTestName());
            releasables.add(() -> ThreadPool.terminate(threadPool, SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS));

            final var sharedGroupFactory = new SharedGroupFactory(
                Settings.builder().put(Netty4Plugin.WORKER_COUNT.getKey(), eventLoopCount).build()
            );
            final var queue = ConcurrentCollections.<ActionListener<Void>>newBlockingQueue();

            final var serverTransport = createServerTransport(
                threadPool,
                sharedGroupFactory,
                maxConcurrentTlsHandshakes,
                maxDelayedTlsHandshakes,
                queue,
                MeterRegistry.NOOP
            );
            releasables.add(serverTransport);

            final var clientEventLoop = new NioEventLoopGroup(1);
            releasables.add(() -> ThreadPool.terminate(clientEventLoop, SAFE_AWAIT_TIMEOUT.millis(), TimeUnit.MILLISECONDS));

            final var serverAddress = randomFrom(serverTransport.boundAddress().boundAddresses());

            final var handshakeCompletePromises = IntStream.range(0, clientCount).mapToObj(i -> new SubscribableListener<Void>()).toList();
            handshakeCompletePromises.forEach(handshakeCompletePromise -> {
                final var clientBootstrap = setupClientBootstrap(clientEventLoop, serverAddress, handshakeCompletePromise);
                final var connectFuture = clientBootstrap.connect().syncUninterruptibly();
                final var channel = connectFuture.channel();
                releasables.add(() -> channel.close().syncUninterruptibly());
            });

            final Supplier<ActionListener<Void>> handshakeBlockPromiseSupplier = () -> {
                try {
                    final var handshakeBlockPromise = queue.poll(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
                    logger.info("got handshakeBlockPromise [{}]", handshakeBlockPromise);
                    return Objects.requireNonNull(handshakeBlockPromise, "timed out waiting for handshake block");
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            };

            final var handshakeBlockPromises = new ArrayDeque<ActionListener<Void>>();
            for (int i = 0; i < eventLoopCount * maxConcurrentTlsHandshakes; i++) {
                handshakeBlockPromises.addLast(handshakeBlockPromiseSupplier.get());
            }
            assertNull(queue.poll()); // this is key, all the handshakes beyond the limit are delayed
            logger.info("--> max number of handshakes received & blocked");

            // BUG cannot be sure that this loop will release all the event loops in turn
            for (int i = 0; i < expectedDelayedHandshakes; i++) {
                // complete one handshake and ensure that this triggers exactly one more handshake
                final var handshakeBlockPromise = handshakeBlockPromises.removeFirst();
                logger.info("complete handshakeBlockPromise to release next [{}]", handshakeBlockPromise);
                handshakeBlockPromise.onResponse(null);
                handshakeBlockPromises.addLast(handshakeBlockPromiseSupplier.get());
                assertNull(queue.poll());
            }

            logger.info("--> all handshakes received");

            // complete the remaining handshakes and ensure that this triggers no more
            while (handshakeBlockPromises.isEmpty() == false) {
                final var handshakeBlockPromise = handshakeBlockPromises.removeFirst();
                logger.info("complete handshakeBlockPromise to drain remainder [{}]", handshakeBlockPromise);
                handshakeBlockPromise.onResponse(null);
                assertNull(queue.poll());
            }

            handshakeCompletePromises.forEach(ESTestCase::safeAwait);
        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            Collections.reverse(releasables);
            Releasables.close(releasables);
        }
    }

    @AwaitsFix(bugUrl = "NOCOMMIT")
    public void testDiscardExcessiveConcurrentHandshakes() {
        final List<Releasable> releasables = new ArrayList<>();
        try {
            // connection-to-event-loop assignment is round-robin so this will always use all available slots before rejecting
            final var eventLoopCount = between(1, 5);
            final var maxConcurrentTlsHandshakes = between(1, 5);
            final var maxDelayedTlsHandshakes = between(1, 5);
            final var excessiveHandshakes = between(eventLoopCount, 5); // at least eventLoopCount to ensure max delayed everywhere
            final var clientCount = eventLoopCount * (maxConcurrentTlsHandshakes + maxDelayedTlsHandshakes) + excessiveHandshakes;

            logger.info(
                "--> eventLoopCount={} maxConcurrentTlsHandshakes={} maxDelayedTlsHandshakes={} excessiveHandshakes={} clientCount={}",
                eventLoopCount,
                maxConcurrentTlsHandshakes,
                maxDelayedTlsHandshakes,
                excessiveHandshakes,
                clientCount
            );

            final var threadPool = new TestThreadPool(getTestName());
            releasables.add(() -> ThreadPool.terminate(threadPool, SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS));

            final var sharedGroupFactory = new SharedGroupFactory(
                Settings.builder().put(Netty4Plugin.WORKER_COUNT.getKey(), eventLoopCount).build()
            );
            final var queue = ConcurrentCollections.<ActionListener<Void>>newBlockingQueue();

            final var meterRegistry = new RecordingMeterRegistry();
            final var metricRecorder = meterRegistry.getRecorder();

            final var serverTransport = createServerTransport(
                threadPool,
                sharedGroupFactory,
                maxConcurrentTlsHandshakes,
                maxDelayedTlsHandshakes,
                queue,
                meterRegistry
            );
            releasables.add(serverTransport);

            final var handshakeCompletePromises = startClientsAndGetHandshakeCompletePromises(
                clientCount,
                randomFrom(serverTransport.boundAddress().boundAddresses()),
                releasables
            );

            final var failedHandshakesLatch = new CountDownLatch(excessiveHandshakes);
            final var exceptionCount = new AtomicInteger();
            handshakeCompletePromises.forEach(handshakeCompletePromise -> handshakeCompletePromise.addListener(new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {}

                @Override
                public void onFailure(Exception e) {
                    assertThat(exceptionCount.incrementAndGet(), lessThanOrEqualTo(excessiveHandshakes));
                    assertThat(e, Matchers.instanceOf(NodeDisconnectedException.class));
                    assertThat(e.getMessage(), equalTo("disconnected before handshake complete"));
                    failedHandshakesLatch.countDown();
                }
            }));

            safeAwait(failedHandshakesLatch);
            logger.info("--> excessive handshakes cancelled");

            metricRecorder.collect();
            assertLongMetric(metricRecorder, LONG_GAUGE, CURRENT_IN_FLIGHT_METRIC, eventLoopCount * maxConcurrentTlsHandshakes);
            assertLongMetric(metricRecorder, LONG_GAUGE, CURRENT_DELAYED_METRIC, eventLoopCount * maxDelayedTlsHandshakes);
            assertLongMetric(
                metricRecorder,
                LONG_ASYNC_COUNTER,
                TOTAL_DELAYED_METRIC,
                eventLoopCount * maxDelayedTlsHandshakes + excessiveHandshakes
            );
            assertLongMetric(metricRecorder, LONG_ASYNC_COUNTER, TOTAL_DROPPED_METRIC, excessiveHandshakes);
            metricRecorder.resetCalls();

            for (int i = 0; i < eventLoopCount * (maxConcurrentTlsHandshakes + maxDelayedTlsHandshakes); i++) {
                Objects.requireNonNull(queue.poll(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS), "timed out waiting for handshake block")
                    .onResponse(null);
            }
            logger.info("--> all handshakes released");

            final var completeLatch = new CountDownLatch(handshakeCompletePromises.size());
            handshakeCompletePromises.forEach(
                handshakeCompletePromise -> handshakeCompletePromise.addListener(ActionListener.running(completeLatch::countDown))
            );
            safeAwait(completeLatch);
            logger.info("--> all handshakes completed");

            assertFinalStats(
                serverTransport,
                metricRecorder,
                eventLoopCount * maxDelayedTlsHandshakes + excessiveHandshakes,
                excessiveHandshakes
            );

        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            Collections.reverse(releasables);
            Releasables.close(releasables);
        }
    }

    private List<SubscribableListener<Void>> startClientsAndGetHandshakeCompletePromises(
        int clientCount,
        TransportAddress serverAddress,
        List<Releasable> releasables
    ) {
        final var clientEventLoop = new NioEventLoopGroup(1);
        releasables.add(() -> ThreadPool.terminate(clientEventLoop, SAFE_AWAIT_TIMEOUT.millis(), TimeUnit.MILLISECONDS));

        final var handshakeCompletePromises = IntStream.range(0, clientCount).mapToObj(i -> new SubscribableListener<Void>()).toList();
        handshakeCompletePromises.forEach(handshakeCompletePromise -> {
            final var clientBootstrap = setupClientBootstrap(clientEventLoop, serverAddress, handshakeCompletePromise);
            final var connectFuture = clientBootstrap.connect();
            releasables.add(() -> connectFuture.syncUninterruptibly().channel().close().syncUninterruptibly());
        });
        return handshakeCompletePromises;
    }

    private static void assertFinalStats(
        Netty4HttpServerTransport serverTransport,
        MetricRecorder<Instrument> metricRecorder,
        int expectedTotalDelayed,
        int expectedTotalDropped
    ) {
        // clients may get handshake completion before server, so we have to busy-wait here before checking final metrics:
        assertTrue(waitUntil(() -> serverTransport.stats().serverOpen() == 0));
        metricRecorder.collect();
        assertLongMetric(metricRecorder, LONG_GAUGE, CURRENT_IN_FLIGHT_METRIC, 0);
        assertLongMetric(metricRecorder, LONG_GAUGE, CURRENT_DELAYED_METRIC, 0);
        assertLongMetric(metricRecorder, LONG_ASYNC_COUNTER, TOTAL_DELAYED_METRIC, expectedTotalDelayed);
        assertLongMetric(metricRecorder, LONG_ASYNC_COUNTER, TOTAL_DROPPED_METRIC, expectedTotalDropped);
        metricRecorder.resetCalls();
    }

    private static void assertLongMetric(
        MetricRecorder<Instrument> metricRecorder,
        InstrumentType instrumentType,
        String name,
        int expectedValue
    ) {
        assertEquals(
            name,
            List.of((long) expectedValue),
            metricRecorder.getMeasurements(instrumentType, name).stream().map(Measurement::getLong).toList()
        );
    }

    public void testEverything() {
        // interleave tests
        testNoThrottling();
        testThrottleConcurrentHandshakes();
        testDiscardExcessiveConcurrentHandshakes();
        System.gc(); // try and trigger leak detection
    }

    private static final HttpServerTransport.Dispatcher NEVER_CALLED_DISPATCHER = new HttpServerTransport.Dispatcher() {
        @Override
        public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
            fail("dispatchRequest should never be called");
        }

        @Override
        public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {
            fail("dispatchBadRequest should never be called");
        }
    };

    private static final String CURRENT_IN_FLIGHT_METRIC = "es.http.tls_handshakes.current";
    private static final String CURRENT_DELAYED_METRIC = "es.http.tls_handshakes.current_delayed";
    private static final String TOTAL_DELAYED_METRIC = "es.http.tls_handshakes.total_delayed";
    private static final String TOTAL_DROPPED_METRIC = "es.http.tls_handshakes.dropped";
}
