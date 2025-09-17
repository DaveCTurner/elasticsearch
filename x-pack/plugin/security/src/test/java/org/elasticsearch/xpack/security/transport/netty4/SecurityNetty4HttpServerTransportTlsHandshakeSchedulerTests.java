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
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.ReferenceCounted;

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
import org.elasticsearch.telemetry.TelemetryProvider;
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
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

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
        String tlsProtocols,
        ThreadPool threadPool,
        SharedGroupFactory sharedGroupFactory,
        int maxConcurrentTlsHandshakes,
        int maxDelayedTlsHandshakes,
        Queue<ActionListener<Void>> handshakeBlockPromiseQueue
    ) {
        final Settings.Builder builder = Settings.builder();
        addSSLSettingsForNodePEMFiles(builder, "xpack.security.http.", randomBoolean());
        final var settings = builder.put("xpack.security.http.ssl.enabled", true)
            .put("path.home", createTempDir())
            .put("xpack.security.http.ssl.supported_protocols", tlsProtocols)
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

        final var server = new Netty4HttpServerTransport(
            settings,
            new NetworkService(Collections.emptyList()),
            threadPool,
            xContentRegistry(),
            NEVER_CALLED_DISPATCHER,
            new ClusterSettings(settings, Set.copyOf(settingsSet)),
            sharedGroupFactory,
            TelemetryProvider.NOOP,
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

                        final var handshakeBlockPromiseEnqueuer = new RunOnce(
                            () -> handshakeBlockPromiseQueue.add(new ThreadedActionListener<>(ch.eventLoop(), handshakeBlockPromise))
                        );

                        ch.pipeline().addBefore("ssl", "handshake-start-detector", new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                handshakeStartRecorder.run();

                                assertSame(workerThread, Thread.currentThread());
                                final var referenceCounted = asInstanceOf(ReferenceCounted.class, msg);
                                referenceCounted.retain();
                                handshakeBlockPromise.addListener(
                                    ActionListener.running(() -> ctx.fireChannelRead(referenceCounted)),
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
                            handshakeCompletePromise.onFailure(new ElasticsearchException("handshake failed", future.cause()));
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

    private void runForAllTlsVersions(Consumer<String> test) {
        List.of("TLSv1.2", "TLSv1.3").forEach(test);
    }

    /**
     * This test ensures that we permit up to the max number of concurrent handshakes without any throttling
     */
    public void testNoThrottling() {
        runForAllTlsVersions(tlsVersion -> {
            final List<Releasable> releasables = new ArrayList<>();
            try {
                final var eventLoopCount = 1;
                final var maxConcurrentTlsHandshakes = 1; // between(1, 5);
                final var clientCount = maxConcurrentTlsHandshakes;
                final var maxDelayedTlsHandshakes = clientCount - maxConcurrentTlsHandshakes;

                final var threadPool = new TestThreadPool(getTestName());
                releasables.add(() -> ThreadPool.terminate(threadPool, SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS));

                final var sharedGroupFactory = new SharedGroupFactory(
                    Settings.builder().put(Netty4Plugin.WORKER_COUNT.getKey(), eventLoopCount).build()
                );
                final var queue = ConcurrentCollections.<ActionListener<Void>>newBlockingQueue();

                final var serverTransport = createServerTransport(
                    tlsVersion,
                    threadPool,
                    sharedGroupFactory,
                    maxConcurrentTlsHandshakes,
                    maxDelayedTlsHandshakes,
                    queue
                );
                releasables.add(serverTransport);

                final var eventLoopGroup = sharedGroupFactory.getHttpGroup();
                releasables.add(eventLoopGroup::shutdown);

                final var serverAddress = randomFrom(serverTransport.boundAddress().boundAddresses());

                final var handshakeCompletePromises = IntStream.range(0, clientCount)
                    .mapToObj(i -> new SubscribableListener<Void>())
                    .toList();
                handshakeCompletePromises.forEach(handshakeCompletePromise -> {
                    final var clientBootstrap = setupClientBootstrap(
                        eventLoopGroup.getLowLevelGroup(),
                        serverAddress,
                        handshakeCompletePromise
                    );
                    final var connectFuture = clientBootstrap.connect().syncUninterruptibly();
                    final var channel = connectFuture.channel();
                    releasables.add(() -> channel.close().syncUninterruptibly());
                });

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

                logger.info("--> all handshakes blocked, releasing now");
                handshakeBlockPromises.forEach(l -> l.onResponse(null));
                handshakeCompletePromises.forEach(ESTestCase::safeAwait);
            } catch (Exception e) {
                throw new AssertionError(e);
            } finally {
                Collections.reverse(releasables);
                Releasables.close(releasables);
            }
        });
    }

    /**
     * This test ensures that if we send more than the permitted number of TLS handshakes at once then the excess are throttled
     */
    public void testThrottleConcurrentHandshakes() {
        runForAllTlsVersions(tlsVersion -> {
            final List<Releasable> releasables = new ArrayList<>();
            try {
                final var eventLoopCount = 1;
                final var maxConcurrentTlsHandshakes = between(1, 5);
                final var clientCount = eventLoopCount * maxConcurrentTlsHandshakes + between(1, 5);
                final var maxDelayedTlsHandshakes = clientCount - maxConcurrentTlsHandshakes;

                final var threadPool = new TestThreadPool(getTestName());
                releasables.add(() -> ThreadPool.terminate(threadPool, SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS));

                final var sharedGroupFactory = new SharedGroupFactory(
                    Settings.builder().put(Netty4Plugin.WORKER_COUNT.getKey(), eventLoopCount).build()
                );
                final var queue = ConcurrentCollections.<ActionListener<Void>>newBlockingQueue();

                final var serverTransport = createServerTransport(
                    tlsVersion,
                    threadPool,
                    sharedGroupFactory,
                    maxConcurrentTlsHandshakes,
                    maxDelayedTlsHandshakes,
                    queue
                );
                releasables.add(serverTransport);

                final var eventLoopGroup = sharedGroupFactory.getHttpGroup();
                releasables.add(eventLoopGroup::shutdown);

                final var serverAddress = randomFrom(serverTransport.boundAddress().boundAddresses());

                final var handshakeCompletePromises = IntStream.range(0, clientCount)
                    .mapToObj(i -> new SubscribableListener<Void>())
                    .toList();
                handshakeCompletePromises.forEach(handshakeCompletePromise -> {
                    final var clientBootstrap = setupClientBootstrap(
                        eventLoopGroup.getLowLevelGroup(),
                        serverAddress,
                        handshakeCompletePromise
                    );
                    final var connectFuture = clientBootstrap.connect().syncUninterruptibly();
                    final var channel = connectFuture.channel();
                    releasables.add(() -> channel.close().syncUninterruptibly());
                });

                final Supplier<ActionListener<Void>> handshakeBlockPromiseSupplier = () -> {
                    try {
                        return Objects.requireNonNull(
                            queue.poll(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS),
                            "timed out waiting for handshake block"
                        );
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                };

                final var handshakeBlockPromises = new ArrayDeque<ActionListener<Void>>();
                for (int i = 0; i < maxConcurrentTlsHandshakes; i++) {
                    handshakeBlockPromises.addLast(handshakeBlockPromiseSupplier.get());
                }
                assertNull(queue.poll());
                logger.info("--> max number of handshakes received & blocked");

                for (int i = 0; i < clientCount - maxConcurrentTlsHandshakes; i++) {
                    handshakeBlockPromises.removeFirst().onResponse(null);
                    handshakeBlockPromises.addLast(handshakeBlockPromiseSupplier.get());
                    assertNull(queue.poll());
                }

                logger.info("--> all handshakes received");

                while (handshakeBlockPromises.isEmpty() == false) {
                    handshakeBlockPromises.removeFirst().onResponse(null);
                    assertNull(queue.poll());
                }

                handshakeCompletePromises.forEach(ESTestCase::safeAwait);
            } catch (Exception e) {
                throw new AssertionError(e);
            } finally {
                Collections.reverse(releasables);
                Releasables.close(releasables);
            }
        });
    }

    public void testDiscardExcessiveConcurrentHandshakes() {
        runForAllTlsVersions(tlsVersion -> {
            final List<Releasable> releasables = new ArrayList<>();
            try {
                final var eventLoopCount = 1;
                final var maxConcurrentTlsHandshakes = between(1, 5);
                final var maxDelayedTlsHandshakes = between(1, 5);
                final var excessiveHandshakes = between(1, 5);
                final var clientCount = maxConcurrentTlsHandshakes + maxDelayedTlsHandshakes + excessiveHandshakes;

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

                final var serverTransport = createServerTransport(
                    tlsVersion,
                    threadPool,
                    sharedGroupFactory,
                    maxConcurrentTlsHandshakes,
                    maxDelayedTlsHandshakes,
                    queue
                );
                releasables.add(serverTransport);

                final var eventLoopGroup = sharedGroupFactory.getHttpGroup();
                releasables.add(eventLoopGroup::shutdown);

                final var serverAddress = randomFrom(serverTransport.boundAddress().boundAddresses());

                final var handshakeCompletePromises = IntStream.range(0, clientCount)
                    .mapToObj(i -> new SubscribableListener<Void>())
                    .toList();
                handshakeCompletePromises.forEach(handshakeCompletePromise -> {
                    final var clientBootstrap = setupClientBootstrap(
                        eventLoopGroup.getLowLevelGroup(),
                        serverAddress,
                        handshakeCompletePromise
                    );
                    final var connectFuture = clientBootstrap.connect().syncUninterruptibly();
                    final var channel = connectFuture.channel();
                    releasables.add(() -> channel.close().syncUninterruptibly());
                    safeSleep(100);
                });

                final var failedHandshakesLatch = new CountDownLatch(excessiveHandshakes);
                handshakeCompletePromises.forEach(handshakeCompletePromise -> {
                    handshakeCompletePromise.addListener(new ActionListener<>() {
                        @Override
                        public void onResponse(Void unused) {}

                        @Override
                        public void onFailure(Exception e) {
                            assertThat(e, Matchers.instanceOf(NodeDisconnectedException.class));
                            assertThat(e.getMessage(), equalTo("disconnected before handshake complete"));
                            failedHandshakesLatch.countDown();
                        }
                    });
                });

                final Supplier<ActionListener<Void>> handshakeBlockPromiseSupplier = () -> {
                    try {
                        return Objects.requireNonNull(
                            queue.poll(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS),
                            "timed out waiting for handshake block"
                        );
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                };

                safeAwait(failedHandshakesLatch);
                logger.info("--> excessive handshakes cancelled");

                final var handshakeBlockPromises = new ArrayDeque<ActionListener<Void>>();
                for (int i = 0; i < maxConcurrentTlsHandshakes; i++) {
                    handshakeBlockPromises.addLast(handshakeBlockPromiseSupplier.get());
                }
                assertNull(queue.poll());
                logger.info("--> max number of handshakes received & blocked");

                for (int i = 0; i < maxDelayedTlsHandshakes; i++) {
                    handshakeBlockPromises.removeFirst().onResponse(null);
                    handshakeBlockPromises.addLast(handshakeBlockPromiseSupplier.get());
                    assertNull(queue.poll());
                }

                logger.info("--> all handshakes received");

                while (handshakeBlockPromises.isEmpty() == false) {
                    handshakeBlockPromises.removeFirst().onResponse(null);
                    assertNull(queue.poll());
                }

                final var completeLatch = new CountDownLatch(handshakeCompletePromises.size());
                final var exceptionCount = new AtomicInteger();
                handshakeCompletePromises.forEach(handshakeCompletePromise -> {
                    handshakeCompletePromise.addListener(new ActionListener<>() {
                        @Override
                        public void onResponse(Void unused) {
                            completeLatch.countDown();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            assertThat(exceptionCount.incrementAndGet(), lessThanOrEqualTo(excessiveHandshakes));
                            assertThat(e, Matchers.instanceOf(NodeDisconnectedException.class));
                            assertThat(e.getMessage(), equalTo("disconnected before handshake complete"));
                            completeLatch.countDown();
                        }
                    });
                });

                safeAwait(completeLatch);
            } catch (Exception e) {
                throw new AssertionError(e);
            } finally {
                Collections.reverse(releasables);
                Releasables.close(releasables);
            }
        });
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
}
