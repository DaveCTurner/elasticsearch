/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleUserEventChannelHandler;
import io.netty.handler.ssl.SslClientHelloHandler;
import io.netty.handler.ssl.SslCompletionEvent;
import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.Future;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.node.NodeClosedException;

import java.util.ArrayDeque;

/**
 * A throttle on TLS handshakes for incoming HTTP connections for a single event loop thread.
 */
class TlsHandshakeThrottle {

    private static final Logger logger = LogManager.getLogger(TlsHandshakeThrottle.class);

    private final Netty4HttpServerTransport transport;
    private final ArrayDeque<AbstractRunnable> delayedHandshakes = new ArrayDeque<>();

    private int inflightHandshakes = 0;

    TlsHandshakeThrottle(Netty4HttpServerTransport transport) {
        this.transport = transport;
    }

    void close() {
        while (delayedHandshakes.isEmpty() == false) {
            delayedHandshakes.pollFirst().onFailure(new NodeClosedException((DiscoveryNode) null));
        }
    }

    ChannelHandler newHandshakeThrottleHandler(SubscribableListener<Void> handshakeCompletePromise) {
        return new HandshakeThrottleHandler(handshakeCompletePromise);
    }

    ChannelHandler newHandshakeCompletionWatcher(SubscribableListener<Void> handshakeCompletePromise) {
        return new HandshakeCompletionWatcher(handshakeCompletePromise);
    }

    void handleHandshakeCompletion() {
        if (delayedHandshakes.isEmpty()) {
            inflightHandshakes -= 1;
        } else {
            delayedHandshakes.removeFirst().run();
        }
    }

    private class HandshakeThrottleHandler extends SslClientHelloHandler<Void> {

        /**
         * Promise which accumulates the messages received until we receive a full handshake. Completed when we receive a full handshake,
         * at which point all the delayed messages are pushed down the pipeline for actual processing.
         */
        private final SubscribableListener<Void> handshakeStartedPromise;

        /**
         * Promise which will be completed by the channel's matching {@link HandshakeCompletionWatcher} when the handshake we sent down the
         * pipeline has completed.
         */
        private final SubscribableListener<Void> handshakeCompletePromise;

        HandshakeThrottleHandler(SubscribableListener<Void> handshakeCompletePromise) {
            this.handshakeCompletePromise = handshakeCompletePromise;
            this.handshakeStartedPromise = new SubscribableListener<>();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof ReferenceCounted referenceCounted) {
                referenceCounted.retain();
            }
            handshakeStartedPromise.addListener(new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    ctx.fireChannelRead(msg);
                }

                @Override
                public void onFailure(Exception e) {
                    if (msg instanceof ReferenceCounted referenceCounted) {
                        referenceCounted.release();
                    }
                }
            });
            super.channelRead(ctx, msg);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            handshakeStartedPromise.addListener(new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    ctx.fireChannelReadComplete();
                }

                @Override
                public void onFailure(Exception e) {}
            });
            super.channelReadComplete(ctx);
        }

        @Override
        protected Future<Void> lookup(ChannelHandlerContext ctx, ByteBuf clientHello) {
            if (clientHello == null) {
                ctx.channel().close();
                final var exception = new IllegalArgumentException(
                    "did not receive initial ClientHello on channel [" + ctx.channel() + "]"
                );
                handshakeStartedPromise.onFailure(exception);
                throw exception;
            }

            if (ctx.channel().isActive() == false) {
                logger.debug("lookup after channel inactive, ignoring [{}]", ctx.channel());
                return ctx.executor().newSucceededFuture(null);
            }

            if (inflightHandshakes < transport.maxConcurrentTlsHandshakes) {
                inflightHandshakes += 1;
                handshakeCompletePromise.addListener(ActionListener.running(TlsHandshakeThrottle.this::handleHandshakeCompletion));
                ctx.channel().pipeline().remove(HandshakeThrottleHandler.this);
                handshakeStartedPromise.onResponse(null);
            } else {
                logger.debug(
                    "[{}] in flight TLS handshakes already, enqueueing new handshake on [{}]",
                    inflightHandshakes,
                    ctx.channel(),
                    new ElasticsearchException("stack trace")
                );
                delayedHandshakes.addFirst(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        logger.debug(
                            "[{}] in flight and [{}] enqueued TLS handshakes, cancelling handshake on [{}]: {}",
                            inflightHandshakes,
                            delayedHandshakes.size(),
                            ctx.channel(),
                            e.getMessage(),
                            new ElasticsearchException("stack trace", e)
                        );
                        ctx.channel().close();
                    }

                    @Override
                    protected void doRun() {
                        logger.debug(
                            "[{}] in flight and [{}] enqueued TLS handshakes, processing delayed handshake on [{}]",
                            inflightHandshakes,
                            delayedHandshakes.size(),
                            ctx.channel()
                        );
                        handshakeCompletePromise.addListener(ActionListener.running(TlsHandshakeThrottle.this::handleHandshakeCompletion));
                        ctx.pipeline().remove(HandshakeThrottleHandler.this);
                        handshakeStartedPromise.onResponse(null);
                    }

                    @Override
                    public String toString() {
                        return "delayed handshake on [" + ctx.channel() + "]";
                    }
                });

                while (delayedHandshakes.size() > transport.maxDelayedTlsHandshakes) {
                    final var lastDelayedHandshake = delayedHandshakes.removeLast();
                    logger.info("--> cancelling [{}]", lastDelayedHandshake);
                    lastDelayedHandshake.onFailure(new ElasticsearchException("too many in-flight TLS handshakes"));
                }
            }

            return ctx.executor().newSucceededFuture(null);
        }

        @Override
        protected void onLookupComplete(ChannelHandlerContext ctx, Future<Void> future) {}
    }

    private static class HandshakeCompletionWatcher extends SimpleUserEventChannelHandler<SslCompletionEvent> {
        private final SubscribableListener<Void> handshakeCompletePromise;

        HandshakeCompletionWatcher(SubscribableListener<Void> handshakeCompletePromise) {
            this.handshakeCompletePromise = handshakeCompletePromise;
        }

        @Override
        protected void eventReceived(ChannelHandlerContext ctx, SslCompletionEvent evt) {
            ctx.pipeline().remove(HandshakeCompletionWatcher.this);
            if (evt.isSuccess()) {
                handshakeCompletePromise.onResponse(null);
            } else {
                ExceptionsHelper.maybeDieOnAnotherThread(evt.cause());
                handshakeCompletePromise.onFailure(
                    evt.cause() instanceof Exception exception ? exception : new ElasticsearchException("TLS handshake failed", evt.cause())
                );
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            if (handshakeCompletePromise.isDone() == false) {
                handshakeCompletePromise.onFailure(new ElasticsearchException("channel closed before TLS handshake completed"));
            }
            super.channelInactive(ctx);
        }
    }
}
