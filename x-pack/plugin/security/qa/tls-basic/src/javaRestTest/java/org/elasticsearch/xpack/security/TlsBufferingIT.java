/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.EmptyByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpVersion;

import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.transport.netty4.NettyAllocator;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.zip.GZIPOutputStream;

public class TlsBufferingIT extends ESRestTestCase {
    public void testBuffering() throws Exception {

        final var resources = new ArrayList<Releasable>();

        try {
            final var eventLoopGroup = new NioEventLoopGroup(1);
            resources.add(() -> eventLoopGroup.shutdownGracefully().syncUninterruptibly());

            final var responseReceivedLatch = new CountDownLatch(1);
            final var clientBootstrap = new Bootstrap().channel(NettyAllocator.getChannelType())
                .option(ChannelOption.ALLOCATOR, NettyAllocator.getAllocator())
                .group(eventLoopGroup)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new HttpClientCodec());
                        ch.pipeline().addLast(new HttpContentDecompressor());
                        ch.pipeline().addLast(new HttpObjectAggregator(ByteSizeUnit.MB.toIntBytes(100)));
                        ch.pipeline().addLast(new SimpleChannelInboundHandler<HttpObject>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
                                logger.info("--> received response [{}]", msg);
                                responseReceivedLatch.countDown();
                            }
                        });

                    }
                });

            final var remoteHost = randomFrom(getClusterHosts());
            final var channelFuture = clientBootstrap.connect(new InetSocketAddress(remoteHost.getAddress(), remoteHost.getPort()));
            channelFuture.sync();
            final var channel = channelFuture.channel();
            resources.add(() -> channel.close().syncUninterruptibly());

            final var request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/_bulk");
            request.headers().add("transfer-encoding", "chunked");
            request.headers().add("content-encoding", "gzip");
            request.headers().add("content-type", "application/json");
            channel.writeAndFlush(request).sync();
            logger.info("--> sent request start");

            try (var requestOutputStream = new OutputStream() {
                @Override
                public void write(int b) {
                    write(new byte[] { (byte) b }, 0, 1);
                }

                @Override
                public void write(byte[] b, int off, int len) {
                    final var byteBuf = NettyAllocator.getAllocator().heapBuffer(len);
                    byteBuf.writeBytes(b, off, len);
                    channel.writeAndFlush(new DefaultHttpContent(byteBuf)).syncUninterruptibly();
                    logger.info("--> sent buffer of size [{}]", len);
                }
            };
                var bufferedStream = new BufferedOutputStream(requestOutputStream, ByteSizeUnit.KB.toIntBytes(32));
                var gzipStream = new GZIPOutputStream(bufferedStream, ByteSizeUnit.KB.toIntBytes(32))
            ) {
                final var rawBytes = randomByteArrayOfLength(ByteSizeUnit.KB.toIntBytes(32));
                Arrays.fill(rawBytes, (byte) 0);
                int remaining = ByteSizeUnit.MB.toIntBytes(100);
                while (remaining > 0) {
                    final var toSend = Math.min(rawBytes.length, remaining);
                    gzipStream.write(rawBytes, 0, toSend);
                    remaining -= toSend;
                }
            }
            channel.writeAndFlush(new DefaultLastHttpContent(new EmptyByteBuf(NettyAllocator.getAllocator()))).syncUninterruptibly();
            logger.info("--> finished sending request");
            safeAwait(responseReceivedLatch);
        } finally {
            Collections.reverse(resources);
            Releasables.close(resources);
        }
    }
}
