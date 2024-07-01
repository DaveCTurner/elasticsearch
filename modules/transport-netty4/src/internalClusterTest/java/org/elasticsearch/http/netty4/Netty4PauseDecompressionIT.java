/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

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

import org.elasticsearch.ESNetty4IntegTestCase;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.transport.netty4.NettyAllocator;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.zip.GZIPOutputStream;

public class Netty4PauseDecompressionIT extends ESNetty4IntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    public void testPauseDecompression() throws Exception {

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

            final var remoteAddress = randomFrom(internalCluster().getInstance(HttpServerTransport.class).boundAddress().boundAddresses())
                .address();

            final var channelFuture = clientBootstrap.connect(remoteAddress);
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
                var bufferedStream = new BufferedOutputStream(requestOutputStream, ByteSizeUnit.KB.toIntBytes(16));
                var gzipStream = new GZIPOutputStream(bufferedStream, ByteSizeUnit.KB.toIntBytes(16))
            ) {
                final var rawBytes = randomByteArrayOfLength(ByteSizeUnit.KB.toIntBytes(16));
                int remaining = ByteSizeUnit.MB.toIntBytes(100);
                while (remaining > 0) {
                    final var toSend = Math.min(rawBytes.length, remaining);
                    gzipStream.write(rawBytes, 0, toSend);
                    remaining -= toSend;
                }
            }
            channel.writeAndFlush(new DefaultLastHttpContent(new EmptyByteBuf(NettyAllocator.getAllocator()))).syncUninterruptibly();
            safeAwait(responseReceivedLatch);
        } finally {
            Collections.reverse(resources);
            Releasables.close(resources);
        }
    }
}
