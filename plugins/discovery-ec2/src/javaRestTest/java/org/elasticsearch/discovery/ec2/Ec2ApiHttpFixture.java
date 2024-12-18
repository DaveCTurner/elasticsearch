/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.discovery.ec2;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.junit.rules.ExternalResource;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.function.Supplier;

public class Ec2ApiHttpFixture extends ExternalResource {
    private final Supplier<String> configPathSupplier;
    private HttpServer server;

    public Ec2ApiHttpFixture(Supplier<String> configPathSupplier) {
        this.configPathSupplier = configPathSupplier;
    }

    public String getAddress() {
        return "http://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort();
    }

    @Override
    protected void before() throws Throwable {
        server = HttpServer.create(resolveAddress(), 0);
        server.createContext("/", this::handleRequest);
        server.start();
    }

    private static InetSocketAddress resolveAddress() {
        try {
            return new InetSocketAddress(InetAddress.getByName("localhost"), 0);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void after() {
        server.stop(0);
    }

    private void handleRequest(HttpExchange exchange) {
        try (exchange) {
            configPathSupplier.get();
            throw new UnsupportedOperationException("boom");
        }
    }
}
