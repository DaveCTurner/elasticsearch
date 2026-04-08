/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.repositories.blobstore.ESMockAPIBasedRepositoryIntegTestCase;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Queue;
import java.util.function.Predicate;

@SuppressForbidden(reason = "We use HttpServer for the fixtures")
public class ResponseInjectingHttpHandler implements ESMockAPIBasedRepositoryIntegTestCase.DelegatingHttpHandler {

    private static final Logger logger = LogManager.getLogger(ResponseInjectingHttpHandler.class);

    private final HttpHandler delegate;
    private final Queue<RequestHandler> requestHandlerQueue;

    public ResponseInjectingHttpHandler(Queue<RequestHandler> requestHandlerQueue, HttpHandler delegate) {
        this.delegate = delegate;
        this.requestHandlerQueue = requestHandlerQueue;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        RequestHandler nextHandler = requestHandlerQueue.peek();
        if (nextHandler != null && nextHandler.matchesRequest(exchange)) {
            requestHandlerQueue.poll().writeResponse(exchange, delegate);
        } else {
            delegate.handle(exchange);
        }
    }

    @Override
    public HttpHandler getDelegate() {
        return delegate;
    }

    @SuppressForbidden(reason = "We use HttpServer for the fixtures")
    @FunctionalInterface
    public interface RequestHandler {
        void writeResponse(HttpExchange exchange, HttpHandler delegate) throws IOException;

        default boolean matchesRequest(HttpExchange exchange) {
            return true;
        }
    }

    @SuppressForbidden(reason = "We use HttpServer for the fixtures")
    public static class FixedRequestHandler implements RequestHandler {

        private final RestStatus status;
        private final String responseBody;
        private final Predicate<HttpExchange> requestMatcher;

        public FixedRequestHandler(RestStatus status) {
            this(status, null, req -> true);
        }

        /**
         * Create a handler that only gets executed for requests that match the supplied predicate. Note
         * that because the errors are stored in a queue this will prevent any subsequently queued errors from
         * being returned until after it returns.
         */
        public FixedRequestHandler(RestStatus status, String responseBody, Predicate<HttpExchange> requestMatcher) {
            this.status = status;
            this.responseBody = responseBody;
            this.requestMatcher = requestMatcher;
        }

        @Override
        public boolean matchesRequest(HttpExchange exchange) {
            return requestMatcher.test(exchange);
        }

        @Override
        public void writeResponse(HttpExchange exchange, HttpHandler delegateHandler) throws IOException {
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "FixedRequestHandler request: {} {} from [{}] (x-ms-client-request-id=[{}]), injecting status=[{}], "
                        + "hasResponseBody=[{}], requestMatcher=[{}]",
                    exchange.getRequestMethod(),
                    exchange.getRequestURI(),
                    exchange.getRemoteAddress(),
                    exchange.getRequestHeaders().getFirst("x-ms-client-request-id"),
                    status,
                    responseBody != null,
                    requestMatcher
                );
            }
            if (responseBody != null) {
                byte[] responseBytes = responseBody.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(status.getStatus(), responseBytes.length == 0 ? -1 : responseBytes.length);
                if (logger.isTraceEnabled()) {
                    logger.trace(
                        "FixedRequestHandler response: status=[{}], responseLength=[{}], x-ms-client-request-id=[{}], "
                            + "responseHeaders=[{}]",
                        status.getStatus(),
                        responseBytes.length == 0 ? -1 : (long) responseBytes.length,
                        exchange.getRequestHeaders().getFirst("x-ms-client-request-id"),
                        exchange.getResponseHeaders()
                    );
                }
                exchange.getResponseBody().write(responseBytes);
            } else {
                exchange.sendResponseHeaders(status.getStatus(), -1);
                if (logger.isTraceEnabled()) {
                    logger.trace(
                        "FixedRequestHandler response: status=[{}], responseLength=[{}], x-ms-client-request-id=[{}], "
                            + "responseHeaders=[{}]",
                        status.getStatus(),
                        -1L,
                        exchange.getRequestHeaders().getFirst("x-ms-client-request-id"),
                        exchange.getResponseHeaders()
                    );
                }
            }
        }
    }
}
