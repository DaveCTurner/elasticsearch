/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import org.elasticsearch.rest.ChunkedRestResponseBody;

public final class Netty4ChunkedHttpContinuation extends DefaultHttpResponse implements Netty4HttpResponse {
    private final int sequence;
    private final ChunkedRestResponseBody body;

    public Netty4ChunkedHttpContinuation(int sequence, HttpVersion version, HttpResponseStatus status, ChunkedRestResponseBody body) {
        // TODO shouldn't be a HttpResponse, this is purely body so headers/version/status make no sense here
        super(version, status);
        this.sequence = sequence;
        this.body = body;
    }

    @Override
    public int getSequence() {
        return sequence;
    }

    public ChunkedRestResponseBody body() {
        return body;
    }
}
