/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

public class PlainActionFuture<T> extends AdapterActionFuture<T, T> {

    public static <T> PlainActionFuture<T> newFuture() {
        return new PlainActionFuture<>();
    }

    @Override
    protected final T convert(T listenerResponse) {
        return listenerResponse;
    }
}
