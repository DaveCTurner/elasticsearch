/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.action.support.PlainActionFuture;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class ActionListeners {

    /**
     * Execute an action which consumes a listener, wait for the listener to be completed or for a timeout of 30s to elapse, and return the
     * resulting response or throw the resulting exception.
     */
    public static <T> T get(Consumer<ActionListener<T>> e) {
        return get(e, 30, TimeUnit.SECONDS);
    }

    /**
     * Execute an action which consumes a listener, wait for the listener to be completed or for the timeout to elapse, and return the
     * resulting response or throw the resulting exception.
     */
    public static <T> T get(Consumer<ActionListener<T>> e, long timeout, TimeUnit unit) {
        PlainActionFuture<T> fut = PlainActionFuture.newFuture();
        e.accept(fut);
        return fut.actionGet(timeout, unit);
    }
}
