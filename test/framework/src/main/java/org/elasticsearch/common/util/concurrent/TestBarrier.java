/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

public class TestBarrier extends CyclicBarrier {

    public TestBarrier(int parties) {
        super(parties);
    }

    @Override
    public int await() {
        try {
            return super.await();
        } catch (Exception e) {
            throw new AssertionError("unexpected", e);
        }
    }

    @Override
    public int await(long timeout, TimeUnit unit) {
        try {
            return super.await(timeout, unit);
        } catch (Exception e) {
            throw new AssertionError("unexpected", e);
        }
    }
}
