/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util.concurrent;

//import org.apache.logging.log4j.message.ParameterizedMessage;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A basic RefCounted implementation that is initialized with a
 * ref count of 1 and calls {@link #closeInternal()} once it reaches
 * a 0 ref count
 */
public abstract class AbstractRefCounted implements RefCounted {
//    private static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(AbstractRefCounted.class);

    private final AtomicInteger refCount = new AtomicInteger(1);
    private final String name;

    public AbstractRefCounted(String name) {
//        logger.info(new ParameterizedMessage("--> [{}] created, refcount now [1]", System.identityHashCode(this)), new Exception("stack trace"));
        this.name = name;
    }

    @Override
    public final void incRef() {
        if (tryIncRef() == false) {
            alreadyClosed();
        }
    }

    @Override
    public final boolean tryIncRef() {
        do {
            int i = refCount.get();
            if (i > 0) {
                if (refCount.compareAndSet(i, i + 1)) {
//                    logger.info(new ParameterizedMessage("--> [{}] tryIncRef succeeded, refcount now [{}]", System.identityHashCode(this), i + 1), new Exception("stack trace"));
                    return true;
                }
            } else {
//                logger.info(new ParameterizedMessage("--> [{}] tryIncRef failed", System.identityHashCode(this)), new Exception("stack trace"));
                return false;
            }
        } while (true);
    }

    @Override
    public final void decRef() {
        int i = refCount.decrementAndGet();
//        logger.info(new ParameterizedMessage("--> [{}] decref, refcount now [{}]", System.identityHashCode(this), i), new Exception("stack trace"));
        assert i >= 0 : "[" + System.identityHashCode(this) + "] has negative refcount";
        if (i == 0) {
            closeInternal();
        }

    }

    protected void alreadyClosed() {
        throw new IllegalStateException(name + " is already closed can't increment refCount current count [" + refCount.get() + "]");
    }

    /**
     * Returns the current reference count.
     */
    public int refCount() {
        return this.refCount.get();
    }


    /** gets the name of this instance */
    public String getName() {
        return name;
    }

    protected abstract void closeInternal();
}
