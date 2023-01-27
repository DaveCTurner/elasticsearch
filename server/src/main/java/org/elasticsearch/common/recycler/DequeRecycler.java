/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.recycler;

import org.elasticsearch.core.Nullable;

import java.lang.ref.WeakReference;
import java.util.Deque;

/**
 * A {@link Recycler} implementation based on a {@link Deque}. This implementation is NOT thread-safe.
 */
public class DequeRecycler<T> extends AbstractRecycler<T> {

    final Deque<WeakReference<T>> deque;
    final int maxSize;

    public DequeRecycler(C<T> c, Deque<WeakReference<T>> queue, int maxSize) {
        super(c);
        this.deque = queue;
        this.maxSize = maxSize;
    }

    @Override
    public V<T> obtain() {
        while (true) {
            final WeakReference<T> vRef = deque.pollFirst();
            if (vRef == null) {
                return new DV(null, c.newInstance(), false);
            }
            final T v = vRef.get();
            if (v != null) {
                return new DV(vRef, v, true);
            }
        }
    }

    /** Called before releasing an object, returns true if the object should be recycled and false otherwise. */
    protected boolean beforeRelease() {
        return deque.size() < maxSize;
    }

    /** Called after a release. */
    protected void afterRelease(boolean recycled) {
        // nothing to do
    }

    private class DV implements Recycler.V<T> {

        @Nullable // if freshly created: we only make the weak ref when we recycle it
        final WeakReference<T> ref;
        T value;
        final boolean recycled;

        DV(WeakReference<T> ref, T value, boolean recycled) {
            this.ref = ref;
            this.value = value;
            this.recycled = recycled;
        }

        @Override
        public T v() {
            return value;
        }

        @Override
        public boolean isRecycled() {
            return recycled;
        }

        @Override
        public void close() {
            if (value == null) {
                assert permitDoubleReleases;
                throw new IllegalStateException("recycler entry already released...");
            }
            final boolean recycle = beforeRelease();
            if (recycle) {
                c.recycle(value);
                deque.addFirst(ref == null ? new WeakReference<>(value) : ref);
            }
            value = null;
            afterRelease(recycle);
        }
    }
}
