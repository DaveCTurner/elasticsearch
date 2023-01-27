/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.recycler;

abstract class AbstractRecycler<T> implements Recycler<T> {

    // flag to disable temporarily assertions so we can test the production behaviour of releasing a page twice
    protected static boolean permitDoubleReleases;

    protected final Recycler.C<T> c;

    protected AbstractRecycler(Recycler.C<T> c) {
        this.c = c;
    }

}
