/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.recycler;

public class QueueRecyclerTests extends AbstractRecyclerTestCase {

    @Override
    protected Recycler<byte[]> newRecycler(int limit) {
        return Recyclers.concurrentDeque(RECYCLER_C, limit);
    }

    public void testGc() {
        Recycler<byte[]> r = newRecycler(limit);
        System.gc();
        obtainAndAssertFresh(r);
        System.gc();
        obtainAndAssertFresh(r);
    }

    private void obtainAndAssertFresh(Recycler<byte[]> r) {
        try (Recycler.V<byte[]> o = r.obtain()) {
            assertFalse(o.isRecycled());
            assertFresh(o.v());
        }
    }

}
