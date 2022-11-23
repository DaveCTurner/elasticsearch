/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.hamcrest.Matchers;

import java.util.HashSet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;

public class CountingPageCacheRecycler extends PageCacheRecycler {

    private static final Logger logger = LogManager.getLogger(CountingPageCacheRecycler.class);

    private static int nextPageId = 1;
    private HashSet<Integer> openPages = new HashSet<>();

    public CountingPageCacheRecycler() {
        super(Settings.EMPTY);
    }

    @Override
    public Recycler.V<byte[]> bytePage(boolean clear) {
        final var page = super.bytePage(clear);
        final var pageId = nextPageId++;
        openPages.add(pageId);
        logger.info("page [{}] acquired", pageId);
        return new Recycler.V<>() {
            boolean closed = false;

            @Override
            public byte[] v() {
                return page.v();
            }

            @Override
            public boolean isRecycled() {
                return page.isRecycled();
            }

            @Override
            public void close() {
                assertFalse(closed);
                closed = true;
                openPages.remove(pageId);
                page.close();
                logger.info("page [{}] released", pageId);
            }
        };
    }

    @Override
    public Recycler.V<Object[]> objectPage() {
        throw new AssertionError("unexpected call to objectPage()");
    }

    public void assertAllPagesReleased() {
        assertThat(openPages, Matchers.empty());
    }
}
