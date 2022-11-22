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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.hamcrest.Matchers;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class CountingRecycler extends PageCacheRecycler {

    private static final Logger logger = LogManager.getLogger(CountingRecycler.class);

    private int pageCount = 0;
    private Set<Integer> openPageIds = new HashSet<>();
    private int openPages = 0;

    public CountingRecycler() {
        super(Settings.EMPTY);
    }

    @Override
    public Recycler.V<byte[]> bytePage(boolean clear) {
        final var pageIndex = pageCount++;
        logger.info("--> obtaining page [" + pageIndex + "]", new ElasticsearchException("stack trace"));
        final var page = super.bytePage(clear);
        openPages += 1;
        openPageIds.add(pageIndex);
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
                logger.info("--> releasing page [" + pageIndex + "]", new ElasticsearchException("stack trace"));
                assertFalse(closed);
                closed = true;
                openPages -= 1;
                openPageIds.remove(pageIndex);
                page.close();
            }
        };
    }

    @Override
    public Recycler.V<Object[]> objectPage() {
        throw new AssertionError("unexpected call to objectPage()");
    }

    public void close() {
        assertThat(openPageIds, Matchers.empty());
        assertEquals(0, openPages);
    }
}
