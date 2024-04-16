/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.cluster;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.resolve.ResolveClusterActionRequest;
import org.elasticsearch.action.admin.indices.resolve.TransportResolveClusterAction;
import org.elasticsearch.common.util.concurrent.ThrottledIterator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.AbstractMultiClustersTestCase;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class ResolveClusterDisruptionIT extends AbstractMultiClustersTestCase {

    private static final String REMOTE_CLUSTER = "remote-cluster";

    @Override
    protected Collection<String> remoteClusterAlias() {
        return skipUnavailableForRemoteClusters().keySet();
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER, true);
    }

    public void testResolveClusterActionDuringRestart() throws Exception {
        final var warmedUpLatch = new CountDownLatch(10);
        final var finishedLatch = new CountDownLatch(1);

        try (var iterator = new StoppableIterator()) {
            ThrottledIterator.run(
                iterator,
                (ref, ignored) -> client().execute(
                    TransportResolveClusterAction.TYPE,
                    new ResolveClusterActionRequest(new String[] { REMOTE_CLUSTER + ":*" }),
                    ActionListener.releasing(ref)
                ),
                5,
                warmedUpLatch::countDown,
                finishedLatch::countDown
            );
            safeAwait(warmedUpLatch);
            cluster(REMOTE_CLUSTER).fullRestart();
        } finally {
            safeAwait(finishedLatch);
        }
    }

    private static final class StoppableIterator implements Iterator<Void>, Releasable {
        private boolean keepGoing = true;

        @Override
        public boolean hasNext() {
            return keepGoing;
        }

        @Override
        public Void next() {
            return null;
        }

        @Override
        public synchronized void close() {
            keepGoing = false;
        }
    }
}
