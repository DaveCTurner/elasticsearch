/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.env;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.index.shard.ShardId;

import java.util.Map;
import java.util.Queue;

class ShardStoreService implements ClusterStateListener {

    private final Map<ShardId, ShardStore> shards = ConcurrentCollections.newConcurrentMap();

    @Override
    public void clusterChanged(ClusterChangedEvent event) {

    }

    private ShardStore acquireOrRecreateShardStore(ShardId shardId, @Nullable ShardStore shardStore) {
        if (shardStore == null || shardStore.tryIncRef() == false) {
            return new ShardStore(shardId);
        } else {
            return shardStore;
        }
    }

    SubscribableListener<RefCounted> acquireShardLock(ShardId shardId, ActionListener<RefCounted> listener) {
        final var shardStore = shards.compute(shardId, this::acquireOrRecreateShardStore);
        try {
            assert shardStore.hasReferences();
            final var subscribableListener = new SubscribableListener<RefCounted>();
            subscribableListener.addListener(ActionListener.releaseAfter(listener, shardStore::decRef));
            shardStore.startWait(subscribableListener);
            return subscribableListener;
        } finally {
            shardStore.decRef();
        }
    }

    private class ShardStore extends AbstractRefCounted {

        private final ShardId shardId;
        private final Queue<ShardStoreUser> waitingUsers = ConcurrentCollections.newQueue();
        private boolean inUse;
        private ActionListener<RefCounted> currentCleanup;

        // Interesting observation: shards are always either in-use or in the process of cleaning up.

        ShardStore(ShardId shardId) {
            this.shardId = shardId;
        }

        ShardId shardId() {
            return shardId;
        }

        @Override
        protected void closeInternal() {
            assert assertNotInUse();
            shards.remove(shardId);
        }

        private synchronized boolean assertNotInUse() {
            assert inUse == false;
            return true;
        }

        void startWait(SubscribableListener<RefCounted> listener) {
            ActionListener<>
            synchronized (ShardStore.this) {
                if (inUse) {
                    assert currentCleanup == null;
                    mustIncRef();
                    final var waitingUser = new ShardStoreUser(listener);
                    waitingUsers.add(waitingUser);
                    listener.addListener(ActionListener.running(waitingUser::removeFromQueueAndDecRef));
                    assert listener.isDone() == false;
                    return;
                }
                currentCleanup = null;
                inUse = true;
            }
            acquired(listener);
        }

        private void acquired(SubscribableListener<RefCounted> listener) {
            mustIncRef();
            final var ref = AbstractRefCounted.of(this::released);
            try {
                assert Thread.holdsLock(ShardStore.this) == false;
                listener.onResponse(ref);
            } finally {
                ref.decRef();
            }
        }

        private void released() {
            try {
                ShardStoreUser nextUser;
                synchronized (ShardStore.this) {
                    assert inUse;
                    do {
                        nextUser = waitingUsers.poll();
                        if (nextUser == null) {
                            inUse = false;
                            assert currentCleanup == null;
                            currentCleanup = new ShardStoreCleanup();
                            // TODO start a cleanup process here (outside the mutex tho) which waits/checks for the cleanup conditions
                            // (e.g. shard goes green elsewhere, or index is removed from the cluster). If & when the cleanup conditions are
                            // met then acquire the store to do the cleanup, but ofc cancel the cleanup if the store is acquired by a new
                            // user instead.
                            //
                            // Do we cancel the cleanup on each new acquire? Or just if "nothing changes" before cleanup is due?
                            return;
                        }
                    } while (nextUser.listener.isDone());
                }
                acquired(nextUser.listener);
            } finally {
                decRef();
                if (currentCleanup != null) {
                    currentCleanup.run();
                }
            }
        }

        private class ShardStoreUser {
            SubscribableListener<RefCounted> listener;

            ShardStoreUser(SubscribableListener<RefCounted> listener) {
                this.listener = listener;
            }

            void removeFromQueueAndDecRef() {
                waitingUsers.remove(ShardStoreUser.this);
                ShardStore.this.decRef();
            }
        }

        private class ShardStoreCleanup implements ActionListener<RefCounted> {

            ShardStoreCleanup() {
                mustIncRef();
            }

            private boolean isCurrent() {
                synchronized (ShardStore.this) {
                    return currentCleanup == ShardStoreCleanup.this;
                }
            }

            @Override
            public void onResponse(RefCounted refCounted) {
                try {
                    if (isCurrent() == false) {
                        return;
                    }
                    currentCleanup = null;
                } finally {
                    decRef();
                }
            }

            @Override
            public void onFailure(Exception e) {
                decRef();
            }
        }
    }
}
