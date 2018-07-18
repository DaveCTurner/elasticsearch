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

package org.elasticsearch.cluster.coordination;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import org.apache.lucene.util.Counter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolInfo;
import org.elasticsearch.threadpool.ThreadPoolStats;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

class DeterministicThreadPool extends ThreadPool {

    private final List<Runnable> pendingTasks = new ArrayList<>();
    private final ExecutorService executorService = new DeterministicExecutorService();

    DeterministicThreadPool(Settings settings) {
        super(settings);
        stopCachedTimeThread();
    }

    boolean hasPendingTasks() {
        return pendingTasks.isEmpty() == false;
    }

    void runNextTask() {
        runTask(0);
    }

    void runRandomTask(Random random) {
        runTask(RandomNumbers.randomIntBetween(random, 0, pendingTasks.size() - 1));
    }

    private void runTask(int index) {
        final Runnable task = pendingTasks.remove(index);
        logger.trace("running task {} of {}: {}", index, pendingTasks.size() + 1, task);
        task.run();
    }

    @Override
    public long relativeTimeInMillis() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long absoluteTimeInMillis() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Counter estimatedTimeInMillisCounter() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ThreadPoolInfo info() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Info info(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ThreadPoolStats stats() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExecutorService generic() {
        return super.generic();
    }

    @Override
    public ExecutorService executor(String name) {
        return generic();
    }

    @Override
    public ScheduledFuture<?> schedule(TimeValue delay, String executor, Runnable command) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Cancellable scheduleWithFixedDelay(Runnable command, TimeValue interval, String executor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Runnable preserveContext(Runnable command) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void shutdown() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void shutdownNow() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScheduledExecutorService scheduler() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ThreadContext getThreadContext() {
        throw new UnsupportedOperationException();
    }

    private class DeterministicExecutorService implements ExecutorService {

        @Override
        public void shutdown() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Runnable> shutdownNow() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isShutdown() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isTerminated() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Future<?> submit(Runnable task) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void execute(Runnable command) {
            pendingTasks.add(command);
        }
    }
}
