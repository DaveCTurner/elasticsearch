/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.AbstractRefCounted;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static org.elasticsearch.tasks.TaskId.EMPTY_TASK_ID;

/**
 * Ensures that there's only at most one thread doing descendant-cancellation work.
 * <p>
 * When we receive a ban for a parent task we prevent future children from starting and then cancel all the current children. Finding
 * the current children is nontrivial, and handling a lot of bans at the same time may be overwhelming.
 */
public class DescendantCancellationService {

    private static final Logger logger = LogManager.getLogger(DescendantCancellationService.class);

    private final TaskCancellationService taskCancellationService;
    private final Consumer<Consumer<CancellableTask>> taskIterator;
    private final ExecutorService executor;

    // access to these fields is synchronized on `this`
    private Map<TaskId, RefCountingListener> currentListeners = new HashMap<>(); // keyed by the ID of the banned parent
    private boolean isActive; // if a ban-processing run is already planned

    DescendantCancellationService(
        TaskCancellationService taskCancellationService,
        Consumer<Consumer<CancellableTask>> taskIterator,
        ExecutorService executor
    ) {
        this.taskCancellationService = taskCancellationService;
        this.taskIterator = taskIterator;
        this.executor = executor;
    }

    /**
     * Enqueue the given ban request for processing, and spawn a ban-processing run if one is not already planned.
     */
    void cancelChildren(final BanParentTaskRequest banRequest, final ActionListener<Void> listener) {
        if (banRequest.getBannedParentTaskId().equals(EMPTY_TASK_ID)) {
            assert false;
            throw new IllegalArgumentException("cannot ban child tasks with no parent");
        }

        synchronized (this) {
            currentListeners.put(banRequest.getBannedParentTaskId(), new RefCountingListener(banRequest, listener));
        }

        pollNext();
    }

    private void processNext(final Map<TaskId, RefCountingListener> listenersToProcess) {
        try {
            // Simply iterate through all the tasks looking for ones whose parent task ID is banned by looking them up in the map.
            // We do this rather than using a better data structure because task cancellation is rare, we mostly look up tasks by their own
            // ID, so maintaining a separate data structure with more efficient lookups by parent ID would be excessively expensive for how
            // often it's used. Task cancellation is a best-effort thing anyway so it's ok if we're a bit slow here.
            taskIterator.accept(t -> {
                final TaskId parentTaskId = t.getParentTaskId();
                if (parentTaskId.equals(EMPTY_TASK_ID)) {
                    return;
                }
                final RefCountingListener refCountingListener = listenersToProcess.get(parentTaskId);
                if (refCountingListener == null) {
                    return;
                }

                refCountingListener.incRef();
                taskCancellationService.cancelTaskAndDescendants(
                    t,
                    refCountingListener.getRequest().getReason(),
                    refCountingListener.getRequest().getWaitForCompletion(),
                    refCountingListener);
            });

            for (RefCountingListener refCountingListener : listenersToProcess.values()) {
                refCountingListener.decRef();
            }
        } finally {
            synchronized (this) {
                assert isActive;
                isActive = false;
            }
            pollNext();
        }
    }

    private void pollNext() {
        final Map<TaskId, RefCountingListener> listenersToProcess;
        synchronized (this) {
            if (isActive || currentListeners.isEmpty()) {
                return;
            }

            isActive = true;
            listenersToProcess = currentListeners;
            currentListeners = new HashMap<>();
        }

        if (logger.isTraceEnabled()) {
            logger.trace("cancelling descendants of banned parents [{}]", listenersToProcess.keySet());
        }
        executor.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                assert false : e;
                logger.warn("failed to execute cancellation task", e);
            }

            @Override
            protected void doRun() {
                processNext(listenersToProcess);
            }
        });
    }

    private static class RefCountingListener extends AbstractRefCounted implements ActionListener<Void> {

        private final BanParentTaskRequest request;
        private final ActionListener<Void> listener;

        RefCountingListener(BanParentTaskRequest banRequest, ActionListener<Void> listener) {
            super("counting");
            this.request = banRequest;
            this.listener = listener;
            assert request.isBan();
        }

        @Override
        protected void closeInternal() {
            listener.onResponse(null);
        }

        BanParentTaskRequest getRequest() {
            return request;
        }

        @Override
        public void onResponse(Void unused) {
            decRef();
        }

        @Override
        public void onFailure(Exception e) {
            assert false : e;
            logger.warn(new ParameterizedMessage("failed to cancel child of [{}]", request.getBannedParentTaskId()), e);
            decRef();
        }
    }

}
