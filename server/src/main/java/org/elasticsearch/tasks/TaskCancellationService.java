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
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ResultDeduplicator;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.Objects;

public class TaskCancellationService {
    public static final String BAN_PARENT_ACTION_NAME = "internal:admin/tasks/ban";
    private static final Logger logger = LogManager.getLogger(TaskCancellationService.class);
    private final TransportService transportService;
    private final TaskManager taskManager;
    private final ResultDeduplicator<CancelRequest, Void> deduplicator = new ResultDeduplicator<>();
    private final DescendantCancellationService descendentCancellationService;

    public TaskCancellationService(TransportService transportService) {
        this.transportService = transportService;
        this.taskManager = transportService.getTaskManager();
        this.descendentCancellationService = new DescendantCancellationService(
            this,
            taskManager::forEachCancellableTask,
            // TODO can we use MANAGEMENT here or does this mean we won't be able to cancel our way out of an overloaded MANAGEMENT pool?
            transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT));
        transportService.registerRequestHandler(BAN_PARENT_ACTION_NAME, ThreadPool.Names.SAME, BanParentTaskRequest::new,
            new BanParentRequestHandler());
    }

    private String localNodeId() {
        return transportService.getLocalNode().getId();
    }

    private static class CancelRequest {
        final CancellableTask task;
        final boolean waitForCompletion;

        CancelRequest(CancellableTask task, boolean waitForCompletion) {
            this.task = task;
            this.waitForCompletion = waitForCompletion;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final CancelRequest that = (CancelRequest) o;
            return waitForCompletion == that.waitForCompletion && Objects.equals(task, that.task);
        }

        @Override
        public int hashCode() {
            return Objects.hash(task, waitForCompletion);
        }
    }

    void cancelTaskAndDescendants(CancellableTask task, String reason, boolean waitForCompletion, ActionListener<Void> finalListener) {
        deduplicator.executeOnce(new CancelRequest(task, waitForCompletion), finalListener,
            (r, listener) -> doCancelTaskAndDescendants(task, reason, waitForCompletion, listener));
    }

    void doCancelTaskAndDescendants(CancellableTask task, String reason, boolean waitForCompletion, ActionListener<Void> listener) {
        final TaskId taskId = task.taskInfo(localNodeId(), false).getTaskId();
        if (task.shouldCancelChildrenOnCancellation()) {
            logger.trace("cancelling task [{}] and its descendants", taskId);
            StepListener<Void> completedListener = new StepListener<>();
            GroupedActionListener<Void> groupedListener = new GroupedActionListener<>(completedListener.map(r -> null), 3);
            Collection<Transport.Connection> childConnections = taskManager.startBanOnChildTasks(task.getId(), reason, () -> {
                logger.trace("child tasks of parent [{}] are completed", taskId);
                groupedListener.onResponse(null);
            });
            taskManager.cancel(task, reason, () -> {
                logger.trace("task [{}] is cancelled", taskId);
                groupedListener.onResponse(null);
            });
            StepListener<Void> setBanListener = new StepListener<>();
            setBanOnChildConnections(reason, waitForCompletion, task, childConnections, setBanListener);
            setBanListener.addListener(groupedListener);
            // If we start unbanning when the last child task completed and that child task executed with a specific user, then unban
            // requests are denied because internal requests can't run with a user. We need to remove bans with the current thread context.
            final Runnable removeBansRunnable = transportService.getThreadPool().getThreadContext()
                .preserveContext(() -> removeBanOnChildConnections(task, childConnections));
            // We remove bans after all child tasks are completed although in theory we can do it on a per-connection basis.
            completedListener.whenComplete(r -> removeBansRunnable.run(), e -> removeBansRunnable.run());
            // if wait_for_completion is true, then only return when (1) bans are placed on child connections, (2) child tasks are
            // completed or failed, (3) the main task is cancelled. Otherwise, return after bans are placed on child connections.
            if (waitForCompletion) {
                completedListener.addListener(listener);
            } else {
                setBanListener.addListener(listener);
            }
        } else {
            logger.trace("task [{}] doesn't have any children that should be cancelled", taskId);
            if (waitForCompletion) {
                taskManager.cancel(task, reason, () -> listener.onResponse(null));
            } else {
                taskManager.cancel(task, reason, () -> {});
                listener.onResponse(null);
            }
        }
    }

    private void setBanOnChildConnections(String reason, boolean waitForCompletion, CancellableTask task,
                                          Collection<Transport.Connection> childConnections, ActionListener<Void> listener) {
        if (childConnections.isEmpty()) {
            listener.onResponse(null);
            return;
        }
        final TaskId taskId = new TaskId(localNodeId(), task.getId());
        logger.trace("cancelling child tasks of [{}] on child connections {}", taskId, childConnections);
        GroupedActionListener<Void> groupedListener = new GroupedActionListener<>(listener.map(r -> null), childConnections.size());
        final BanParentTaskRequest banRequest = BanParentTaskRequest.createSetBanParentTaskRequest(taskId, reason, waitForCompletion);
        for (Transport.Connection connection : childConnections) {
            assert TransportService.unwrapConnection(connection) == connection : "Child connection must be unwrapped";
            transportService.sendRequest(connection, BAN_PARENT_ACTION_NAME, banRequest, TransportRequestOptions.EMPTY,
                new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                    @Override
                    public void handleResponse(TransportResponse.Empty response) {
                        logger.trace("sent ban for tasks with the parent [{}] for connection [{}]", taskId, connection);
                        groupedListener.onResponse(null);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        final Throwable cause = ExceptionsHelper.unwrapCause(exp);
                        assert cause instanceof ElasticsearchSecurityException == false;
                        if (isUnimportantBanFailure(cause)) {
                            logger.debug(
                                new ParameterizedMessage(
                                    "cannot send ban for tasks with the parent [{}] on connection [{}]",
                                    taskId,
                                    connection),
                                exp);
                        } else if (logger.isDebugEnabled()) {
                            logger.warn(
                                new ParameterizedMessage(
                                    "cannot send ban for tasks with the parent [{}] on connection [{}]",
                                    taskId,
                                    connection),
                                exp);
                        } else {
                            logger.warn(
                                "cannot send ban for tasks with the parent [{}] on connection [{}]: {}",
                                taskId,
                                connection,
                                exp.getMessage());
                        }

                        groupedListener.onFailure(exp);
                    }
                });
        }
    }

    private void removeBanOnChildConnections(CancellableTask task, Collection<Transport.Connection> childConnections) {
        final BanParentTaskRequest request =
            BanParentTaskRequest.createRemoveBanParentTaskRequest(new TaskId(localNodeId(), task.getId()));
        for (Transport.Connection connection : childConnections) {
            assert TransportService.unwrapConnection(connection) == connection : "Child connection must be unwrapped";
            logger.trace(
                "Sending remove ban for tasks with the parent [{}] for connection [{}]",
                request.getBannedParentTaskId(),
                connection);
            transportService.sendRequest(connection, BAN_PARENT_ACTION_NAME, request, TransportRequestOptions.EMPTY,
                new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                    @Override
                    public void handleException(TransportException exp) {
                        final Throwable cause = ExceptionsHelper.unwrapCause(exp);
                        assert cause instanceof ElasticsearchSecurityException == false;
                        if (isUnimportantBanFailure(cause)) {
                            logger.debug(
                                new ParameterizedMessage(
                                    "failed to remove ban for tasks with the parent [{}] on connection [{}]",
                                    request.getBannedParentTaskId(),
                                    connection),
                                exp);
                        } else if (logger.isDebugEnabled()) {
                            logger.warn(
                                new ParameterizedMessage(
                                    "failed to remove ban for tasks with the parent [{}] on connection [{}]",
                                    request.getBannedParentTaskId(),
                                    connection),
                                exp);
                        } else {
                            logger.warn(
                                "failed to remove ban for tasks with the parent [{}] on connection [{}]: {}",
                                request.getBannedParentTaskId(),
                                connection,
                                exp.getMessage());
                        }
                    }
                });
        }
    }

    private static boolean isUnimportantBanFailure(Throwable cause) {
        return cause instanceof NodeDisconnectedException || cause instanceof NodeNotConnectedException;
    }

    private class BanParentRequestHandler implements TransportRequestHandler<BanParentTaskRequest> {
        @Override
        public void messageReceived(final BanParentTaskRequest request, final TransportChannel channel, Task task) throws Exception {
            if (request.isBan()) {
                logger.debug(
                    "Received ban for the parent [{}] on the node [{}], reason: [{}]",
                    request.getParentTask(),
                    localNodeId(),
                    request.getReason());
                taskManager.setBan(request.getBannedParentTaskId(), request.getReason(), channel);
                descendentCancellationService.cancelChildren(
                    request,
                    new ChannelActionListener<>(
                        channel,
                        BAN_PARENT_ACTION_NAME,
                        request).map(r -> TransportResponse.Empty.INSTANCE));
            } else {
                logger.debug("Removing ban for the parent [{}] on the node [{}]", request.getBannedParentTaskId(), localNodeId());
                taskManager.removeBan(request.getBannedParentTaskId());
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            }
        }
    }
}
