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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportMessageListener;
import org.elasticsearch.transport.TransportRequest;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class TaskTracer implements TransportMessageListener {

    private static final Logger logger = LogManager.getLogger(TaskTracer.class);

    private final List<TaskTracingListener> listeners = new CopyOnWriteArrayList<>();

    public void addListener(TaskTracingListener listener) {
        listeners.add(listener);
    }

    void onTaskRegistered(Task task) {
        listeners.forEach(l -> {
            try {
                l.onTaskRegistered(task);
            } catch (Exception e) {
                assert false : e;
                logger.warn(new ParameterizedMessage(
                        "task tracing listener [{}] failed on registration of task [{}][{}]",
                        l,
                        task.getId(),
                        task.getAction()), e);
            }
        });
    }

    void onTaskUnregistered(Task task) {
        listeners.forEach(l -> {
            try {
                l.onTaskUnregistered(task);
            } catch (Exception e) {
                assert false : e;
                logger.warn(new ParameterizedMessage(
                        "task tracing listener [{}] failed on unregistration of task [{}][{}]",
                        l,
                        task.getId(),
                        task.getAction()), e);
            }
        });
    }

    public void beforeRequestSent(
            DiscoveryNode node,
            long requestId,
            String action,
            TransportRequest request) {

        if (request.getParentTask() == TaskId.EMPTY_TASK_ID) {
            return;
        }

        listeners.forEach(l -> {
            try {
                l.onChildRequestStart(node, requestId, action, request.getParentTask());
            } catch (Exception e) {
                assert false : e;
                logger.warn(new ParameterizedMessage(
                        "task tracing listener [{}] failed on registration of child action [{}] of task [{}][{}] on node [{}]",
                        l,
                        action,
                        request.getParentTask(),
                        action,
                        node), e);
            }
        });

    }

    @Override
    public void onResponseReceived(long requestId, @SuppressWarnings("rawtypes") Transport.ResponseContext context) {
        onRequestComplete(requestId);
    }

    public void onRequestComplete(long requestId) {
        listeners.forEach(l -> {
            try {
                l.onRequestComplete(requestId);
            } catch (Exception e) {
                assert false : e;
                logger.warn(new ParameterizedMessage(
                        "task tracing listener [{}] failed on completion of child action with request id [{}]",
                        l,
                        requestId), e);
            }
        });
    }
}
