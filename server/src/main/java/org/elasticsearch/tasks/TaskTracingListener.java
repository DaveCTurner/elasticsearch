/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

import org.elasticsearch.cluster.node.DiscoveryNode;

/**
 * A listener for events related to tasks: notified when tasks are registered and unregistered, and when they send transport actions that
 * would spawn remote child tasks.
 */
public interface TaskTracingListener {

    /**
     * Called when the given task has been registered with the {@link TaskManager}.
     */
    void onTaskRegistered(Task task);

    /**
     * Called when the given task has been unregistered from the {@link TaskManager}.
     */
    void onTaskUnregistered(Task task);

    /**
     * Called when a transport request to start a child action is about to be sent.
     *
     * @param node       The target node of the child request
     * @param requestId  The (locally unique) ID of the request
     * @param action     The name of the transport action
     * @param parentTask The ID of the parent task
     */
    void onChildRequestStart(DiscoveryNode node, long requestId, String action, TaskId parentTask);

    /**
     * Called when a response to a transport request is received. May be called for requests that had no corresponding call to {@link
     * #onChildRequestStart} since when the request completes we no longer know whether it was a child request or not. Implementations must
     * filter out the uninteresting request completion events themselves.
     *
     * @param requestId The (locally unique) ID of the request.
     */
    void onRequestComplete(long requestId);
}
