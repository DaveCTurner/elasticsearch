/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.tasks.TaskTracingListener;

import java.util.List;

/**
 * A plugin that supplies {@link TaskTracingListener}s in order to trace the lifecycles of tasks.
 */
public interface TaskTracerPlugin {

    /**
     * Creates and returns the listeners for tracing the lifecycles of tasks.
     */
    List<TaskTracingListener> getListeners();
}
