/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.transport.TransportService;

/**
 * An action which can be invoked by {@link Client#execute}. The implementation must be registered with the node using
 * {@link ActionModule#setupActions} (for actions in the {@code :server} package) or {@link ActionPlugin#getActions} (for actions in
 * plugins).
 * <p>
 * Typically, every {@link UnnecessaryActionTypeSubclass} instance is a global constant (i.e. a public static final field) called {@code INSTANCE} or {@code
 * TYPE}. Some legacy implementations create custom subclasses of {@link UnnecessaryActionTypeSubclass} but this is unnecessary and somewhat wasteful. Prefer
 * to create instances of this class directly whenever possible.
 */
@SuppressWarnings("unused") // Response type arg is used to enable better type inference when calling Client#execute
public class UnnecessaryActionTypeSubclass<Response extends ActionResponse> {

    /**
     * Construct an {@link UnnecessaryActionTypeSubclass} with the given name.
     * <p>
     * There is no facility for directly executing an action on a different node in the local cluster. To achieve this, implement an action
     * which runs on the local node and knows how to use the {@link TransportService} to forward the request to a different node. There are
     * several utilities that help implement such an action, including {@link TransportNodesAction} or {@link TransportMasterNodeAction}.
     *
     * @param name The name of the action, which must be unique across actions.
     * @return an {@link UnnecessaryActionTypeSubclass} which callers can execute on the local node.
     * @deprecated Just create the {@link UnnecessaryActionTypeSubclass} directly.
     */
    @Deprecated(forRemoval = true)
    public static <T extends ActionResponse> UnnecessaryActionTypeSubclass<T> localOnly(String name) {
        return new UnnecessaryActionTypeSubclass<>(name);
    }

    private final String name;

    /**
     * Construct an {@link UnnecessaryActionTypeSubclass} with the given name.
     * <p>
     * There is no facility for directly executing an action on a different node in the local cluster. To achieve this, implement an action
     * which runs on the local node and knows how to use the {@link TransportService} to forward the request to a different node. There are
     * several utilities that help implement such an action, including {@link TransportNodesAction} or {@link TransportMasterNodeAction}.
     *
     * @param name The name of the action, which must be unique across actions.
     */
    public UnnecessaryActionTypeSubclass(String name) {
        this.name = name;
    }

    /**
     * The name of the action. Must be unique across actions.
     */
    public String name() {
        return this.name;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof UnnecessaryActionTypeSubclass<?> actionType && name.equals(actionType.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        return name;
    }
}
