/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

/**
 * The only way to extend {@link ActionType} is to subclass this class. This is never necessary any more but there's a lot of legacy code
 * to clean up. New code should just use an instance of {@link ActionType} directly.
 */
@Deprecated(forRemoval = true)
public abstract non-sealed class UnnecessaryActionTypeSubclass<Response extends ActionResponse> extends ActionType<Response> {
    protected UnnecessaryActionTypeSubclass(String name) {
        super(name);
    }
}
