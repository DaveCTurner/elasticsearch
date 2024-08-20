/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.Map;

/**
 * The repository-verify-integrity tasks that this node is currently coordinating.
 */
public class OngoingRequests {

    private final Map<Long, RepositoryVerifyIntegrityResponseBuilder> ongoingRequests = ConcurrentCollections.newConcurrentMap();

    public Releasable registerResponseBuilder(long taskId, RepositoryVerifyIntegrityResponseBuilder responseBuilder) {
        final var previous = ongoingRequests.putIfAbsent(taskId, responseBuilder);
        if (previous != null) {
            final var exception = new IllegalStateException("already executing verify task [" + taskId + "]");
            assert false : exception;
            throw exception;
        }

        return Releasables.assertOnce(() -> {
            final var removed = ongoingRequests.remove(taskId, responseBuilder);
            if (removed == false) {
                final var exception = new IllegalStateException("already completed verify task [" + taskId + "]");
                assert false : exception;
                throw exception;
            }
        });
    }

    public RepositoryVerifyIntegrityResponseBuilder get(long taskId) {
        final var outerRequest = ongoingRequests.get(taskId);
        if (outerRequest == null) {
            throw new ResourceNotFoundException("verify task [" + taskId + "] not found");
        }
        return outerRequest;
    }
}
