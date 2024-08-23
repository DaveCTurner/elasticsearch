/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class RepositoryVerifyIntegrityResponse extends ActionResponse {
    private final long originalRepositoryGeneration;
    private final long finalRepositoryGeneration;

    RepositoryVerifyIntegrityResponse(long originalRepositoryGeneration, long finalRepositoryGeneration) {
        this.originalRepositoryGeneration = originalRepositoryGeneration;
        this.finalRepositoryGeneration = finalRepositoryGeneration;
    }

    RepositoryVerifyIntegrityResponse(StreamInput in) throws IOException {
        originalRepositoryGeneration = in.readLong();
        finalRepositoryGeneration = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(originalRepositoryGeneration);
        out.writeLong(finalRepositoryGeneration);
    }

    public long originalRepositoryGeneration() {
        return originalRepositoryGeneration;
    }

    public long finalRepositoryGeneration() {
        return finalRepositoryGeneration;
    }
}
