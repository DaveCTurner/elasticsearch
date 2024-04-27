/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.repositories.cleanup;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class CleanupRepositoryRequest extends AcknowledgedRequest<CleanupRepositoryRequest> {

    private String repository;

    public CleanupRepositoryRequest(TimeValue masterNodeTimeout, String repository) {
        super(masterNodeTimeout);
        this.repository = repository;
    }

    public CleanupRepositoryRequest(StreamInput in) throws IOException {
        super(MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT /* TODO bug!! should read this from the wire */);
        repository = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(repository);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (repository == null) {
            validationException = addValidationError("repository is null", null);
        }
        return validationException;
    }

    public String name() {
        return repository;
    }

    public void name(String repository) {
        this.repository = repository;
    }
}
