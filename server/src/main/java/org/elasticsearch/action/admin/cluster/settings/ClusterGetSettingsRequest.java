/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class ClusterGetSettingsRequest extends MasterNodeReadRequest<ClusterGetSettingsRequest> {

    public ClusterGetSettingsRequest() {}

    public ClusterGetSettingsRequest(StreamInput in) throws IOException {
        super(in);
        assert in.getVersion().onOrAfter(Version.V_8_3_0) : in.getVersion();
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        assert out.getVersion().onOrAfter(Version.V_8_3_0) : out.getVersion();
        super.writeTo(out);
    }
}
