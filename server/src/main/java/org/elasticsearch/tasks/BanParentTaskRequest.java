/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

public class BanParentTaskRequest extends TransportRequest {

    private final TaskId parentTaskId;
    private final boolean ban;
    private final boolean waitForCompletion;
    private final String reason;

    static BanParentTaskRequest createSetBanParentTaskRequest(TaskId parentTaskId, String reason, boolean waitForCompletion) {
        return new BanParentTaskRequest(parentTaskId, reason, waitForCompletion);
    }

    static BanParentTaskRequest createRemoveBanParentTaskRequest(TaskId parentTaskId) {
        return new BanParentTaskRequest(parentTaskId);
    }

    private BanParentTaskRequest(TaskId parentTaskId, String reason, boolean waitForCompletion) {
        this.parentTaskId = parentTaskId;
        this.ban = true;
        this.reason = reason;
        this.waitForCompletion = waitForCompletion;
    }

    private BanParentTaskRequest(TaskId parentTaskId) {
        this.parentTaskId = parentTaskId;
        this.ban = false;
        this.reason = null;
        this.waitForCompletion = false;
    }

    BanParentTaskRequest(StreamInput in) throws IOException {
        super(in);
        parentTaskId = TaskId.readFromStream(in);
        ban = in.readBoolean();
        reason = ban ? in.readString() : null;
        if (in.getVersion().onOrAfter(Version.V_7_8_0)) {
            waitForCompletion = in.readBoolean();
        } else {
            waitForCompletion = false;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        parentTaskId.writeTo(out);
        out.writeBoolean(ban);
        if (ban) {
            out.writeString(reason);
        }
        if (out.getVersion().onOrAfter(Version.V_7_8_0)) {
            out.writeBoolean(waitForCompletion);
        }
    }

    public TaskId getBannedParentTaskId() {
        return parentTaskId;
    }

    public boolean isBan() {
        return ban;
    }

    public String getReason() {
        return reason;
    }

    public boolean getWaitForCompletion() {
        return waitForCompletion;
    }
}
