/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.IOException;

public record SnapshotDescription(SnapshotId snapshotId, long startTimeMillis, long endTimeMillis) implements Writeable {

    public SnapshotDescription(StreamInput in) throws IOException {
        this(new SnapshotId(in), in.readZLong(), in.readZLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        snapshotId.writeTo(out);
        out.writeZLong(startTimeMillis);
        out.writeZLong(endTimeMillis);
    }
}
