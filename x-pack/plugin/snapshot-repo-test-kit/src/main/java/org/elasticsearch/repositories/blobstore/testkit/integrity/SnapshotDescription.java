/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.Locale;

public record SnapshotDescription(SnapshotId snapshotId, long startTimeMillis, long endTimeMillis) {

    private static final DateFormatter dateFormatter = DateFormatter.forPattern(FormatNames.ISO8601.getName()).withLocale(Locale.ROOT);

    void writeXContent(XContentBuilder builder) throws IOException {
        builder.startObject("snapshot");
        builder.field("id", snapshotId.getUUID());
        builder.field("name", snapshotId.getName());
        if (startTimeMillis != 0) {
            builder.field("start_time", dateFormatter.format(Instant.ofEpochMilli(startTimeMillis)));
        }
        if (endTimeMillis != 0) {
            builder.field("end_time", dateFormatter.format(Instant.ofEpochMilli(endTimeMillis)));
        }
        builder.endObject();
    }
}
