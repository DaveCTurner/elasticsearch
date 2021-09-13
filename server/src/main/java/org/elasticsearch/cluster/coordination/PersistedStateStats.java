/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

/**
 * Statistics about cluster state persistence, for example how much data has been persisted and how long it took.
 */
public class PersistedStateStats implements Writeable, ToXContentFragment {

    private final long documentCount;
    private final long documentBytes;
    private final long writeMillis;
    private final long prepareCommitCount;
    private final long prepareCommitMillis;
    private final long commitCount;
    private final long commitMillis;
    private final int documentBufferSize;

    public PersistedStateStats(StreamInput in) throws IOException {
        writeMillis = in.readVLong();
        prepareCommitMillis = in.readVLong();
        commitMillis = in.readVLong();
        documentCount = in.readVLong();
        documentBytes = in.readVLong();
        prepareCommitCount = in.readVLong();
        commitCount = in.readVLong();
        documentBufferSize = in.readVInt();
    }

    public PersistedStateStats(
        long documentCount,
        long documentBytes,
        long writeMillis,
        long prepareCommitCount,
        long prepareCommitMillis,
        long commitCount,
        long commitMillis,
        int documentBufferSize
    ) {
        this.documentCount = documentCount;
        this.documentBytes = documentBytes;
        this.writeMillis = writeMillis;
        this.prepareCommitCount = prepareCommitCount;
        this.prepareCommitMillis = prepareCommitMillis;
        this.commitCount = commitCount;
        this.commitMillis = commitMillis;
        this.documentBufferSize = documentBufferSize;
    }

    private static void msField(XContentBuilder builder, String name, long millis) throws IOException {
        builder.humanReadableField(name + "_time_millis", name + "_time", TimeValue.timeValueMillis(millis));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("cluster_state_persistence");
        {
            builder.field("doc_count", documentCount);
            builder.humanReadableField("doc_size_in_bytes", "doc_size", ByteSizeValue.ofBytes(documentBytes));
            msField(builder, "doc_write", writeMillis);
            builder.field("prepare_commit_count", prepareCommitCount);
            msField(builder, "prepare_commit", prepareCommitMillis);
            builder.field("commit_count", prepareCommitCount);
            msField(builder, "commit", commitMillis);
            builder.humanReadableField("buffer_size_in_bytes", "buffer_size", ByteSizeValue.ofBytes(documentBufferSize));
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(documentCount);
        out.writeVLong(documentBytes);
        out.writeVLong(writeMillis);
        out.writeVLong(prepareCommitMillis);
        out.writeVLong(commitMillis);
        out.writeVLong(prepareCommitCount);
        out.writeVLong(commitCount);
        out.writeVInt(documentBufferSize);
    }
}
