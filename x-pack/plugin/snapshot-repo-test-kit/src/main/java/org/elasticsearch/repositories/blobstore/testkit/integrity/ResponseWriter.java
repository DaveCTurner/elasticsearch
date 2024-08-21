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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public interface ResponseWriter {

    record ResponseChunk(
        String anomaly,
        SnapshotDescription snapshotDescription,
        IndexDescription indexDescription,
        int shardId,
        ShardGeneration shardGeneration,
        String blobName,
        String physicalFileName,
        int partIndex,
        int partCount,
        ByteSizeValue fileLength,
        ByteSizeValue partLength,
        ByteSizeValue blobLength,
        int totalSnapshotCount,
        int restorableSnapshotCount,
        Exception exception
    ) implements Writeable, ToXContentFragment {

        public ResponseChunk {
            if (fileLength == null
                || partLength == null
                || blobLength == null
                || shardId < -1
                || partIndex < -1
                || partCount < -1
                || totalSnapshotCount < -1
                || restorableSnapshotCount < -1
                || (totalSnapshotCount >= 0 != restorableSnapshotCount >= 0)) {
                throw new IllegalArgumentException("invalid: " + this);
            }
        }

        public ResponseChunk(StreamInput in) throws IOException {
            this(
                in.readOptionalString(),
                in.readOptionalWriteable(SnapshotDescription::new),
                in.readOptionalWriteable(IndexDescription::new),
                in.readInt(),
                in.readOptionalWriteable(ShardGeneration::new),
                in.readOptionalString(),
                in.readOptionalString(),
                in.readInt(),
                in.readInt(),
                ByteSizeValue.readFrom(in),
                ByteSizeValue.readFrom(in),
                ByteSizeValue.readFrom(in),
                in.readInt(),
                in.readInt(),
                in.readOptionalWriteable(StreamInput::readException)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(anomaly);
            out.writeOptionalWriteable(snapshotDescription);
            out.writeOptionalWriteable(indexDescription);
            out.writeInt(shardId);
            out.writeOptionalWriteable(shardGeneration);
            out.writeOptionalString(blobName);
            out.writeOptionalString(physicalFileName);
            out.writeInt(partIndex);
            out.writeInt(partCount);
            out.writeOptionalWriteable(fileLength);
            out.writeOptionalWriteable(partLength);
            out.writeOptionalWriteable(blobLength);
            out.writeInt(totalSnapshotCount);
            out.writeInt(restorableSnapshotCount);
            out.writeException(exception);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (anomaly() != null) {
                builder.field("anomaly", anomaly());
            }
            if (snapshotDescription() != null) {
                builder.startObject("snapshot");
                builder.field("name", snapshotDescription().snapshotId().getName());
                builder.field("uuid", snapshotDescription().snapshotId().getUUID());
                if (snapshotDescription().startTimeMillis() > 0) {
                    builder.timeField("start_time_millis", "start_time", snapshotDescription().startTimeMillis());
                }
                if (snapshotDescription().endTimeMillis() > 0) {
                    builder.timeField("end_time_millis", "end_time", snapshotDescription().endTimeMillis());
                }
                builder.endObject();
            }
            if (indexDescription() != null) {
                builder.startObject("index");
                builder.field("name", indexDescription().indexId().getName());
                builder.field("uuid", indexDescription().indexId().getId());
                if (indexDescription().indexMetadataBlob() != null) {
                    builder.field("metadata_blob", indexDescription().indexMetadataBlob());
                }
                if (indexDescription().shardCount() > 0) {
                    builder.field("shards", indexDescription().shardCount());
                }
                builder.endObject();
            }
            if (shardId() >= 0) {
                builder.field("shard_id", shardId());
            }
            if (shardGeneration() != null) {
                builder.field("shard_generation", shardGeneration());
            }
            if (blobName() != null) {
                builder.field("blob_name", blobName());
            }
            if (physicalFileName() != null) {
                builder.field("physical_file_name", physicalFileName());
            }
            if (partIndex() >= 0) {
                builder.field("part_index", partIndex());
            }
            if (partCount() >= 0) {
                builder.field("part_count", partCount());
            }
            if (fileLength() != ByteSizeValue.MINUS_ONE) {
                builder.humanReadableField("file_length_in_bytes", "file_length", fileLength());
            }
            if (partLength() != ByteSizeValue.MINUS_ONE) {
                builder.humanReadableField("part_length_in_bytes", "part_length", partLength());
            }
            if (blobLength() != ByteSizeValue.MINUS_ONE) {
                builder.humanReadableField("blob_length_in_bytes", "blob_length", blobLength());
            }
            if (totalSnapshotCount() >= 0 && restorableSnapshotCount() >= 0) {
                builder.startObject("snapshots");
                builder.field("total_count", totalSnapshotCount());
                builder.field("restorable_count", totalSnapshotCount());
                builder.endObject();
            }
            return builder;
        }
    }

    void writeResponseChunk(ResponseChunk responseChunk, Releasable releasable);
}
