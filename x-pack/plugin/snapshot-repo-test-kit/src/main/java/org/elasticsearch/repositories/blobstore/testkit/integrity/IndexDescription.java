/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public record IndexDescription(IndexId indexId, String indexMetadataBlob, int shardCount) {
    void writeXContent(XContentBuilder builder) throws IOException {
        MetadataVerifier.writeIndexId(indexId, builder, b -> b.field("metadata_blob", indexMetadataBlob).field("shards", shardCount));
    }
}
