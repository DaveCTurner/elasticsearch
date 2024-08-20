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
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Objects;

public record RepositoryVerifyIntegrityParams(
    String repository,
    int metaThreadPoolConcurrency,
    int blobThreadPoolConcurrency,
    int snapshotVerificationConcurrency,
    int indexVerificationConcurrency,
    int indexSnapshotVerificationConcurrency,
    boolean verifyBlobContents,
    ByteSizeValue maxBytesPerSec
) implements Writeable {

    public RepositoryVerifyIntegrityParams {
        Objects.requireNonNull(repository, "repository");
        requireNonNegative("meta_thread_pool_concurrency", metaThreadPoolConcurrency);
        requireNonNegative("blob_thread_pool_concurrency", blobThreadPoolConcurrency);
        requireNonNegative("snapshot_verification_concurrency", snapshotVerificationConcurrency);
        requireNonNegative("index_verification_concurrency", indexVerificationConcurrency);
        requireNonNegative("indexSnapshot_verification_concurrency", indexSnapshotVerificationConcurrency);
        if (maxBytesPerSec.getBytes() < 1) {
            throw new IllegalArgumentException("invalid rate limit");
        }
    }

    private static void requireNonNegative(String name, int value) {
        if (value < 0) {
            throw new IllegalArgumentException("argument [" + name + "] must be at least [0]");
        }
    }

    RepositoryVerifyIntegrityParams(RestRequest restRequest) {
        this(
            restRequest.param("repository"),
            restRequest.paramAsInt("meta_thread_pool_concurrency", 0),
            restRequest.paramAsInt("blob_thread_pool_concurrency", 0),
            restRequest.paramAsInt("snapshot_verification_concurrency", 0),
            restRequest.paramAsInt("index_verification_concurrency", 0),
            restRequest.paramAsInt("indexSnapshot_verification_concurrency", 0),
            restRequest.paramAsBoolean("verify_blob_contents", false),
            restRequest.paramAsSize("max_bytes_per_sec", ByteSizeValue.ofMb(40))
        );
    }

    RepositoryVerifyIntegrityParams(StreamInput in) throws IOException {
        this(
            in.readString(),
            in.readVInt(),
            in.readVInt(),
            in.readVInt(),
            in.readVInt(),
            in.readVInt(),
            in.readBoolean(),
            ByteSizeValue.readFrom(in)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(repository);
        out.writeVInt(metaThreadPoolConcurrency);
        out.writeVInt(blobThreadPoolConcurrency);
        out.writeVInt(snapshotVerificationConcurrency);
        out.writeVInt(indexVerificationConcurrency);
        out.writeVInt(indexSnapshotVerificationConcurrency);
        out.writeBoolean(verifyBlobContents);
        maxBytesPerSec.writeTo(out);
    }

    public RepositoryVerifyIntegrityParams withResolvedDefaults(ThreadPool.Info metadataThreadPoolInfo) {
        if (metaThreadPoolConcurrency > 0
            && blobThreadPoolConcurrency > 0
            && snapshotVerificationConcurrency > 0
            && indexVerificationConcurrency > 0
            && indexSnapshotVerificationConcurrency > 0) {
            return this;
        }

        final var maxThreads = Math.max(1, metadataThreadPoolInfo.getMax());
        final var halfMaxThreads = Math.max(1, maxThreads / 2);
        return new RepositoryVerifyIntegrityParams(
            repository,
            metaThreadPoolConcurrency > 0 ? metaThreadPoolConcurrency : halfMaxThreads,
            blobThreadPoolConcurrency > 0 ? blobThreadPoolConcurrency : 1,
            snapshotVerificationConcurrency > 0 ? snapshotVerificationConcurrency : halfMaxThreads,
            indexVerificationConcurrency > 0 ? indexVerificationConcurrency : maxThreads,
            indexSnapshotVerificationConcurrency > 0 ? indexSnapshotVerificationConcurrency : 1,
            verifyBlobContents,
            maxBytesPerSec
        );
    }
}
