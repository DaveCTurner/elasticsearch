/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3;

import com.amazonaws.services.s3.model.StorageClass;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * A provider for a {@link S3StorageClassStrategy}, which may itself be provided via SPI.
 */
public interface S3StorageClassStrategyProvider {
    S3StorageClassStrategy getS3StorageClassStrategy(ThreadPool threadPool, Settings repositorySettings);

    static StorageClass parseStorageClass(String storageClassString) {
        return S3BlobStore.initStorageClass(storageClassString);
    }
}
