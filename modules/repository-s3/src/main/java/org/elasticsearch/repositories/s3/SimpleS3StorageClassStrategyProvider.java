/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3;

import com.amazonaws.services.s3.model.StorageClass;

import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

public class SimpleS3StorageClassStrategyProvider implements S3StorageClassStrategyProvider {

    public static final S3StorageClassStrategyProvider INSTANCE = new SimpleS3StorageClassStrategyProvider();

    private SimpleS3StorageClassStrategyProvider() {/* singleton */}

    /**
     * Sets the S3 storage class type for the backup data objects. Values may be standard, reduced_redundancy,
     * standard_ia, onezone_ia and intelligent_tiering. Defaults to standard.
     */
    public static final Setting<String> STORAGE_CLASS_SETTING = Setting.simpleString("storage_class");

    @Override
    public S3StorageClassStrategy getS3StorageClassStrategy(ThreadPool threadPool, Settings repositorySettings) {
        return new S3StorageClassStrategy() {
            private final StorageClass storageClass = S3StorageClassStrategyProvider.parseStorageClass(
                STORAGE_CLASS_SETTING.get(repositorySettings)
            );

            @Override
            public StorageClass getStorageClass(OperationPurpose operationPurpose) {
                return storageClass;
            }

            @Override
            public String toString() {
                return "Simple[" + storageClass + "]";
            }
        };
    }
}
