/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.s3.advancedstoragetiering;

import com.amazonaws.services.s3.model.StorageClass;

import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.s3.S3StorageClassStrategy;
import org.elasticsearch.repositories.s3.S3StorageClassStrategyProvider;
import org.elasticsearch.repositories.s3.SimpleS3StorageClassStrategyProvider;

import static org.elasticsearch.repositories.s3.SimpleS3StorageClassStrategyProvider.STORAGE_CLASS_SETTING;

public class AdvancedS3StorageClassStrategyProvider implements S3StorageClassStrategyProvider {
    /**
     * Sets the S3 storage class type for the backup metadata objects. Values may be standard, reduced_redundancy, standard_ia, onezone_ia
     * and intelligent_tiering. By default, falls back to {@link SimpleS3StorageClassStrategyProvider#STORAGE_CLASS_SETTING}.
     */
    public static final Setting<String> METADATA_STORAGE_CLASS_SETTING = Setting.simpleString(
        "metadata_storage_class",
        STORAGE_CLASS_SETTING
    );

    @Override
    public S3StorageClassStrategy getS3StorageClassStrategy(Settings repositorySettings) {
        return new S3StorageClassStrategy() {
            private final StorageClass dataStorageClass = S3StorageClassStrategyProvider.parseStorageClass(
                STORAGE_CLASS_SETTING.get(repositorySettings)
            );
            private final StorageClass metadataStorageClass = S3StorageClassStrategyProvider.parseStorageClass(
                METADATA_STORAGE_CLASS_SETTING.get(repositorySettings)
            );

            @Override
            public StorageClass getStorageClass(OperationPurpose operationPurpose) {
                if (operationPurpose == OperationPurpose.INDICES) { // TODO!
                    return dataStorageClass;
                } else {
                    return metadataStorageClass;
                }
            }

            @Override
            public String toString() {
                return "Advanced[data=" + dataStorageClass + ", metadata=" + metadataStorageClass + "]";
            }
        };
    }
}
