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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.s3.S3StorageClassStrategy;
import org.elasticsearch.repositories.s3.S3StorageClassStrategyProvider;
import org.elasticsearch.repositories.s3.SimpleS3StorageClassStrategyProvider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackPlugin;

import static org.elasticsearch.repositories.s3.SimpleS3StorageClassStrategyProvider.STORAGE_CLASS_SETTING;
import static org.elasticsearch.repositories.s3.advancedstoragetiering.S3AdvancedStorageTieringPlugin.S3_ADVANCED_STORAGE_TIERING_FEATURE;

public class AdvancedS3StorageClassStrategyProvider implements S3StorageClassStrategyProvider {
    /**
     * Sets the S3 storage class type for the backup metadata objects. Values may be standard, reduced_redundancy, standard_ia, onezone_ia
     * and intelligent_tiering. By default, falls back to {@link SimpleS3StorageClassStrategyProvider#STORAGE_CLASS_SETTING}.
     */
    public static final Setting<String> METADATA_STORAGE_CLASS_SETTING = Setting.simpleString(
        "metadata_storage_class",
        STORAGE_CLASS_SETTING
    );

    private static final Logger logger = LogManager.getLogger(AdvancedS3StorageClassStrategyProvider.class);

    @Override
    public S3StorageClassStrategy getS3StorageClassStrategy(ThreadPool threadPool, Settings repositorySettings) {
        return new S3StorageClassStrategy() {
            private static final long WARNING_INTERVAL_MILLIS = TimeValue.timeValueMinutes(10).millis();

            private final StorageClass dataStorageClass = S3StorageClassStrategyProvider.parseStorageClass(
                STORAGE_CLASS_SETTING.get(repositorySettings)
            );
            private final StorageClass metadataStorageClass = S3StorageClassStrategyProvider.parseStorageClass(
                METADATA_STORAGE_CLASS_SETTING.get(repositorySettings)
            );

            private long lastWarningTimeMillis = threadPool.relativeTimeInMillis() - WARNING_INTERVAL_MILLIS;

            private synchronized boolean needsNotAvailableWarning(long currentTimeMillis) {
                if (currentTimeMillis - lastWarningTimeMillis <= WARNING_INTERVAL_MILLIS) {
                    return false;
                }

                lastWarningTimeMillis = currentTimeMillis;
                return true;
            }

            @Override
            public StorageClass getStorageClass(OperationPurpose operationPurpose) {
                if (metadataStorageClass == dataStorageClass) {
                    return dataStorageClass;
                }

                final var licenseState = XPackPlugin.getSharedLicenseState();
                if (S3_ADVANCED_STORAGE_TIERING_FEATURE.check(licenseState) == false) {
                    if (needsNotAvailableWarning(threadPool.relativeTimeInMillis())) {
                        logger.warn(
                            "advanced storage tiering is not available with the current license level of [{}]",
                            licenseState.getOperationMode().description()
                        );
                    }
                    return dataStorageClass;
                }

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
