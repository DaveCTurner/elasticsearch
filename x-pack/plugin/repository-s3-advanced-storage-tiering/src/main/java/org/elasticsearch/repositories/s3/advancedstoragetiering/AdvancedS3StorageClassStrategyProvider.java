/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.s3.advancedstoragetiering;

import com.amazonaws.services.s3.model.StorageClass;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.s3.S3StorageClassStrategy;
import org.elasticsearch.repositories.s3.S3StorageClassStrategyProvider;
import org.elasticsearch.repositories.s3.SimpleS3StorageClassStrategyProvider;
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
                if (metadataStorageClass == dataStorageClass) {
                    return dataStorageClass;
                }

                final var licenseState = XPackPlugin.getSharedLicenseState();
                if (S3_ADVANCED_STORAGE_TIERING_FEATURE.check(licenseState) == false) {
                    throw new IllegalArgumentException(
                        Strings.format(
                            """
                                advanced storage tiering is not available with the current license level of [%s]; \
                                either install an appropriate license or remove the [%s] repository setting""",
                            licenseState.getOperationMode(),
                            METADATA_STORAGE_CLASS_SETTING.getKey()
                        )
                    );
                }

                if (operationPurpose == OperationPurpose.INDICES) { // TODO!
                    return dataStorageClass;
                } else {
                    return metadataStorageClass;
                }
            }

            @Override
            public String toString() {
                if (dataStorageClass == metadataStorageClass) {
                    return dataStorageClass.toString();
                } else {
                    return "Advanced[data=" + dataStorageClass + ", metadata=" + metadataStorageClass + "]";
                }
            }
        };
    }
}
