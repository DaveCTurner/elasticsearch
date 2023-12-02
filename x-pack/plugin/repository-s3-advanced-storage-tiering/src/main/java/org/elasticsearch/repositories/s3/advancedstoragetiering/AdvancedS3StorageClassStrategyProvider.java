/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.s3.advancedstoragetiering;

import com.amazonaws.services.s3.model.StorageClass;

import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.s3.S3StorageClassStrategy;
import org.elasticsearch.repositories.s3.S3StorageClassStrategyProvider;

public class AdvancedS3StorageClassStrategyProvider implements S3StorageClassStrategyProvider {
    @Override
    public S3StorageClassStrategy getS3StorageClassStrategy(Settings repositorySettings) {
        return new S3StorageClassStrategy() {
            @Override
            public StorageClass getStorageClass(OperationPurpose operationPurpose) {
                return StorageClass.Standard; // TODO!
            }

            @Override
            public String toString() {
                return "Advanced"; // TODO!
            }
        };
    }
}
