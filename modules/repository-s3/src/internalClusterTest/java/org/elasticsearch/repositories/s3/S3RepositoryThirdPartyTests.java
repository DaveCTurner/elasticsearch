/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.s3;

import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.AbstractThirdPartyRepositoryTestCase;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class S3RepositoryThirdPartyTests extends AbstractThirdPartyRepositoryTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(S3RepositoryPlugin.class);
    }

    @Override
    protected SecureSettings credentials() {
        assertThat(System.getProperty("test.s3.account"), not(blankOrNullString()));
        assertThat(System.getProperty("test.s3.key"), not(blankOrNullString()));
        assertThat(System.getProperty("test.s3.bucket"), not(blankOrNullString()));

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.access_key", System.getProperty("test.s3.account"));
        secureSettings.setString("s3.client.default.secret_key", System.getProperty("test.s3.key"));
        return secureSettings;
    }

    @Override
    protected void createRepository(String repoName) {
        Settings.Builder settings = Settings.builder()
            .put("bucket", System.getProperty("test.s3.bucket"))
            .put("base_path", System.getProperty("test.s3.base", "testpath"));
        final String endpoint = System.getProperty("test.s3.endpoint");
        if (endpoint != null) {
            settings.put("endpoint", endpoint);
        } else {
            // only test different storage classes when running against the default endpoint, i.e. a genuine S3 service
            if (randomBoolean()) {
                final String storageClass = randomFrom(
                    "standard",
                    "reduced_redundancy",
                    "standard_ia",
                    "onezone_ia",
                    "intelligent_tiering"
                );
                logger.info("--> using storage_class [{}]", storageClass);
                settings.put("storage_class", storageClass);
            }
        }
        AcknowledgedResponse putRepositoryResponse = clusterAdmin().preparePutRepository("test-repo")
            .setType("s3")
            .setSettings(settings)
            .get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
    }

    @TestLogging(reason = "nocommit", value = "org.apache.http.wire:TRACE,com.amazonaws.request:TRACE")
    public void testCompareAndExchangeCleanup() throws IOException {

        // construct our own repo instance so we can inject a threadpool that allows to control the passage of time
        try (
            var repository = new S3Repository(
                node().injector().getInstance(RepositoriesService.class).repository(TEST_REPO_NAME).getMetadata(),
                xContentRegistry(),
                node().injector().getInstance(PluginsService.class).filterPlugins(S3RepositoryPlugin.class).get(0).getService(),
                node().injector().getInstance(ClusterService.class),
                BigArrays.NON_RECYCLING_INSTANCE,
                new RecoverySettings(node().settings(), node().injector().getInstance(ClusterService.class).getClusterSettings())
            )
        ) {
            repository.start();

            final var blobStore = (S3BlobStore) repository.blobStore();
            final var blobContainer = (S3BlobContainer) blobStore.blobContainer(repository.basePath().add(getTestName()));

            try (var clientReference = blobStore.clientReference()) {

                logger.info("--> initial CAS");

                assertEquals(
                    Boolean.TRUE,
                    PlainActionFuture.<Boolean, RuntimeException>get(
                        future -> blobContainer.compareAndSetRegister("key", bytes(), bytes((byte) 1), future),
                        10,
                        TimeUnit.SECONDS
                    )
                );

                logger.info("--> initial CAS done, reading object back");

                assertEquals(
                    bytes((byte) 1),
                    PlainActionFuture.<BytesReference, RuntimeException>get(
                        future -> blobContainer.getRegister("key", future.map(OptionalBytesReference::bytesReference)),
                        10,
                        TimeUnit.SECONDS
                    )
                );

                logger.info("--> successfully read object back");

                final var client = clientReference.client();
                final var bucketName = S3Repository.BUCKET_SETTING.get(repository.getMetadata().settings());
                final var registerBlobPath = blobContainer.buildKey("key");

                logger.info("--> check object exists at {}:{}", bucketName, registerBlobPath);

                // check we're looking at the right blob
                assertArrayEquals(
                    new byte[] { (byte) 1 },
                    client.getObject(new GetObjectRequest(bucketName, registerBlobPath)).getObjectContent().readAllBytes()
                );

                logger.info("--> object exists; starting new upload");
                final var uploadId = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, registerBlobPath))
                    .getUploadId();

                assertEquals(
                    Boolean.FALSE,
                    PlainActionFuture.<Boolean, RuntimeException>get(
                        future -> blobContainer.compareAndSetRegister("key", bytes((byte) 1), bytes((byte) 2), future),
                        10,
                        TimeUnit.SECONDS
                    )
                );

                client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, registerBlobPath, uploadId));

                assertEquals(
                    Boolean.TRUE,
                    PlainActionFuture.<Boolean, RuntimeException>get(
                        future -> blobContainer.compareAndSetRegister("key", bytes((byte) 1), bytes((byte) 2), future),
                        10,
                        TimeUnit.SECONDS
                    )
                );

                logger.info("--> done");
                fail("boom");
            } finally {
                blobContainer.delete();
            }
        }
    }

    private static BytesReference bytes(byte... bytes) {
        return new BytesArray(bytes);
    }
}
