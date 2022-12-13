/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.test.CorruptionUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

public class BlobStoreMetadataIntegrityIT extends AbstractSnapshotIntegTestCase {

    private static final String REPOSITORY_NAME = "test-repo";

    public void testIntegrityCheck() throws Exception {

        disableRepoConsistencyCheck("testing integrity checks involves breaking the repo");

        final var repoPath = randomRepoPath();
        createRepository(
            REPOSITORY_NAME,
            "fs",
            Settings.builder().put(BlobStoreRepository.SUPPORT_URL_REPO.getKey(), false).put("location", repoPath)
        );
        final var repository = internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository(REPOSITORY_NAME);

        final var indexCount = between(1, 3);
        for (int i = 0; i < indexCount; i++) {
            createIndexWithRandomDocs("test-index-" + i, between(1, 1000));
        }

        for (int snapshotIndex = 0; snapshotIndex < 4; snapshotIndex++) {
            final var indexRequests = new ArrayList<IndexRequestBuilder>();
            for (int i = 0; i < indexCount; i++) {
                if (randomBoolean()) {
                    final var indexName = "test-index-" + i;
                    if (randomBoolean()) {
                        assertAcked(client().admin().indices().prepareDelete(indexName));
                        createIndexWithRandomDocs(indexName, between(1, 1000));
                    }
                    final var numDocs = between(1, 1000);
                    for (int doc = 0; doc < numDocs; doc++) {
                        indexRequests.add(client().prepareIndex(indexName).setSource("field1", "bar " + doc));
                    }
                }
            }
            indexRandom(true, indexRequests);
            assertEquals(0, client().admin().indices().prepareFlush().get().getFailedShards());
            createFullSnapshot(REPOSITORY_NAME, "test-snapshot-" + snapshotIndex);
        }

        final var successFuture = new PlainActionFuture<Void>();
        repository.verifyMetadataIntegrity(successFuture);
        successFuture.get(30, TimeUnit.SECONDS);
        final var tempDir = createTempDir();

        final List<Path> blobs;
        try (var paths = Files.walk(repoPath)) {
            blobs = paths.filter(Files::isRegularFile).sorted().toList();
        }
        for (final var blob : blobs) {
            logger.info("repo contents: {}", blob);
        }

        for (int i = 0; i < 200; i++) {
            final var blobToDamage = randomFrom(blobs);
            final var isDataBlob = blobToDamage.getFileName().toString().startsWith(BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX);
            logger.info("--> deleting {}", blobToDamage);

            if (isDataBlob || randomBoolean()) {
                Files.move(blobToDamage, tempDir.resolve("tmp"));
            } else {
                Files.copy(blobToDamage, tempDir.resolve("tmp"));
                CorruptionUtils.corruptFile(random(), blobToDamage);
            }
            try {
                final var failFuture = new PlainActionFuture<Void>();
                repository.verifyMetadataIntegrity(failFuture);
                final var exception = expectThrows(IllegalStateException.class, () -> failFuture.actionGet(30, TimeUnit.SECONDS));
                if (isDataBlob) {
                    assertThat(exception.getMessage(), containsString(blobToDamage.getFileName().toString()));
                }
            } finally {
                Files.deleteIfExists(blobToDamage);
                Files.move(tempDir.resolve("tmp"), blobToDamage);
            }

            final var repairFuture = new PlainActionFuture<Void>();
            repository.verifyMetadataIntegrity(repairFuture);
            repairFuture.get(30, TimeUnit.SECONDS);
        }
    }
}
