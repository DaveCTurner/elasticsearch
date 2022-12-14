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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshotsIntegritySuppressor;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.test.CorruptionUtils;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

public class BlobStoreMetadataIntegrityIT extends AbstractSnapshotIntegTestCase {

    private static final String REPOSITORY_NAME = "test-repo";

    private Releasable integrityCheckSuppressor;

    @Before
    public void suppressIntegrityChecks() {
        disableRepoConsistencyCheck("testing integrity checks involves breaking the repo");
        assertNull(integrityCheckSuppressor);
        integrityCheckSuppressor = new BlobStoreIndexShardSnapshotsIntegritySuppressor();
    }

    @After
    public void enableIntegrityChecks() {
        Releasables.closeExpectNoException(integrityCheckSuppressor);
        integrityCheckSuppressor = null;
    }

    public void testIntegrityCheck() throws Exception {
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

        assertThat(PlainActionFuture.get(repository::verifyMetadataIntegrity, 30, TimeUnit.SECONDS), empty());

        final var tempDir = createTempDir();

        final List<Path> blobs;
        try (var paths = Files.walk(repoPath)) {
            blobs = paths.filter(Files::isRegularFile).sorted().toList();
        }
        for (final var blob : blobs) {
            logger.info("repo contents: {}", blob);
        }

        final var repositoryDataFuture = new PlainActionFuture<RepositoryData>();
        repository.getRepositoryData(repositoryDataFuture);
        final var repositoryData = repositoryDataFuture.get();
        final var repositoryDataBlob = repoPath.resolve("index-" + repositoryData.getGenId());

        for (int i = 0; i < 2000; i++) {
            final var blobToDamage = randomFrom(blobs);
            final var isDataBlob = blobToDamage.getFileName().toString().startsWith(BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX);
            final var isIndexBlob = blobToDamage.equals(repositoryDataBlob);
            if (isDataBlob || isIndexBlob || randomBoolean()) {
                logger.info("--> deleting {}", blobToDamage);
                Files.move(blobToDamage, tempDir.resolve("tmp"));
            } else {
                logger.info("--> corrupting {}", blobToDamage);
                Files.copy(blobToDamage, tempDir.resolve("tmp"));
                CorruptionUtils.corruptFile(random(), blobToDamage);
            }
            try {
                final var verificationResponse = PlainActionFuture.get(repository::verifyMetadataIntegrity, 30, TimeUnit.SECONDS);
                assertThat(verificationResponse, not(empty()));
                if (isDataBlob) {
                    assertThat(
                        verificationResponse.stream().map(Strings::toString).collect(Collectors.joining("\n")),
                        allOf(containsString(blobToDamage.getFileName().toString()), containsString("missing blob"))
                    );
                }
            } finally {
                Files.deleteIfExists(blobToDamage);
                Files.move(tempDir.resolve("tmp"), blobToDamage);
            }

            assertThat(PlainActionFuture.get(repository::verifyMetadataIntegrity, 30, TimeUnit.SECONDS), empty());
        }
    }
}
