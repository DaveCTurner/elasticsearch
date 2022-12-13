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
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class BlobStoreMetadataIntegrityIT extends AbstractSnapshotIntegTestCase {

    private static final String REPOSITORY_NAME = "test-repo";

    public void testIntegrityCheck() throws Exception {

        createRepository(REPOSITORY_NAME, "fs");
        logger.info(
            "--> repo created: {}",
            client().admin().cluster().prepareGetRepositories(REPOSITORY_NAME).get().repositories().get(0).settings()
        );

        final var indexCount = between(1, 2);
        for (int i = 0; i < indexCount; i++) {
            createIndexWithRandomDocs("test-index-" + i, between(1, 1000));
        }

        for (int snapshotIndex = 0; snapshotIndex < 3; snapshotIndex++) {
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

        final var future = new PlainActionFuture<Void>();
        internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class)
            .repository(REPOSITORY_NAME)
            .verifyMetadataIntegrity(future);
        future.get(30, TimeUnit.SECONDS);

        logger.info("--> pause");

    }

}
