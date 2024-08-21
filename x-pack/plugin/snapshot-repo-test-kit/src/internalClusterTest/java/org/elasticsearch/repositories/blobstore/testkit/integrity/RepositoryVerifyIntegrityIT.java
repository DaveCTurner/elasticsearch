/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.blobstore.testkit.SnapshotRepositoryTestKit;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

public class RepositoryVerifyIntegrityIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable HTTP
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(
            super.nodePlugins(),
            SnapshotRepositoryTestKit.class,
            MockTransportService.TestPlugin.class
        );
    }

    public void testResponse() throws IOException {
        if (internalCluster().size() == 1) {
            internalCluster().startNode();
        }

        final var repositoryName = randomIdentifier();
        final var repositoryRootPath = randomRepoPath();

        createRepository(repositoryName, FsRepository.TYPE, repositoryRootPath);

        final var indexNames = randomList(1, 3, ESTestCase::randomIdentifier);
        for (var indexName : indexNames) {
            createIndexWithRandomDocs(indexName, between(1, 100));
            flushAndRefresh(indexName);
        }

        final var snapshotNames = randomList(1, 3, ESTestCase::randomIdentifier);
        for (var snapshotName : snapshotNames) {
            createSnapshot(repositoryName, snapshotName, indexNames);
        }

        final var masterName = internalCluster().getMasterName();
        final var masterTransportService = MockTransportService.getInstance(masterName);

        masterTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportRepositoryVerifyIntegrityResponseChunkAction.SNAPSHOT_CHUNK_ACTION_NAME)) {
                logger.info(
                    "--> captured response chunk [{}]",
                    asInstanceOf(TransportRepositoryVerifyIntegrityResponseChunkAction.Request.class, request).chunkContents()
                );
            }
            connection.sendRequest(requestId, action, request, options);
        });

        final var nonMasterName = randomValueOtherThan(masterName, () -> internalCluster().getRandomNodeName());
        try (var client = createRestClient(nonMasterName)) {
            final var request = new Request("POST", "/_snapshot/" + repositoryName + "/_verify_integrity");
            final var response = client.performRequest(request);
            logger.info("--> response body: {}", new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8));
        }
    }
}
