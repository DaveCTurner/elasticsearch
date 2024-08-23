/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.blobstore.testkit.SnapshotRepositoryTestKit;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

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

    public void testSuccess() throws Exception {
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

        final var masterTransportService = MockTransportService.getInstance(internalCluster().getMasterName());

        // use non-master node to coordinate the request so that we can capture chunks being sent back
        final var coordNodeName = randomValueOtherThan(internalCluster().getMasterName(), () -> internalCluster().getRandomNodeName());
        final var coordNodeTransportService = MockTransportService.getInstance(coordNodeName);

        final SubscribableListener<RepositoryVerifyIntegrityTask.Status> snapshotsCompleteStatusListener = new SubscribableListener<>();

        coordNodeTransportService.<TransportRepositoryVerifyIntegrityResponseChunkAction.Request>addRequestHandlingBehavior(
            TransportRepositoryVerifyIntegrityResponseChunkAction.ACTION_NAME,
            (handler, request, channel, task) -> {
                final SubscribableListener<Void> unblockChunkHandlingListener = switch (request.chunkContents().type()) {
                    case START_RESPONSE -> {
                        final var status = asInstanceOf(
                            RepositoryVerifyIntegrityTask.Status.class,
                            masterTransportService.getTaskManager().getTask(task.getParentTaskId().getId()).getStatus()
                        );
                        assertEquals(repositoryName, status.repositoryName());
                        assertEquals(snapshotNames.size(), status.snapshotCount());
                        assertEquals(0L, status.snapshotsVerified());
                        assertEquals(indexNames.size(), status.indexCount());
                        assertEquals(0L, status.indicesVerified());
                        assertEquals(indexNames.size() * snapshotNames.size(), status.indexSnapshotCount());
                        assertEquals(0L, status.indexSnapshotsVerified());
                        assertEquals(0L, status.blobsVerified());
                        assertEquals(0L, status.blobBytesVerified());
                        yield SubscribableListener.newSucceeded(null);
                    }
                    case INDEX_RESTORABILITY -> {
                        // several of these chunks are racing at once; we want to verify the task status before processing any of them,
                        // so use SubscribableListener to pick out the first status
                        snapshotsCompleteStatusListener.onResponse(
                            asInstanceOf(
                                RepositoryVerifyIntegrityTask.Status.class,
                                masterTransportService.getTaskManager().getTask(task.getParentTaskId().getId()).getStatus()
                            )
                        );
                        yield snapshotsCompleteStatusListener.andThenAccept(status -> {
                            assertEquals(repositoryName, status.repositoryName());
                            assertEquals(snapshotNames.size(), status.snapshotCount());
                            assertEquals(snapshotNames.size(), status.snapshotsVerified());
                            assertEquals(indexNames.size(), status.indexCount());
                            assertEquals(0L, status.indicesVerified());
                        });
                    }
                    case SNAPSHOT_INFO -> SubscribableListener.newSucceeded(null);
                    case ANOMALY -> fail(null, "should not see anomalies");
                };

                unblockChunkHandlingListener.addListener(
                    ActionTestUtils.assertNoFailureListener(ignored -> handler.messageReceived(request, channel, task))
                );
            }
        );

        masterTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportRepositoryVerifyIntegrityResponseChunkAction.ACTION_NAME)) {
                logger.info(
                    "--> captured response chunk [{}]",
                    asInstanceOf(TransportRepositoryVerifyIntegrityResponseChunkAction.Request.class, request).chunkContents()
                );
            }
            connection.sendRequest(requestId, action, request, options);
        });

        try (var client = createRestClient(coordNodeName)) {
            final var request = new Request("POST", "/_snapshot/" + repositoryName + "/_verify_integrity");
            request.addParameter("human", null);
            request.addParameter("pretty", null);
            final var response = client.performRequest(request);
            assertEquals(200, response.getStatusLine().getStatusCode());
            final var responseObjectPath = ObjectPath.createFromResponse(response);
            final var logEntryCount = responseObjectPath.evaluateArraySize("log");
            final var seenSnapshotNames = new HashSet<String>();
            final var seenIndexNames = new HashSet<String>();
            for (int i = 0; i < logEntryCount; i++) {
                final String maybeSnapshotName = responseObjectPath.evaluate("log." + i + ".snapshot.snapshot");
                if (maybeSnapshotName != null) {
                    assertTrue(seenSnapshotNames.add(maybeSnapshotName));
                } else {
                    final String indexName = responseObjectPath.evaluate("log." + i + ".index.name");
                    assertNotNull(indexName);
                    assertTrue(seenIndexNames.add(indexName));
                    assertEquals(snapshotNames.size(), (int) responseObjectPath.evaluate("log." + i + ".snapshots.total_count"));
                    assertEquals(snapshotNames.size(), (int) responseObjectPath.evaluate("log." + i + ".snapshots.restorable_count"));
                }
            }
            assertEquals(Set.copyOf(snapshotNames), seenSnapshotNames);
            assertEquals(Set.copyOf(indexNames), seenIndexNames);

            assertEquals(0, (int) responseObjectPath.evaluate("results.total_anomalies"));
            assertEquals("pass", responseObjectPath.evaluate("results.result"));
        }
    }
}
