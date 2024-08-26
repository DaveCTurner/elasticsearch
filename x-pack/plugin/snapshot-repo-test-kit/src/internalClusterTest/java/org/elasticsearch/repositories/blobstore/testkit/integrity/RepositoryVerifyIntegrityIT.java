/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshotsIntegritySuppressor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.blobstore.BlobStoreCorruptionUtils;
import org.elasticsearch.repositories.blobstore.RepositoryFileType;
import org.elasticsearch.repositories.blobstore.testkit.SnapshotRepositoryTestKit;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;

import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

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
        final var testContext = createTestContext();
        final var request = testContext.getVerifyIntegrityRequest();
        if (randomBoolean()) {
            request.addParameter("verify_blob_contents", null);
        }
        final var response = getRestClient().performRequest(request);
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
                assertEquals(testContext.snapshotNames().size(), (int) responseObjectPath.evaluate("log." + i + ".snapshots.total_count"));
                assertEquals(
                    testContext.snapshotNames().size(),
                    (int) responseObjectPath.evaluate("log." + i + ".snapshots.restorable_count")
                );
            }
        }
        assertEquals(Set.copyOf(testContext.snapshotNames()), seenSnapshotNames);
        assertEquals(Set.copyOf(testContext.indexNames()), seenIndexNames);

        assertEquals(0, (int) responseObjectPath.evaluate("results.total_anomalies"));
        assertEquals("pass", responseObjectPath.evaluate("results.result"));
    }

    public void testTaskStatus() throws Exception {
        final var testContext = createTestContext();

        // use non-master node to coordinate the request so that we can capture chunks being sent back
        final var coordNodeName = getCoordinatingNodeName();
        final var coordNodeTransportService = MockTransportService.getInstance(coordNodeName);
        final var masterTaskManager = MockTransportService.getInstance(internalCluster().getMasterName()).getTaskManager();

        final SubscribableListener<RepositoryVerifyIntegrityTask.Status> snapshotsCompleteStatusListener = new SubscribableListener<>();
        final AtomicInteger chunksSeenCounter = new AtomicInteger();

        coordNodeTransportService.<TransportRepositoryVerifyIntegrityResponseChunkAction.Request>addRequestHandlingBehavior(
            TransportRepositoryVerifyIntegrityResponseChunkAction.ACTION_NAME,
            (handler, request, channel, task) -> {
                final SubscribableListener<Void> unblockChunkHandlingListener = switch (request.chunkContents().type()) {
                    case START_RESPONSE -> {
                        final var status = asInstanceOf(
                            RepositoryVerifyIntegrityTask.Status.class,
                            masterTaskManager.getTask(task.getParentTaskId().getId()).getStatus()
                        );
                        assertEquals(testContext.repositoryName(), status.repositoryName());
                        assertEquals(testContext.snapshotNames().size(), status.snapshotCount());
                        assertEquals(0L, status.snapshotsVerified());
                        assertEquals(testContext.indexNames().size(), status.indexCount());
                        assertEquals(0L, status.indicesVerified());
                        assertEquals(testContext.indexNames().size() * testContext.snapshotNames().size(), status.indexSnapshotCount());
                        assertEquals(0L, status.indexSnapshotsVerified());
                        assertEquals(0L, status.blobsVerified());
                        assertEquals(0L, status.blobBytesVerified());
                        yield SubscribableListener.newSucceeded(null);
                    }
                    case INDEX_RESTORABILITY -> {
                        // several of these chunks might arrive concurrently; we want to verify the task status before processing any of
                        // them, so use SubscribableListener to pick out the first status
                        snapshotsCompleteStatusListener.onResponse(
                            asInstanceOf(
                                RepositoryVerifyIntegrityTask.Status.class,
                                masterTaskManager.getTask(task.getParentTaskId().getId()).getStatus()
                            )
                        );
                        yield snapshotsCompleteStatusListener.andThenAccept(status -> {
                            assertEquals(testContext.repositoryName(), status.repositoryName());
                            assertEquals(testContext.snapshotNames().size(), status.snapshotCount());
                            assertEquals(testContext.snapshotNames().size(), status.snapshotsVerified());
                            assertEquals(testContext.indexNames().size(), status.indexCount());
                            assertEquals(0L, status.indicesVerified());
                        });
                    }
                    case SNAPSHOT_INFO -> SubscribableListener.newSucceeded(null);
                    case ANOMALY -> fail(null, "should not see anomalies");
                };

                unblockChunkHandlingListener.addListener(ActionTestUtils.assertNoFailureListener(ignored -> {
                    chunksSeenCounter.incrementAndGet();
                    handler.messageReceived(request, channel, task);
                }));
            }
        );

        try (var client = createRestClient(coordNodeName)) {
            final var response = client.performRequest(testContext.getVerifyIntegrityRequest());
            assertEquals(1 + testContext.indexNames().size() + testContext.snapshotNames().size(), chunksSeenCounter.get());
            assertEquals(200, response.getStatusLine().getStatusCode());
            final var responseObjectPath = ObjectPath.createFromResponse(response);
            assertEquals(0, (int) responseObjectPath.evaluate("results.total_anomalies"));
            assertEquals("pass", responseObjectPath.evaluate("results.result"));
        } finally {
            coordNodeTransportService.clearAllRules();
        }
    }

    public void testCorruption() throws Exception {
        final var testContext = createTestContext();

        final Response response;
        final Path corruptedFile;
        final RepositoryFileType corruptedFileType;
        try (var ignored = new BlobStoreIndexShardSnapshotsIntegritySuppressor()) {
            corruptedFile = BlobStoreCorruptionUtils.corruptRandomFile(testContext.repositoryRootPath());
            corruptedFileType = RepositoryFileType.getRepositoryFileType(testContext.repositoryRootPath(), corruptedFile);
            logger.info("--> corrupted file: {}", corruptedFile);
            logger.info("--> corrupted file type: {}", corruptedFileType);

            final var request = testContext.getVerifyIntegrityRequest();
            if (corruptedFileType == RepositoryFileType.SHARD_DATA || randomBoolean()) {
                request.addParameter("verify_blob_contents", null);
            }
            response = getRestClient().performRequest(request);
        } finally {
            assertAcked(
                client().admin().cluster().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, testContext.repositoryName())
            );
        }
        assertEquals(200, response.getStatusLine().getStatusCode());
        final var responseObjectPath = ObjectPath.createFromResponse(response);
        logger.info("--> op: {}", responseObjectPath);
        final var logEntryCount = responseObjectPath.evaluateArraySize("log");
        final var anomalies = new HashSet<String>();
        final var seenIndexNames = new HashSet<String>();
        int fullyRestorableIndices = 0;
        for (int i = 0; i < logEntryCount; i++) {
            final String maybeAnomaly = responseObjectPath.evaluate("log." + i + ".anomaly");
            if (maybeAnomaly != null) {
                anomalies.add(maybeAnomaly);
            } else {
                final String indexName = responseObjectPath.evaluate("log." + i + ".index.name");
                if (indexName != null) {
                    assertTrue(seenIndexNames.add(indexName));
                    assertThat(testContext.indexNames(), hasItem(indexName));
                    final int totalSnapshots = responseObjectPath.evaluate("log." + i + ".snapshots.total_count");
                    final int restorableSnapshots = responseObjectPath.evaluate("log." + i + ".snapshots.restorable_count");
                    if (totalSnapshots == restorableSnapshots) {
                        fullyRestorableIndices += 1;
                    }
                }
            }
        }

        assertThat(
            fullyRestorableIndices,
            corruptedFileType == RepositoryFileType.SHARD_GENERATION || corruptedFileType.equals(RepositoryFileType.GLOBAL_METADATA)
                ? equalTo(testContext.indexNames().size())
                : lessThan(testContext.indexNames().size())
        );
        assertThat(anomalies, not(empty()));
        assertThat(responseObjectPath.evaluate("results.total_anomalies"), greaterThanOrEqualTo(anomalies.size()));
        assertEquals("fail", responseObjectPath.evaluate("results.result"));

        logger.info("--> anomalies: {}", anomalies);

        // remove permitted/expected anomalies to verify that no unexpected ones were seen
        switch (corruptedFileType) {
            case SNAPSHOT_INFO -> anomalies.remove("failed to load snapshot info");
            case GLOBAL_METADATA -> anomalies.remove("failed to load global metadata");
            case INDEX_METADATA -> anomalies.remove("failed to load index metadata");
            case SHARD_GENERATION -> anomalies.remove("failed to load shard generation");
            case SHARD_SNAPSHOT_INFO -> anomalies.remove("failed to load shard snapshot");
            case SHARD_DATA -> {
                anomalies.remove("missing blob");
                anomalies.remove("mismatched blob length");
                anomalies.remove("corrupt data blob");
            }
        }
        assertThat(anomalies, empty());
    }

    public void testTransportException() throws Exception {
        final var testContext = createTestContext();

        // use non-master node to coordinate the request so that we can capture chunks being sent back
        final var coordNodeName = getCoordinatingNodeName();
        final var coordNodeTransportService = MockTransportService.getInstance(coordNodeName);
        final var masterTransportService = MockTransportService.getInstance(internalCluster().getMasterName());

        final var messageCount = 2 // request & response
            * (1 // forward to master
                + 1 // start response
                + testContext.indexNames().size() + testContext.snapshotNames().size());
        final var failureStep = between(1, messageCount);

        final var failTransportMessageBehaviour = new StubbableTransport.RequestHandlingBehavior<>() {
            final AtomicInteger currentStep = new AtomicInteger();

            @Override
            public void messageReceived(
                TransportRequestHandler<TransportRequest> handler,
                TransportRequest request,
                TransportChannel channel,
                Task task
            ) throws Exception {
                if (currentStep.incrementAndGet() == failureStep) {
                    throw new ElasticsearchException("simulated");
                } else {
                    handler.messageReceived(request, new TransportChannel() {
                        @Override
                        public String getProfileName() {
                            return "test";
                        }

                        @Override
                        public void sendResponse(TransportResponse response) {
                            if (currentStep.incrementAndGet() == failureStep) {
                                channel.sendResponse(new ElasticsearchException("simulated"));
                            } else {
                                channel.sendResponse(response);
                            }
                        }

                        @Override
                        public void sendResponse(Exception exception) {
                            if (currentStep.incrementAndGet() == failureStep) {
                                throw new AssertionError("shouldn't have failed yet");
                            } else {
                                channel.sendResponse(exception);
                            }
                        }
                    }, task);
                }
            }
        };

        masterTransportService.addRequestHandlingBehavior(
            TransportRepositoryVerifyIntegrityMasterNodeAction.ACTION_NAME,
            failTransportMessageBehaviour
        );

        coordNodeTransportService.addRequestHandlingBehavior(
            TransportRepositoryVerifyIntegrityResponseChunkAction.ACTION_NAME,
            failTransportMessageBehaviour
        );

        final var request = testContext.getVerifyIntegrityRequest();
        if (failureStep <= 2) {
            request.addParameter("ignore", "500");
        }
        final Response response;
        try (var restClient = createRestClient(coordNodeName)) {
            response = restClient.performRequest(request);
        }
        final var responseObjectPath = ObjectPath.createFromResponse(response);
        if (failureStep <= 2) {
            assertEquals(500, response.getStatusLine().getStatusCode());
            assertNotNull(responseObjectPath.evaluate("error"));
            assertEquals(500, (int) responseObjectPath.evaluate("status"));
        } else {
            assertEquals(200, response.getStatusLine().getStatusCode());
            logger.info("--> op: {}", responseObjectPath);
            assertNotNull(responseObjectPath.evaluate("log"));
            assertNotNull(responseObjectPath.evaluate("exception"));
        }

        assertNull(responseObjectPath.evaluate("results"));
    }

    private record TestContext(String repositoryName, Path repositoryRootPath, List<String> indexNames, List<String> snapshotNames) {
        Request getVerifyIntegrityRequest() {
            final var request = new Request("POST", "/_snapshot/" + repositoryName + "/_verify_integrity");
            if (randomBoolean()) {
                request.addParameter("human", null);
            }
            if (randomBoolean()) {
                request.addParameter("pretty", null);
            }
            return request;
        }
    }

    private TestContext createTestContext() throws Exception {
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

        return new TestContext(repositoryName, repositoryRootPath, indexNames, snapshotNames);
    }

    private static String getCoordinatingNodeName() {
        if (internalCluster().size() == 1) {
            internalCluster().startNode();
        }
        return randomValueOtherThan(internalCluster().getMasterName(), () -> internalCluster().getRandomNodeName());
    }
}
