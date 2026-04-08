/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import fixture.azure.AzureHttpHandler;
import fixture.azure.MockAzureBlobStore;

import org.elasticsearch.common.blobstore.BlobStoreActionStats;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.http.ResponseInjectingHttpHandler;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.junit.annotations.TestIssueLogging;
import org.junit.Before;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.elasticsearch.repositories.azure.AzureBlobStore.Operation.BLOB_BATCH;
import static org.elasticsearch.repositories.azure.AzureBlobStore.Operation.LIST_BLOBS;
import static org.elasticsearch.repositories.azure.AzureBlobStore.Operation.PUT_BLOB;

public class AzureBlobContainerStatsTests extends AbstractAzureServerTestCase {

    private final Queue<ResponseInjectingHttpHandler.RequestHandler> requestHandlers = new ConcurrentLinkedQueue<>();

    @SuppressForbidden(reason = "use a http server")
    @Before
    public void configureAzureHandler() {
        httpServer.createContext(
            "/",
            new ResponseInjectingHttpHandler(
                requestHandlers,
                new AzureHttpHandler(ACCOUNT, CONTAINER, null, MockAzureBlobStore.LeaseExpiryPredicate.NEVER_EXPIRE)
            )
        );
    }

    /**
     * Extra logging for <a href="https://github.com/elastic/elasticsearch/issues/145281">#145281</a>.
     * <p>
     * Test output uses {@code log4j2-test.properties}, which abbreviates logger names ({@code %c{1.}}), so
     * {@code com.azure...} appears as {@code c.a....} in {@code TEST-*.xml} (for example
     * {@code c.a.c.h.n.i.NettyUtility} is the Azure SDK Netty helper). Elasticsearch and fixture loggers
     * are shortened the same way ({@code o.e.r.a.*}, {@code f.a.*}). The Azure SDK emits relatively few
     * {@code TRACE} lines; {@code INFO}/{@code DEBUG} under {@code c.a.*} is expected. Reactor Netty client
     * diagnostics use {@code reactor.netty.http.client} (often abbreviated {@code r.n.h.c.*}).
     */
    @TestIssueLogging(
        value = "org.elasticsearch.repositories.azure:TRACE,com.azure:TRACE,fixture.azure:TRACE,org.elasticsearch.http:TRACE,"
            + "reactor.netty.http.client:DEBUG",
        issueUrl = "https://github.com/elastic/elasticsearch/issues/145281"
    )
    public void testRetriesAndOperationsAreTrackedSeparately() throws IOException {
        serverlessMode = true;
        final AzureBlobContainer blobContainer = asInstanceOf(AzureBlobContainer.class, createBlobContainer(between(1, 3)));
        final AzureBlobStore blobStore = blobContainer.getBlobStore();
        final OperationPurpose purpose = randomFrom(OperationPurpose.values());

        // Just a sample of the easy operations to test
        final List<AzureBlobStore.Operation> supportedOperations = Arrays.asList(PUT_BLOB, LIST_BLOBS, BLOB_BATCH);
        final Map<AzureBlobStore.Operation, BlobStoreActionStats> expectedActionStats = new HashMap<>();

        final int iterations = randomIntBetween(10, 50);
        logger.info(
            "testRetriesAndOperationsAreTrackedSeparately: purpose=[{}], iterations=[{}], tests.seed=[{}]",
            purpose,
            iterations,
            System.getProperty("tests.seed")
        );
        for (int i = 0; i < iterations; i++) {
            final boolean triggerRetry = randomBoolean();
            if (triggerRetry) {
                requestHandlers.offer(new ResponseInjectingHttpHandler.FixedRequestHandler(RestStatus.TOO_MANY_REQUESTS));
            }
            final AzureBlobStore.Operation operation = randomFrom(supportedOperations);
            final long deltaRequests = triggerRetry ? 2L : 1L;
            final String key = statsKey(purpose, operation);
            switch (operation) {
                case PUT_BLOB -> blobStore.writeBlob(
                    purpose,
                    randomIdentifier(),
                    BytesReference.fromByteBuffer(ByteBuffer.wrap(randomBlobContent())),
                    false
                );
                case LIST_BLOBS -> blobStore.listBlobsByPrefix(purpose, randomIdentifier(), randomIdentifier());
                case BLOB_BATCH -> blobStore.deleteBlobs(
                    purpose,
                    List.of(randomIdentifier(), randomIdentifier(), randomIdentifier()).iterator()
                );
            }
            expectedActionStats.compute(operation, (op, existing) -> {
                BlobStoreActionStats currentStats = new BlobStoreActionStats(1, deltaRequests);
                if (existing != null) {
                    currentStats = existing.add(currentStats);
                }
                return currentStats;
            });
            logger.info(
                "step [{}]: triggerRetry=[{}], operation=[{}], statsKey=[{}], deltaOps=1, deltaRequests=[{}], "
                    + "cumulativeExpectedForOperation=[{}]",
                i,
                triggerRetry,
                operation,
                key,
                deltaRequests,
                expectedActionStats.get(operation)
            );
        }

        final Map<String, BlobStoreActionStats> stats = blobStore.stats();
        logger.info("expectedActionStats (by Operation enum): [{}]", expectedActionStats);
        logger.info("blobStore.stats() (stateless keys): [{}]", stats);
        expectedActionStats.forEach((operation, value) -> {
            String assertKey = statsKey(purpose, operation);
            assertEquals(assertKey, stats.get(assertKey), value);
        });
    }

    public void testOperationPurposeIsReflectedInBlobStoreStats() throws IOException {
        serverlessMode = true;
        AzureBlobContainer blobContainer = asInstanceOf(AzureBlobContainer.class, createBlobContainer(between(1, 3)));
        AzureBlobStore blobStore = blobContainer.getBlobStore();
        OperationPurpose purpose = randomFrom(OperationPurpose.values());

        String blobName = randomIdentifier();
        // PUT_BLOB
        blobStore.writeBlob(purpose, blobName, BytesReference.fromByteBuffer(ByteBuffer.wrap(randomBlobContent())), false);
        // LIST_BLOBS
        blobStore.listBlobsByPrefix(purpose, randomIdentifier(), randomIdentifier());
        // GET_BLOB_PROPERTIES
        blobStore.blobExists(purpose, blobName);
        // PUT_BLOCK & PUT_BLOCK_LIST
        byte[] blobContent = randomByteArrayOfLength((int) blobStore.getUploadBlockSize());
        blobStore.writeBlob(purpose, randomIdentifier(), false, os -> {
            os.write(blobContent);
            os.flush();
        });
        // BLOB_BATCH
        blobStore.deleteBlobs(purpose, List.of(randomIdentifier(), randomIdentifier(), randomIdentifier()).iterator());

        Map<String, BlobStoreActionStats> stats = blobStore.stats();
        String statsMapString = stats.toString();
        assertEquals(statsMapString, 1L, stats.get(statsKey(purpose, AzureBlobStore.Operation.PUT_BLOB)).operations());
        assertEquals(statsMapString, 1L, stats.get(statsKey(purpose, AzureBlobStore.Operation.LIST_BLOBS)).operations());
        assertEquals(statsMapString, 1L, stats.get(statsKey(purpose, AzureBlobStore.Operation.GET_BLOB_PROPERTIES)).operations());
        assertEquals(statsMapString, 1L, stats.get(statsKey(purpose, AzureBlobStore.Operation.PUT_BLOCK)).operations());
        assertEquals(statsMapString, 1L, stats.get(statsKey(purpose, AzureBlobStore.Operation.PUT_BLOCK_LIST)).operations());
        assertEquals(statsMapString, 1L, stats.get(statsKey(purpose, AzureBlobStore.Operation.BLOB_BATCH)).operations());
    }

    public void testOperationPurposeIsNotReflectedInBlobStoreStatsWhenNotServerless() throws IOException {
        serverlessMode = false;
        AzureBlobContainer blobContainer = asInstanceOf(AzureBlobContainer.class, createBlobContainer(between(1, 3)));
        AzureBlobStore blobStore = blobContainer.getBlobStore();

        int repeatTimes = randomIntBetween(1, 3);
        for (int i = 0; i < repeatTimes; i++) {
            OperationPurpose purpose = randomFrom(OperationPurpose.values());

            String blobName = randomIdentifier();
            // PUT_BLOB
            blobStore.writeBlob(purpose, blobName, BytesReference.fromByteBuffer(ByteBuffer.wrap(randomBlobContent())), false);
            // LIST_BLOBS
            blobStore.listBlobsByPrefix(purpose, randomIdentifier(), randomIdentifier());
            // GET_BLOB_PROPERTIES
            blobStore.blobExists(purpose, blobName);
            // PUT_BLOCK & PUT_BLOCK_LIST
            byte[] blobContent = randomByteArrayOfLength((int) blobStore.getUploadBlockSize());
            blobStore.writeBlob(purpose, randomIdentifier(), false, os -> {
                os.write(blobContent);
                os.flush();
            });
            // BLOB_BATCH
            blobStore.deleteBlobs(purpose, List.of(randomIdentifier(), randomIdentifier(), randomIdentifier()).iterator());
        }

        Map<String, BlobStoreActionStats> stats = blobStore.stats();
        String statsMapString = stats.toString();
        assertEquals(statsMapString, repeatTimes, stats.get(AzureBlobStore.Operation.PUT_BLOB.getKey()).operations());
        assertEquals(statsMapString, repeatTimes, stats.get(AzureBlobStore.Operation.LIST_BLOBS.getKey()).operations());
        assertEquals(statsMapString, repeatTimes, stats.get(AzureBlobStore.Operation.GET_BLOB_PROPERTIES.getKey()).operations());
        assertEquals(statsMapString, repeatTimes, stats.get(AzureBlobStore.Operation.PUT_BLOCK.getKey()).operations());
        assertEquals(statsMapString, repeatTimes, stats.get(AzureBlobStore.Operation.PUT_BLOCK_LIST.getKey()).operations());
        assertEquals(statsMapString, repeatTimes, stats.get(AzureBlobStore.Operation.BLOB_BATCH.getKey()).operations());
    }

    private static String statsKey(OperationPurpose purpose, AzureBlobStore.Operation operation) {
        return purpose.getKey() + "_" + operation.getKey();
    }
}
