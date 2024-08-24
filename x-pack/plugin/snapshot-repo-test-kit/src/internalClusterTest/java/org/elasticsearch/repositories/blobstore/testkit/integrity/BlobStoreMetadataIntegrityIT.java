/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshotsIntegritySuppressor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.testkit.SnapshotRepositoryTestKit;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.CorruptionUtils;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class BlobStoreMetadataIntegrityIT extends AbstractSnapshotIntegTestCase {

    private static final String REPOSITORY_NAME = "test-repo";

    private Releasable integrityCheckSuppressor;

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable HTTP
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(
            CollectionUtils.appendToCopy(super.nodePlugins(), LocalStateCompositeXPackPlugin.class),
            SnapshotRepositoryTestKit.class
        );
    }

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

    @TestLogging(reason = "testing", value = "org.elasticsearch.repositories.blobstore.testkit.integrity:DEBUG")
    public void testIntegrityCheck() throws Exception {
        final var repoPath = randomRepoPath();
        createRepository(
            REPOSITORY_NAME,
            "mock",
            Settings.builder().put(BlobStoreRepository.SUPPORT_URL_REPO.getKey(), false).put("location", repoPath)
        );
        final MockRepository repository = asInstanceOf(
            MockRepository.class,
            internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository(REPOSITORY_NAME)
        );

        final var indexCount = between(1, 3);
        for (int i = 0; i < indexCount; i++) {
            createIndexWithRandomDocs("test-index-" + i, between(1, 1000));
        }

        final var snapshotCount = between(2, 4);
        for (int snapshotIndex = 0; snapshotIndex < snapshotCount; snapshotIndex++) {
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
            final var snapshotInfo = clusterAdmin().prepareCreateSnapshot(
                TEST_REQUEST_TIMEOUT,
                REPOSITORY_NAME,
                "test-snapshot-" + snapshotIndex
            ).setIncludeGlobalState(randomBoolean()).setWaitForCompletion(true).get().getSnapshotInfo();
            assertThat(snapshotInfo.successfulShards(), is(snapshotInfo.totalShards()));
            assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));
        }

        repository.setBlockOnReadIndexMeta();

        final var tasksFuture = new PlainActionFuture<List<TaskInfo>>();
        repository.threadPool().generic().execute(() -> {
            try {
                assertBusy(() -> assertTrue(repository.blocked()));
            } catch (Exception e) {
                throw new AssertionError(e);
            }

            ActionListener.completeWith(
                tasksFuture,
                () -> client().admin()
                    .cluster()
                    .prepareListTasks()
                    .setDetailed(true)
                    .get()
                    .getTasks()
                    .stream()
                    .filter(t -> t.action().equals(TransportRepositoryVerifyIntegrityMasterNodeAction.ACTION_NAME) && t.status() != null)
                    .toList()
            );

            repository.unblock();
        });

        verifyAndAssertSuccessful(indexCount);

        final var tasks = safeGet(tasksFuture);
        assertThat(tasks, not(empty()));
        for (TaskInfo task : tasks) {
            final var status = asInstanceOf(RepositoryVerifyIntegrityTask.Status.class, task.status());
            assertEquals(REPOSITORY_NAME, status.repositoryName());
            assertThat(status.repositoryGeneration(), greaterThan(0L));
            assertEquals(snapshotCount, status.snapshotCount());
            assertEquals(snapshotCount, status.snapshotsVerified());
            assertEquals(indexCount, status.indexCount());
            assertEquals(0, status.indicesVerified());
            assertThat(status.indexSnapshotCount(), greaterThanOrEqualTo((long) indexCount));
            assertEquals(0, status.indexSnapshotsVerified());
            assertEquals(0, status.blobsVerified());
            assertEquals(0, status.blobBytesVerified());
        }

        final var tempDir = createTempDir();

        final var repositoryData = safeAwait(
            SubscribableListener.<RepositoryData>newForked(l -> repository.getRepositoryData(EsExecutors.DIRECT_EXECUTOR_SERVICE, l))
        );
        final var repositoryDataBlob = repoPath.resolve("index-" + repositoryData.getGenId());

        final List<Path> blobs;
        try (var paths = Files.walk(repoPath)) {
            blobs = paths.filter(path -> Files.isRegularFile(path) && path.equals(repositoryDataBlob) == false).sorted().toList();
        }

        for (int i = 0; i < 20; i++) {
            final var blobToDamage = randomFrom(blobs);
            final var isDataBlob = blobToDamage.getFileName().toString().startsWith(BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX);
            final var truncate = randomBoolean();
            final var corrupt = randomBoolean();
            if (truncate) {
                logger.info("--> truncating {}", blobToDamage);
                Files.copy(blobToDamage, tempDir.resolve("tmp"));
                Files.write(blobToDamage, new byte[0]);
            } else if (corrupt) {
                logger.info("--> corrupting {}", blobToDamage);
                Files.copy(blobToDamage, tempDir.resolve("tmp"));
                CorruptionUtils.corruptFile(random(), blobToDamage);
            } else {
                logger.info("--> deleting {}", blobToDamage);
                Files.move(blobToDamage, tempDir.resolve("tmp"));
            }
            try {
                // TODO include some cancellation tests

                verifyAndGetAnomalies(indexCount, repoPath.relativize(blobToDamage), truncate, corrupt);

                //
                // final var isCancelled = new AtomicBoolean();
                //
                // final var verificationResponse = PlainActionFuture.get(
                // (PlainActionFuture<Void> listener) -> repository.verifyMetadataIntegrity(
                // client(),
                // () -> new RecyclerBytesStreamOutput(NON_RECYCLING_INSTANCE),
                // request,
                // listener,
                // () -> {
                // if (rarely() && rarely()) {
                // isCancelled.set(true);
                // return true;
                // }
                // return isCancelled.get();
                // }
                // ),
                // 30,
                // TimeUnit.SECONDS
                // );
                // for (SearchHit hit : client().prepareSearch("metadata_verification_results").setSize(10000).get().getHits().getHits()) {
                // logger.info("--> {}", Strings.toString(hit));
                // }
                // assertThat(verificationResponse, not(nullValue()));
                // final var responseString = verificationResponse.stream().map(Throwable::getMessage).collect(Collectors.joining("\n"));
                // if (isCancelled.get()) {
                // assertThat(responseString, containsString("verification task cancelled before completion"));
                // }
                // if (isDataBlob && isCancelled.get() == false) {
                // assertThat(
                // responseString,
                // allOf(containsString(blobToDamage.getFileName().toString()), containsString("missing blob"))
                // );
                // }
            } finally {
                Files.deleteIfExists(blobToDamage);
                Files.move(tempDir.resolve("tmp"), blobToDamage);
            }

            verifyAndAssertSuccessful(indexCount);
        }
    }

    private void verifyAndAssertSuccessful(int indexCount) throws IOException {
        final var restResponse = getRestClient().performRequest(
            new Request("POST", "/_snapshot/" + REPOSITORY_NAME + "/_verify_integrity")
        );

        logger.info(
            "--> expected-success response {}",
            new String(restResponse.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8)
        );
    }

    private void verifyAndGetAnomalies(long indexCount, Path damagedBlob, boolean truncate, boolean corrupt) throws IOException {
        final var damagedFileName = damagedBlob.getFileName().toString();
        final var isDataBlob = damagedFileName.startsWith(BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX);

        final var restResponse = getRestClient().performRequest(
            new Request("POST", "/_snapshot/" + REPOSITORY_NAME + "/_verify_integrity")
        );

        logger.info(
            "--> expected-failure response {}",
            new String(restResponse.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8)
        );
        //
        // final VerifyRepositoryIntegrityAction.Response response = safeAwait(
        // listener -> client().execute(
        // VerifyRepositoryIntegrityAction.INSTANCE,
        // new VerifyRepositoryIntegrityAction.Request(
        // TEST_REQUEST_TIMEOUT,
        // REPOSITORY_NAME,
        // "",
        // 0,
        // 0,
        // 0,
        // 0,
        // 0,
        // (isDataBlob && truncate == false && corrupt) || randomBoolean(),
        // ByteSizeValue.ofMb(10),
        // true
        // ),
        // listener
        // )
        // );
        // assertThat(
        // client().prepareSearch(response.getResultsIndex())
        // .setSize(10000)
        // .setQuery(new ExistsQueryBuilder("anomaly"))
        // .get()
        // .getHits()
        // .getTotalHits().value,
        // greaterThan(0L)
        // );
        // assertEquals(
        // indexCount,
        // client().prepareSearch(response.getResultsIndex())
        // .setSize(0)
        // .setQuery(new ExistsQueryBuilder("restorability"))
        // .setTrackTotalHits(true)
        // .get()
        // .getHits()
        // .getTotalHits().value
        // );
        // assertThat(
        // client().prepareSearch(response.getResultsIndex())
        // .setSize(0)
        // .setQuery(new TermQueryBuilder("restorability", "full"))
        // .setTrackTotalHits(true)
        // .get()
        // .getHits()
        // .getTotalHits().value,
        // damagedFileName.startsWith(BlobStoreRepository.SNAPSHOT_PREFIX)
        // || isDataBlob
        // || (damagedFileName.startsWith(BlobStoreRepository.METADATA_PREFIX) && damagedBlob.startsWith("indices"))
        // ? lessThan(indexCount)
        // : equalTo(indexCount)
        // );
        // assertThat(
        // (int) client().prepareSearch(response.getResultsIndex())
        // .setSize(1)
        // .setQuery(new TermQueryBuilder("completed", true))
        // .get()
        // .getHits()
        // .getHits()[0].getSourceAsMap()
        // .get("total_anomalies"),
        // greaterThan(0)
        // );
        // if (damagedBlob.toString().startsWith(BlobStoreRepository.SNAPSHOT_PREFIX)) {
        // assertAnomaly(response.getResultsIndex(), MetadataVerifier.Anomaly.FAILED_TO_LOAD_SNAPSHOT_INFO);
        // } else if (damagedFileName.startsWith(BlobStoreRepository.SNAPSHOT_PREFIX)) {
        // assertAnomaly(response.getResultsIndex(), MetadataVerifier.Anomaly.FAILED_TO_LOAD_SHARD_SNAPSHOT);
        // } else if (damagedBlob.toString().startsWith(BlobStoreRepository.METADATA_PREFIX)) {
        // assertAnomaly(response.getResultsIndex(), MetadataVerifier.Anomaly.FAILED_TO_LOAD_GLOBAL_METADATA);
        // } else if (damagedFileName.startsWith(BlobStoreRepository.METADATA_PREFIX)) {
        // assertAnomaly(response.getResultsIndex(), MetadataVerifier.Anomaly.FAILED_TO_LOAD_INDEX_METADATA);
        // } else if (damagedFileName.startsWith(BlobStoreRepository.INDEX_FILE_PREFIX)) {
        // assertAnomaly(response.getResultsIndex(), MetadataVerifier.Anomaly.FAILED_TO_LOAD_SHARD_GENERATION);
        // } else if (isDataBlob) {
        // assertAnomaly(
        // response.getResultsIndex(),
        // truncate ? MetadataVerifier.Anomaly.MISMATCHED_BLOB_LENGTH
        // : corrupt ? MetadataVerifier.Anomaly.CORRUPT_DATA_BLOB
        // : MetadataVerifier.Anomaly.MISSING_BLOB
        // );
        // }
        // assertAcked(client().admin().indices().prepareDelete(response.getResultsIndex()));
    }
    //
    // private void assertAnomaly(String resultsIndex, MetadataVerifier.Anomaly anomaly) {
    // assertThat(
    // client().prepareSearch(resultsIndex)
    // .setSize(0)
    // .setQuery(new TermQueryBuilder("anomaly", anomaly.toString()))
    // .setTrackTotalHits(true)
    // .get()
    // .getHits()
    // .getTotalHits().value,
    // greaterThan(0L)
    // );
    // }
}
