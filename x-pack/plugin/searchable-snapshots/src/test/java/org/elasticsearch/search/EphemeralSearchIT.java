/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.search;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.ReadOnlyEngine;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.SearchableSnapshotDirectory;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.SNAPSHOT_CODEC;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.SNAPSHOT_NAME_FORMAT;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class EphemeralSearchIT extends ESIntegTestCase {

    public static class EphemeralStorePlugin extends Plugin implements IndexStorePlugin, EnginePlugin {

        private RepositoriesService repositoriesService;
        private ThreadPool threadPool;

        void setup(RepositoriesService repositoriesService) {
            this.repositoriesService = Objects.requireNonNull(repositoriesService);
        }

        @Override
        public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                                   ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                                   NamedXContentRegistry xContentRegistry, Environment environment,
                                                   NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
            this.threadPool = threadPool;
            return Collections.emptyList();
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            if (indexSettings.getSettings().hasValue(OperationRouting.EPHEMERAL_INDEX_REPOSITORY_SETTING.getKey())) {
                return Optional.of(config -> new ReadOnlyEngine(config, null, new TranslogStats(), false, Function.identity()));
            } else {
                return Optional.empty();
            }
        }

        @Override
        public Map<String, DirectoryFactory> getDirectoryFactories() {
            return Map.of("ephemeral", (indexSettings, shardPath) -> {
                final StepListener<RepositoryData> repositoryDataFuture = new StepListener<>();
                final PlainActionFuture<Directory> directoryFuture = new PlainActionFuture<>();
                final String repositoryName = OperationRouting.EPHEMERAL_INDEX_REPOSITORY_SETTING.get(indexSettings.getSettings());
                final BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(repositoryName);

                threadPool.generic().execute(() -> repository.getRepositoryData(repositoryDataFuture));

                repositoryDataFuture.whenComplete(repositoryData -> ActionListener.completeWith(directoryFuture, () -> {

                    final String snapshotName = OperationRouting.EPHEMERAL_INDEX_SNAPSHOT_SETTING.get(indexSettings.getSettings());

                    final SnapshotId snapshotId = repositoryData.getSnapshotIds().stream()
                        .filter(s -> s.getName().equals(snapshotName))
                        .findFirst()
                        .orElseThrow(() -> new ElasticsearchException(
                            "snapshot [" + snapshotName + "] not found in repository [" + repositoryName + "]"));

                    final IndexId indexId = repositoryData.getIndices().get(indexSettings.getIndex().getName());

                    final BlobContainer shardContainer = repository.blobStore().blobContainer(repository.basePath()
                        .add("indices").add(indexId.getId()).add(Integer.toString(shardPath.getShardId().id())));

                    final BlobStoreIndexShardSnapshot blobs = new ChecksumBlobStoreFormat<>(SNAPSHOT_CODEC, SNAPSHOT_NAME_FORMAT,
                        BlobStoreIndexShardSnapshot::fromXContent, NamedXContentRegistry.EMPTY, true)
                        .read(shardContainer, snapshotId.getUUID());

                    return new SearchableSnapshotDirectory(blobs,
                        (name, from, length, buffer, offset) -> {
                            try (InputStream stream = shardContainer.readBlob(name)) {
                                final long skipped = stream.skip(from);
                                assert skipped == from;
                                int read = stream.read(buffer, offset, length);
                                assert read == length;
                            }
                        });
                }), directoryFuture::onFailure);

                return directoryFuture.actionGet();
            });
        }

    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(EphemeralStorePlugin.class);
    }

    @TestLogging(reason = "nocommit", value = "org.elasticsearch.action.search:TRACE")
    public void testSearchEphemeralShard() throws InterruptedException {

        for (String nodeName : internalCluster().getNodeNames()) {
            final PluginsService pluginsService = internalCluster().getInstance(PluginsService.class, nodeName);
            for (EphemeralStorePlugin ephemeralStorePlugin : pluginsService.filterPlugins(EphemeralStorePlugin.class)) {
                ephemeralStorePlugin.setup(internalCluster().getInstance(RepositoriesService.class, nodeName));
            }
        }

        final String repoName = randomAlphaOfLength(10);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        final Path repo = randomRepoPath();
        assertAcked(client().admin().cluster().preparePutRepository(repoName)
            .setType("fs")
            .setSettings(Settings.builder()
                .put("location", repo)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        createIndex(indexName);
        indexRandom(true, client().prepareIndex(indexName).setSource("foo", "bar"));
        flushAndRefresh(indexName);

        final TotalHits originalTotalHits = internalCluster().client().prepareSearch(indexName).get().getHits().getTotalHits();

        CreateSnapshotResponse createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true).get();
        final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));

        final IndexMetaData indexMetaData = client().admin().cluster().prepareState().get().getState().metaData().index(indexName);

        assertAcked(client().admin().indices().prepareDelete(indexName));

        final AllocationService allocationService = internalCluster().getInstance(AllocationService.class, internalCluster().getMasterName());
        final ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());

        final CountDownLatch indexCreatedLatch = new CountDownLatch(1);
        clusterService.submitStateUpdateTask("test",
            new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return allocationService.reroute(ClusterState.builder(currentState).metaData(MetaData.builder(currentState.metaData())
                        .put(IndexMetaData.builder(indexMetaData).settings(Settings.builder()
                            .put(indexMetaData.getSettings())
                            .put(OperationRouting.EPHEMERAL_INDEX_REPOSITORY_SETTING.getKey(), repoName)
                            .put(OperationRouting.EPHEMERAL_INDEX_SNAPSHOT_SETTING.getKey(), snapshotName)
                            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), "ephemeral")
                            .build()
                        ))).build(), "adding ephemeral index");
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    indexCreatedLatch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    throw new AssertionError(source, e);
                }
            });

        indexCreatedLatch.await();

        final TotalHits newTotalHits = internalCluster().client().prepareSearch(indexName).get().getHits().getTotalHits();

        assertThat(newTotalHits.value, equalTo(originalTotalHits.value));
    }
}
