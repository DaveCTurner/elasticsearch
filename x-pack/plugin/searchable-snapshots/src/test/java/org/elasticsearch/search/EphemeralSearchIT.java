/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseContext;
import org.elasticsearch.action.search.SearchShardIterator;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.ReadOnlyEngine;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.SearchableSnapshotDirectory;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.cluster.metadata.MetaData.ALL_CONTEXTS;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.indices.IndicesQueryCache.INDICES_CACHE_QUERY_COUNT_SETTING;
import static org.elasticsearch.indices.IndicesQueryCache.INDICES_CACHE_QUERY_SIZE_SETTING;
import static org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache.INDICES_FIELDDATA_CACHE_SIZE_KEY;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.SNAPSHOT_CODEC;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.SNAPSHOT_NAME_FORMAT;
import static org.elasticsearch.search.EphemeralSearchIT.EphemeralStorePlugin.EXTRA_CONTEXT_TYPE_EPHEMERAL;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class EphemeralSearchIT extends ESIntegTestCase {

    public static class EphemeralStorePlugin extends Plugin implements IndexStorePlugin, EnginePlugin {

        private static final Logger logger = LogManager.getLogger(EphemeralStorePlugin.class);

        static final String EXTRA_CONTEXT_TYPE_EPHEMERAL = "ephemeral";
        private static final String EXTRA_CONTEXT_REPOSITORY_KEY = "repository";
        private static final String EXTRA_CONTEXT_SNAPSHOT_NAME_KEY = "snapshot_name";
        private static final String EXTRA_CONTEXT_SNAPSHOT_UUID_KEY = "snapshot_uuid";

        public static final Setting<String> EPHEMERAL_INDEX_REPOSITORY_SETTING =
            Setting.simpleString("index.ephemeral.repository", Setting.Property.IndexScope, Setting.Property.PrivateIndex);
        public static final Setting<String> EPHEMERAL_INDEX_SNAPSHOT_NAME_SETTING =
            Setting.simpleString("index.ephemeral.snapshot_name", Setting.Property.IndexScope, Setting.Property.PrivateIndex);
        public static final Setting<String> EPHEMERAL_INDEX_SNAPSHOT_UUID_SETTING =
            Setting.simpleString("index.ephemeral.snapshot_uuid", Setting.Property.IndexScope, Setting.Property.PrivateIndex);

        private RepositoriesService repositoriesService;
        private ThreadPool threadPool;
        private ClusterService clusterService;
        private IndicesService indicesService;

        void setup(RepositoriesService repositoriesService, IndicesService indicesService) {
            this.repositoriesService = Objects.requireNonNull(repositoriesService);
            this.indicesService = Objects.requireNonNull(indicesService);
        }

        @Override
        public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                                   ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                                   NamedXContentRegistry xContentRegistry, Environment environment,
                                                   NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
            this.threadPool = threadPool;
            this.clusterService = clusterService;
            return emptyList();
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            if (indexSettings.getSettings().hasValue(EPHEMERAL_INDEX_REPOSITORY_SETTING.getKey())) {
                return Optional.of(config -> new ReadOnlyEngine(config, null, new TranslogStats(), false, Function.identity()));
            } else {
                return Optional.empty();
            }
        }

        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return singletonList(new NamedWriteableRegistry.Entry(MetaData.Custom.class,
                EphemeralIndexDescription.NAME, EphemeralIndexDescription::new));
        }

        @Override
        public List<Setting<?>> getSettings() {
            return Arrays.asList(
                EPHEMERAL_INDEX_REPOSITORY_SETTING,
                EPHEMERAL_INDEX_SNAPSHOT_NAME_SETTING,
                EPHEMERAL_INDEX_SNAPSHOT_UUID_SETTING);
        }

        @Override
        public Map<String, DirectoryFactory> getDirectoryFactories() {
            return Map.of("ephemeral", (indexSettings, shardPath) -> {
                final StepListener<RepositoryData> repositoryDataFuture = new StepListener<>();
                final PlainActionFuture<Directory> directoryFuture = new PlainActionFuture<>();
                final String repositoryName = EPHEMERAL_INDEX_REPOSITORY_SETTING.get(indexSettings.getSettings());
                final BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(repositoryName);

                repositoryDataFuture.whenComplete(repositoryData -> ActionListener.completeWith(directoryFuture, () -> {

                    final String snapshotName = EPHEMERAL_INDEX_SNAPSHOT_NAME_SETTING.get(indexSettings.getSettings());

                    final SnapshotId snapshotId = repositoryData.getSnapshotIds().stream()
                        .filter(s -> s.getName().equals(snapshotName))
                        .findFirst()
                        .orElseThrow(() -> new ElasticsearchException(
                            "snapshot [" + snapshotName + "] not found in repository [" + repositoryName + "]"));

                    final IndexId indexId = repositoryData.resolveIndexId(indexSettings.getIndex().getName());

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

                // do this after calling whenComplete() so that the completion handler runs on the generic thread too
                threadPool.generic().execute(() -> repository.getRepositoryData(repositoryDataFuture));

                return directoryFuture.actionGet();
            });
        }

        Tuple<IndexService, Releasable> makeEphemeralIndexService(ShardSearchRequest request) {

            assert request.extraContext().get(EXTRA_CONTEXT_REPOSITORY_KEY) != null;
            assert request.extraContext().get(EXTRA_CONTEXT_SNAPSHOT_NAME_KEY) != null;
            assert request.extraContext().get(EXTRA_CONTEXT_SNAPSHOT_UUID_KEY) != null;

            final List<Closeable> closeables = new ArrayList<>(2);
            try {
                final Repository repository = repositoriesService.repository(request.extraContext().get(EXTRA_CONTEXT_REPOSITORY_KEY));
                final IndexId indexId = new IndexId(request.shardId().getIndex().getName(), request.shardId().getIndex().getUUID());
                final SnapshotId snapshotId = new SnapshotId(request.extraContext().get(EXTRA_CONTEXT_SNAPSHOT_NAME_KEY),
                    request.extraContext().get(EXTRA_CONTEXT_SNAPSHOT_UUID_KEY));

                final PlainActionFuture<IndexMetaData> indexMetaDataFuture = new PlainActionFuture<>();
                threadPool.generic().execute(() -> ActionListener.completeWith(indexMetaDataFuture,
                    () -> repository.getSnapshotIndexMetaData(snapshotId, indexId)));
                final IndexMetaData indexMetaData = IndexMetaData.builder(indexMetaDataFuture.actionGet())
                    .settings(Settings.builder().put(indexMetaDataFuture.actionGet().getSettings())
                        .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), "ephemeral")
                        .put(EPHEMERAL_INDEX_REPOSITORY_SETTING.getKey(), request.extraContext().get(EXTRA_CONTEXT_REPOSITORY_KEY))
                        .put(EPHEMERAL_INDEX_SNAPSHOT_NAME_SETTING.getKey(), request.extraContext().get(EXTRA_CONTEXT_SNAPSHOT_NAME_KEY))
                        .put(EPHEMERAL_INDEX_SNAPSHOT_UUID_SETTING.getKey(), request.extraContext().get(EXTRA_CONTEXT_SNAPSHOT_UUID_KEY))
                        .put(IndexMetaData.SETTING_INDEX_UUID, indexId.getId())
                    ).build();
                final IndexService indexService = indicesService.createIndexService(IndexService.IndexCreationContext.CREATE_INDEX,
                    indexMetaData,
                    new IndicesQueryCache(Settings.builder()
                        .put(INDICES_CACHE_QUERY_SIZE_SETTING.getKey(), "0b")
                        .put(INDICES_CACHE_QUERY_COUNT_SETTING.getKey(), 1).build()),
                    new IndicesFieldDataCache(Settings.builder()
                        .put(INDICES_FIELDDATA_CACHE_SIZE_KEY.getKey(), "0b").build(),
                        new IndexFieldDataCache.Listener() {
                        }),
                    emptyList());
                closeables.add(() -> indexService.removeShard(request.shardId().id(), "failed to create search context"));
                closeables.add(() -> indexService.close("failed to create search context", true));

                indexService.updateMetaData(indexMetaData, indexMetaData);
                indexService.updateMapping(indexMetaData, indexMetaData);

                final ShardRouting unassigned = ShardRouting.newUnassigned(request.shardId(), true,
                    RecoverySource.ExistingStoreRecoverySource.INSTANCE,
                    new UnassignedInfo(UnassignedInfo.Reason.EXISTING_INDEX_RESTORED, "ephemeral"));
                final ShardRouting initializing = unassigned.initialize(clusterService.localNode().getId(), null, 0L);

                final PlainActionFuture<Boolean> recoveryCompleteListener = new PlainActionFuture<>();
                final IndexShard indexShard = indexService.createShard(initializing, shardId -> {
                }, RetentionLeaseSyncer.EMPTY);
                indexShard.markAsRecovering("ephemeral shard", new RecoveryState(initializing, clusterService.localNode(), null));
                indexShard.recoverFromStore(recoveryCompleteListener);
                recoveryCompleteListener.actionGet();

                final Tuple<IndexService, Releasable> result = Tuple.tuple(indexService, () -> {
                    try {
                        IOUtils.close(
                            () -> indexService.removeShard(request.shardId().id(), "after search"),
                            () -> indexService.close("after search", true));
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });

                closeables.clear();
                return result;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                if (closeables.isEmpty() == false) {
                    IOUtils.closeWhileHandlingException(closeables);
                }
            }
        }

        void resolveExtraIndices(ClusterState clusterState, long absoluteStartTimeMillis, OriginalIndices originalIndices,
                                 ActionListener<SearchService.ExtraIndicesResolverResponse> listener) {
            final EphemeralIndexDescription ephemeralIndexDescription = clusterState.metaData().custom(EphemeralIndexDescription.NAME);
            if (ephemeralIndexDescription == null
                || Arrays.stream(originalIndices.indices()).anyMatch(s -> s.equals(ephemeralIndexDescription.alias)) == false) {
                listener.onResponse(new SearchService.ExtraIndicesResolverResponse(originalIndices, emptyList()));
            } else {
                final Repository repository = repositoriesService.repository(ephemeralIndexDescription.repositoryName);
                threadPool.generic().execute(() -> repository.getRepositoryData(ActionListener.wrap(repositoryData -> {
                    final IndexId indexId = repositoryData.resolveIndexId(ephemeralIndexDescription.indexName);
                    final SnapshotId snapshotId = repositoryData.getSnapshotIds().stream().filter(s ->
                        s.getName().equals(ephemeralIndexDescription.snapshotName)).findFirst().orElseThrow(() -> new ElasticsearchException(
                        "snapshot [" + ephemeralIndexDescription.snapshotName + "] not found in repository ["
                            + ephemeralIndexDescription.repositoryName + "]"));
                    final IndexMetaData indexMetaData = repository.getSnapshotIndexMetaData(snapshotId, indexId);
                    final DiscoveryNode discoveryNode = clusterState.nodes().getDataNodes().valuesIt().next();
                    final RecoverySource.SnapshotRecoverySource snapshotRecoverySource = new RecoverySource.SnapshotRecoverySource("_na_",
                        new Snapshot(ephemeralIndexDescription.repositoryName, snapshotId),
                        Version.CURRENT, ephemeralIndexDescription.indexName);
                    final OriginalIndices ephemeralIndex = new OriginalIndices(new String[]{ephemeralIndexDescription.alias}, originalIndices.indicesOptions());
                    final List<SearchShardIterator> shardIterators = new ArrayList<>();
                    for (int shardNumber = 0; shardNumber < indexMetaData.getNumberOfShards(); shardNumber++) {
                        final ShardId shardId = new ShardId(indexId.getName(), indexId.getId(), shardNumber);
                        final ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, true, snapshotRecoverySource,
                            new UnassignedInfo(UnassignedInfo.Reason.REINITIALIZED, "ephemeral"))
                            .initialize(discoveryNode.getId(), null, 0L);
                        shardIterators.add(new SearchShardIterator(null, shardId, singletonList(shardRouting), ephemeralIndex) {
                            @Override
                            public ShardSearchRequest buildShardSearchRequest(SearchPhaseContext searchPhaseContext) {

                                final Map<String, String> extraContext = Map.of(
                                    SearchService.CUSTOM_INDEX_SERVICE_SUPPLIER_TYPE_KEY, EXTRA_CONTEXT_TYPE_EPHEMERAL,
                                    EXTRA_CONTEXT_REPOSITORY_KEY, ephemeralIndexDescription.repositoryName,
                                    EXTRA_CONTEXT_SNAPSHOT_NAME_KEY, snapshotId.getName(),
                                    EXTRA_CONTEXT_SNAPSHOT_UUID_KEY, snapshotId.getUUID());

                                // default to no AliasFilter and an IndexBoost of 1.0f here, TODO TBD do we want to support other values here?

                                return new ShardSearchRequest(getOriginalIndices(), searchPhaseContext.getRequest(), shardId,
                                    searchPhaseContext.getNumShards(), new AliasFilter((QueryBuilder) null), 1.0f, absoluteStartTimeMillis,
                                    null, new String[0], extraContext);
                            }
                        });
                    }

                    final OriginalIndices remainingIndices = new OriginalIndices(Arrays.stream(originalIndices.indices())
                        .filter(s -> s.equals(ephemeralIndexDescription.alias) == false).toArray(String[]::new), originalIndices.indicesOptions());

                    listener.onResponse(new SearchService.ExtraIndicesResolverResponse(remainingIndices, shardIterators));
                }, listener::onFailure)));
            }
        }
    }

    public static class EphemeralIndexDescription extends AbstractNamedDiffable<MetaData.Custom> implements MetaData.Custom {

        public static final String NAME = "ephemeral-index";

        public final String alias;
        public final String repositoryName;
        public final String snapshotName;
        public final String indexName;

        public EphemeralIndexDescription(String alias, String repositoryName, String snapshotName, String indexName) {
            this.alias = alias;
            this.repositoryName = repositoryName;
            this.snapshotName = snapshotName;
            this.indexName = indexName;
        }

        public EphemeralIndexDescription(StreamInput in) throws IOException {
            alias = in.readString();
            repositoryName = in.readString();
            snapshotName = in.readString();
            indexName = in.readString();
        }

        @Override
        public EnumSet<MetaData.XContentContext> context() {
            return ALL_CONTEXTS;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.V_8_0_0;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(alias);
            out.writeString(repositoryName);
            out.writeString(snapshotName);
            out.writeString(indexName);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME);
            builder.field("alias", alias);
            builder.field("repository", repositoryName);
            builder.field("snapshot", snapshotName);
            builder.field("index", indexName);
            builder.endObject();
            return builder;
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
                ephemeralStorePlugin.setup(
                    internalCluster().getInstance(RepositoriesService.class, nodeName),
                    internalCluster().getInstance(IndicesService.class, nodeName));

                final SearchService searchService = internalCluster().getInstance(SearchService.class, nodeName);
                searchService.addIndexServiceSupplier(EXTRA_CONTEXT_TYPE_EPHEMERAL, new SearchService.CustomIndexServiceSupplier() {
                    @Override
                    public String executorName() {
                        return ThreadPool.Names.SEARCH_THROTTLED;
                    }

                    @Override
                    public Tuple<IndexService, Releasable> getIndexService(ShardSearchRequest shardSearchRequest) {
                        return ephemeralStorePlugin.makeEphemeralIndexService(shardSearchRequest);
                    }
                });
                searchService.addExtraIndicesResolver(ephemeralStorePlugin::resolveExtraIndices);
            }
        }

        final String repoName = randomAlphaOfLength(10);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String ephemeralAlias = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        final Path repo = randomRepoPath();
        assertAcked(client().admin().cluster().preparePutRepository(repoName)
            .setType("fs")
            .setSettings(Settings.builder()
                .put("location", repo)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        createIndex(indexName);
        final List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        for (int i = between(10, 50); i >= 0; i--) {
            indexRequestBuilders.add(client().prepareIndex(indexName).setSource("foo", randomBoolean() ? "bar" : "baz"));
        }
        indexRandom(true, indexRequestBuilders);
        flushAndRefresh(indexName);

        final TotalHits originalAllHits = internalCluster().client().prepareSearch(indexName).get().getHits().getTotalHits();
        final TotalHits originalBarHits = internalCluster().client().prepareSearch(indexName)
            .setQuery(matchQuery("foo", "bar")).get().getHits().getTotalHits();
        logger.info("--> [{}] in total, of which [{}] match the query", originalAllHits, originalBarHits);

        CreateSnapshotResponse createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true).get();
        final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));

        assertAcked(client().admin().indices().prepareDelete(indexName));

        final ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());

        final CountDownLatch indexCreatedLatch = new CountDownLatch(1);
        clusterService.submitStateUpdateTask("test",
            new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return ClusterState.builder(currentState).metaData(MetaData.builder(currentState.metaData())
                        .putCustom(EphemeralIndexDescription.NAME,
                            new EphemeralIndexDescription(ephemeralAlias, repoName, snapshotName, indexName))).build();
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

        // need dataNodeClient() because client nodes don't apply cluster state updates to snapshot repos - TODO fix this
        final TotalHits newAllHits = internalCluster().dataNodeClient().prepareSearch(ephemeralAlias).get().getHits().getTotalHits();
        final TotalHits newBarHits = internalCluster().dataNodeClient().prepareSearch(ephemeralAlias)
            .setQuery(matchQuery("foo", "bar")).get().getHits().getTotalHits();

        logger.info("--> [{}] in total, of which [{}] match the query", newAllHits, newBarHits);

        assertThat(newAllHits, equalTo(originalAllHits));
        assertThat(newBarHits, equalTo(originalBarHits));
    }
}
