/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.lifecycle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsAction;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.datastreams.ModifyDataStreamsAction;
import org.elasticsearch.action.datastreams.PutDataStreamOptionsAction;
import org.elasticsearch.action.datastreams.lifecycle.ErrorEntry;
import org.elasticsearch.action.datastreams.lifecycle.ExplainDataStreamLifecycleAction;
import org.elasticsearch.action.datastreams.lifecycle.ExplainIndexDataStreamLifecycle;
import org.elasticsearch.action.datastreams.lifecycle.PutDataStreamLifecycleAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.coordination.StableMasterHealthIndicatorService;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAction;
import org.elasticsearch.cluster.metadata.DataStreamFailureStore;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.lifecycle.health.DataStreamLifecycleHealthIndicatorService;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.GetHealthAction;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.node.DataStreamLifecycleHealthInfo;
import org.elasticsearch.health.node.DslErrorInfo;
import org.elasticsearch.health.node.FetchHealthInfoCacheAction;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;

import java.io.IOException;
import java.time.Clock;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.backingIndexEqualTo;
import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.READ_ONLY;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.DATA_STREAM_MERGE_POLICY_TARGET_FACTOR_SETTING;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.DATA_STREAM_MERGE_POLICY_TARGET_FLOOR_SEGMENT_SETTING;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.ONE_HUNDRED_MB;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.TARGET_MERGE_FACTOR_VALUE;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleServiceIT.TestSystemDataStreamPlugin.SYSTEM_DATA_STREAM_NAME;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleServiceIT.TestSystemDataStreamPlugin.SYSTEM_DATA_STREAM_RETENTION_DAYS;
import static org.elasticsearch.datastreams.lifecycle.health.DataStreamLifecycleHealthIndicatorService.STAGNATING_BACKING_INDICES_DIAGNOSIS_DEF;
import static org.elasticsearch.datastreams.lifecycle.health.DataStreamLifecycleHealthIndicatorService.STAGNATING_INDEX_IMPACT;
import static org.elasticsearch.index.IndexSettings.LIFECYCLE_ORIGINATION_DATE;
import static org.elasticsearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class DataStreamLifecycleServiceIT extends ESIntegTestCase {
    private static final Logger logger = LogManager.getLogger(DataStreamLifecycleServiceIT.class);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            DataStreamsPlugin.class,
            MockTransportService.TestPlugin.class,
            TestSystemDataStreamPlugin.class,
            MapperExtrasPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        settings.put(DataStreamLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL, "1s");
        settings.put(DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING.getKey(), "min_docs=1,max_docs=1");
        // we'll test DSL errors reach the health node, so we're lowering the threshold over which we report errors
        settings.put(DataStreamLifecycleService.DATA_STREAM_SIGNALLING_ERROR_RETRY_INTERVAL_SETTING.getKey(), "3");
        return settings.build();
    }

    @After
    public void cleanup() {
        // we change SETTING_CLUSTER_MAX_SHARDS_PER_NODE in a test so let's make sure we clean it up even when the test fails
        updateClusterSettings(Settings.builder().putNull("*"));
    }

    public void testRolloverLifecycle() throws Exception {
        // empty lifecycle contains the default rollover
        DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.Template.DATA_DEFAULT;

        putComposableIndexTemplate("id1", null, List.of("metrics-foo*"), null, null, lifecycle, false);
        String dataStreamName = "metrics-foo";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            dataStreamName
        );
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        indexDocs(dataStreamName, 1);

        List<String> backingIndices = waitForDataStreamBackingIndices(dataStreamName, 2);
        assertThat(backingIndices.size(), equalTo(2));
        String backingIndex = backingIndices.get(0);
        assertThat(backingIndex, backingIndexEqualTo(dataStreamName, 1));
        String writeIndex = backingIndices.get(1);
        assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 2));
    }

    public void testRolloverAndRetention() throws Exception {
        DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.ZERO).buildTemplate();

        putComposableIndexTemplate("id1", null, List.of("metrics-foo*"), null, null, lifecycle, false);

        String dataStreamName = "metrics-foo";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            dataStreamName
        );
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        indexDocs(dataStreamName, 1);

        assertBusy(() -> {
            List<String> backingIndices = getDataStreamBackingIndexNames(dataStreamName);
            assertThat(backingIndices.size(), equalTo(1));
            // we expect the data stream to have only one backing index, the write one, with generation 2
            // as generation 1 would've been deleted by the data stream lifecycle given the configuration
            String writeIndex = backingIndices.get(0);
            assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 2));
        });
    }

    @SuppressWarnings("unchecked")
    public void testSystemDataStreamRetention() throws Exception {
        /*
         * This test makes sure that global data stream retention is ignored by system data streams, and that the configured retention
         * for a system data stream is respected instead.
         */
        Iterable<DataStreamLifecycleService> dataStreamLifecycleServices = internalCluster().getInstances(DataStreamLifecycleService.class);
        Clock clock = Clock.systemUTC();
        AtomicLong now = new AtomicLong(clock.millis());
        dataStreamLifecycleServices.forEach(dataStreamLifecycleService -> dataStreamLifecycleService.setNowSupplier(now::get));
        try {
            try {

                CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    SYSTEM_DATA_STREAM_NAME
                );
                client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).actionGet();
                indexDocs(SYSTEM_DATA_STREAM_NAME, 1);
                now.addAndGet(TimeValue.timeValueSeconds(30).millis());
                List<String> backingIndices = waitForDataStreamBackingIndices(SYSTEM_DATA_STREAM_NAME, 2);
                // we expect the data stream to have two backing indices since the effective retention is 100 days
                String writeIndex = backingIndices.get(1);
                assertThat(writeIndex, backingIndexEqualTo(SYSTEM_DATA_STREAM_NAME, 2));

                // Now we advance the time to well beyond the configured retention. We expect that the older index will have been deleted.
                now.addAndGet(TimeValue.timeValueDays(3 * TestSystemDataStreamPlugin.SYSTEM_DATA_STREAM_RETENTION_DAYS).millis());
                assertBusy(() -> {
                    GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(
                        TEST_REQUEST_TIMEOUT,
                        new String[] { SYSTEM_DATA_STREAM_NAME }
                    );
                    GetDataStreamAction.Response getDataStreamResponse = client().execute(
                        GetDataStreamAction.INSTANCE,
                        getDataStreamRequest
                    ).actionGet();
                    assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
                    assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getName(), equalTo(SYSTEM_DATA_STREAM_NAME));
                    List<Index> currentBackingIndices = getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices();
                    assertThat(currentBackingIndices.size(), equalTo(1)); // global retention is ignored
                    // we expect the data stream to have only one backing index, the write one, with generation 2
                    // as generation 1 would've been deleted by the data stream lifecycle given the configuration
                    String currentWriteIndex = currentBackingIndices.get(0).getName();
                    assertThat(currentWriteIndex, backingIndexEqualTo(SYSTEM_DATA_STREAM_NAME, 2));
                    try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
                        builder.humanReadable(true);
                        ToXContent.Params withEffectiveRetention = new ToXContent.MapParams(
                            DataStreamLifecycle.INCLUDE_EFFECTIVE_RETENTION_PARAMS
                        );
                        getDataStreamResponse.getDataStreams()
                            .get(0)
                            .toXContent(
                                builder,
                                withEffectiveRetention,
                                getDataStreamResponse.getRolloverConfiguration(),
                                getDataStreamResponse.getDataGlobalRetention(),
                                getDataStreamResponse.getFailuresGlobalRetention()
                            );
                        String serialized = Strings.toString(builder);
                        Map<String, Object> resultMap = XContentHelper.convertToMap(
                            XContentType.JSON.xContent(),
                            serialized,
                            randomBoolean()
                        );
                        assertNotNull(resultMap);
                        Map<String, Object> lifecycleMap = (Map<String, Object>) resultMap.get("lifecycle");
                        assertNotNull(lifecycleMap);
                        assertThat(
                            lifecycleMap.get("data_retention"),
                            equalTo(TimeValue.timeValueDays(SYSTEM_DATA_STREAM_RETENTION_DAYS).getStringRep())
                        );
                        assertThat(
                            lifecycleMap.get("effective_retention"),
                            equalTo(TimeValue.timeValueDays(SYSTEM_DATA_STREAM_RETENTION_DAYS).getStringRep())
                        );
                        assertThat(lifecycleMap.get("retention_determined_by"), equalTo("data_stream_configuration"));
                        assertThat(lifecycleMap.get("enabled"), equalTo(true));
                    }
                });

                client().execute(
                    DeleteDataStreamAction.INSTANCE,
                    new DeleteDataStreamAction.Request(TEST_REQUEST_TIMEOUT, SYSTEM_DATA_STREAM_NAME)
                ).actionGet();
            } finally {
                // reset properties
            }
        } finally {
            dataStreamLifecycleServices.forEach(dataStreamLifecycleService -> dataStreamLifecycleService.setNowSupplier(clock::millis));
        }
    }

    public void testOriginationDate() throws Exception {
        /*
         * In this test, we set up a datastream with 7 day retention. Then we add two indices to it -- one with an origination date 365
         * days ago, and one with an origination date 1 day ago. After data stream lifecycle runs, we expect the one with the old
         * origination date to have been deleted, and the one with the newer origination date to remain.
         */
        DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.dataLifecycleBuilder()
            .dataRetention(TimeValue.timeValueDays(7))
            .buildTemplate();

        putComposableIndexTemplate("id1", null, List.of("metrics-foo*"), null, null, lifecycle, false);

        String dataStreamName = "metrics-foo";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            dataStreamName
        );
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        indexDocs(dataStreamName, 1);

        String mapping = """
             {
                "properties":{
                    "@timestamp": {
                        "type": "date"
                    }
                }
            }""";
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request("id2");
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of("index_*"))
                .template(Template.builder().mappings(CompressedXContent.fromJSON(mapping)))
                .build()
        );
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();

        String indexWithOldOriginationDate = "index_old";
        long originTimeMillis = System.currentTimeMillis() - TimeValue.timeValueDays(365).millis();
        createIndex(indexWithOldOriginationDate, Settings.builder().put(LIFECYCLE_ORIGINATION_DATE, originTimeMillis).build());
        client().execute(
            ModifyDataStreamsAction.INSTANCE,
            new ModifyDataStreamsAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                List.of(DataStreamAction.addBackingIndex(dataStreamName, indexWithOldOriginationDate))
            )
        ).get();

        String indexWithNewOriginationDate = "index_new";
        originTimeMillis = System.currentTimeMillis() - TimeValue.timeValueDays(1).millis();
        createIndex(indexWithNewOriginationDate, Settings.builder().put(LIFECYCLE_ORIGINATION_DATE, originTimeMillis).build());
        client().execute(
            ModifyDataStreamsAction.INSTANCE,
            new ModifyDataStreamsAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                List.of(DataStreamAction.addBackingIndex(dataStreamName, indexWithNewOriginationDate))
            )
        ).get();

        assertBusy(() -> {
            List<String> backingIndices = getDataStreamBackingIndexNames(dataStreamName);
            assertTrue(backingIndices.contains("index_new"));
            assertFalse(backingIndices.contains("index_old"));
        });
    }

    public void testUpdatingLifecycleAppliesToAllBackingIndices() throws Exception {
        DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.Template.DATA_DEFAULT;

        putComposableIndexTemplate("id1", null, List.of("metrics-foo*"), null, null, lifecycle, false);

        String dataStreamName = "metrics-foo";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            dataStreamName
        );
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        client().admin().indices().rolloverIndex(new RolloverRequest(dataStreamName, null)).actionGet();
        client().admin().indices().rolloverIndex(new RolloverRequest(dataStreamName, null)).actionGet();
        int finalGeneration = 3;

        // Update the lifecycle of the data stream
        updateLifecycle(dataStreamName, TimeValue.timeValueMillis(1));
        // Verify that the retention has changed for all backing indices
        List<String> backingIndices = waitForDataStreamBackingIndices(dataStreamName, 1);
        assertThat(backingIndices.getFirst(), backingIndexEqualTo(dataStreamName, finalGeneration));
    }

    public void testAutomaticForceMerge() throws Exception {
        /*
         * This test makes sure that (1) data stream lifecycle does _not_ call forcemerge on an index in the same data stream lifecycle
         * pass when it rolls over the index and that (2) it _does_ call forcemerge on an index that was rolled over in a previous data
         * stream lifecycle pass. It's harder than you would think to detect through the REST API that forcemerge has been called. The
         * reason is that segment merging happens automatically during indexing, and when forcemerge is called it likely does nothing
         * because all necessary merging has already happened automatically. So in order to detect whether forcemerge has been called, we
         * use a SendRequestBehavior in the MockTransportService to detect it.
         */
        DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.Template.DATA_DEFAULT;
        disableDataStreamLifecycle();
        String dataStreamName = "metrics-foo";
        putComposableIndexTemplate(
            "id1",
            null,
            List.of(dataStreamName + "*"),
            indexSettings(1, 1).put(MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING.getKey(), ONE_HUNDRED_MB)
                .put(MergePolicyConfig.INDEX_MERGE_POLICY_MERGE_FACTOR_SETTING.getKey(), TARGET_MERGE_FACTOR_VALUE)
                .build(),
            null,
            lifecycle,
            false
        );
        // This is the set of all indices against which a ForceMergeAction has been run:
        final Set<String> forceMergedIndices = new HashSet<>();
        // Here we update the transport service on each node to record when a forcemerge action is called for an index:
        for (DiscoveryNode node : internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName())
            .state()
            .getNodes()) {
            MockTransportService.getInstance(node.getName())
                .addRequestHandlingBehavior(ForceMergeAction.NAME + "[n]", (handler, request, channel, task) -> {
                    String index = ((IndicesRequest) request).indices()[0];
                    forceMergedIndices.add(index);
                    logger.info("Force merging {}", index);
                    handler.messageReceived(request, channel, task);
                });
        }

        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            dataStreamName
        );
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        int finalGeneration = randomIntBetween(3, 4);
        for (int currentGeneration = 1; currentGeneration < finalGeneration; currentGeneration++) {
            // This is currently the write index, but it will be rolled over as soon as data stream lifecycle runs:
            final var backingIndexNames = waitForDataStreamBackingIndices(dataStreamName, currentGeneration);
            final String toBeRolledOverIndex = backingIndexNames.get(currentGeneration - 1);
            for (int i = 0; i < randomIntBetween(10, 50); i++) {
                indexDocs(dataStreamName, randomIntBetween(1, 300));
                // Make sure the segments get written:
                BroadcastResponse flushResponse = indicesAdmin().flush(new FlushRequest(toBeRolledOverIndex)).actionGet();
                assertThat(flushResponse.getStatus(), equalTo(RestStatus.OK));
            }

            final String toBeForceMergedIndex;
            if (currentGeneration == 1) {
                toBeForceMergedIndex = null; // Not going to be used
            } else {
                toBeForceMergedIndex = backingIndexNames.get(currentGeneration - 2);
            }
            int currentBackingIndexCount = currentGeneration;
            DataStreamLifecycleService dataStreamLifecycleService = internalCluster().getInstance(
                DataStreamLifecycleService.class,
                internalCluster().getMasterName()
            );
            ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
            // run data stream lifecycle once
            dataStreamLifecycleService.run(clusterService.state());
            assertBusy(() -> {
                List<String> backingIndices = getDataStreamBackingIndexNames(dataStreamName);
                assertThat(backingIndices.size(), equalTo(currentBackingIndexCount + 1));
                assertThat(backingIndices.getLast(), backingIndexEqualTo(dataStreamName, currentBackingIndexCount + 1));
                /*
                 * We only expect forcemerge to happen on the 2nd data stream lifecycle run and later, since on the first there's only the
                 *  single write index to be rolled over.
                 */
                if (currentBackingIndexCount > 1) {
                    assertThat(
                        "The segments for " + toBeForceMergedIndex + " were not merged",
                        forceMergedIndices.contains(toBeForceMergedIndex),
                        equalTo(true)
                    );
                }
                // We want to assert that when data stream lifecycle rolls over the write index it, it doesn't forcemerge it on that
                // iteration:
                assertThat(
                    "The segments for " + toBeRolledOverIndex + " were unexpectedly merged",
                    forceMergedIndices.contains(toBeRolledOverIndex),
                    equalTo(false)
                );
            });
        }
    }

    private static void disableDataStreamLifecycle() {
        updateClusterSettings(Settings.builder().put(DataStreamLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL, TimeValue.MAX_VALUE));
    }

    public void testErrorRecordingOnRollover() throws Exception {
        // empty lifecycle contains the default rollover
        DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.Template.DATA_DEFAULT;
        /*
         * We set index.auto_expand_replicas to 0-1 so that if we get a single-node cluster it is not yellow. The cluster being yellow
         * could result in data stream lifecycle's automatic forcemerge failing, which would result in an unexpected error in the error
         * store.
         */
        putComposableIndexTemplate(
            "id1",
            null,
            List.of("metrics-foo*"),
            Settings.builder().put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1").build(),
            null,
            lifecycle,
            false
        );

        String dataStreamName = "metrics-foo";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            dataStreamName
        );
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        indexDocs(dataStreamName, 1);

        // let's allow one rollover to go through
        var backingIndices = waitForDataStreamBackingIndices(dataStreamName, 2);

        // prevent new indices from being created (ie. future rollovers)
        updateClusterSettings(Settings.builder().put(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), 1));

        indexDocs(dataStreamName, 1);

        String writeIndexName = backingIndices.get(1);
        assertBusy(() -> {
            DataStreamLifecycleService lifecycleService = internalCluster().getCurrentMasterNodeInstance(DataStreamLifecycleService.class);

            ErrorEntry writeIndexRolloverError = lifecycleService.getErrorStore().getError(Metadata.DEFAULT_PROJECT_ID, writeIndexName);
            assertThat(writeIndexRolloverError, is(notNullValue()));
            assertThat(writeIndexRolloverError.error(), containsString("maximum normal shards open"));

            ExplainDataStreamLifecycleAction.Request explainRequest = new ExplainDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStreamName }
            );
            ExplainDataStreamLifecycleAction.Response response = client().execute(ExplainDataStreamLifecycleAction.INSTANCE, explainRequest)
                .actionGet();
            boolean found = false;
            for (ExplainIndexDataStreamLifecycle index : response.getIndices()) {
                if (index.getError() != null && index.getError().retryCount() > 3) {
                    found = true;
                    break;
                }
            }
            assertTrue(found);
        }, 30, TimeUnit.SECONDS);

        // Ensure data stream did not roll over yet.
        assertEquals(2, getDataStreamBackingIndexNames(dataStreamName).size());

        // DSL should signal to the health node that there's an error in the store that's been retried at least 3 times
        assertBusy(() -> {
            FetchHealthInfoCacheAction.Response healthNodeResponse = client().execute(
                FetchHealthInfoCacheAction.INSTANCE,
                new FetchHealthInfoCacheAction.Request()
            ).get();
            DataStreamLifecycleHealthInfo dslHealthInfoOnHealthNode = healthNodeResponse.getHealthInfo().dslHealthInfo();
            assertThat(dslHealthInfoOnHealthNode, is(not(DataStreamLifecycleHealthInfo.NO_DSL_ERRORS)));
            assertThat(dslHealthInfoOnHealthNode.dslErrorsInfo().size(), is(1));
            DslErrorInfo errorInfo = dslHealthInfoOnHealthNode.dslErrorsInfo().get(0);

            assertThat(errorInfo.indexName(), is(writeIndexName));
            assertThat(errorInfo.retryCount(), greaterThanOrEqualTo(3));
        });

        GetHealthAction.Response healthResponse = client().execute(GetHealthAction.INSTANCE, new GetHealthAction.Request(true, 1000))
            .actionGet();
        HealthIndicatorResult masterIsStableIndicator = healthResponse.findIndicator(StableMasterHealthIndicatorService.NAME);
        // if the cluster doesn't have a stable master we'll avoid asserting on the health report API as some indicators will not
        // be computed
        if (masterIsStableIndicator.status() == HealthStatus.GREEN) {
            // the shards capacity indicator is dictating the overall status
            assertThat(healthResponse.getStatus(), is(HealthStatus.RED));
            HealthIndicatorResult dslIndicator = healthResponse.findIndicator(DataStreamLifecycleHealthIndicatorService.NAME);
            assertThat(dslIndicator.status(), is(HealthStatus.YELLOW));
            assertThat(dslIndicator.impacts(), is(STAGNATING_INDEX_IMPACT));
            assertThat(
                dslIndicator.symptom(),
                is("A backing index has repeatedly encountered errors whilst trying to advance in its lifecycle")
            );

            Diagnosis diagnosis = dslIndicator.diagnosisList().get(0);
            assertThat(diagnosis.definition(), is(STAGNATING_BACKING_INDICES_DIAGNOSIS_DEF));
            assertThat(diagnosis.affectedResources().get(0).getValues(), containsInAnyOrder(writeIndexName));
        }

        // let's reset the cluster max shards per node limit to allow rollover to proceed and check the error store is empty
        updateClusterSettings(Settings.builder().putNull("*"));

        assertBusy(() -> {
            List<String> currentBackingIndices = getDataStreamBackingIndexNames(dataStreamName);
            assertThat(currentBackingIndices.size(), equalTo(3));
            String writeIndex = currentBackingIndices.get(2);
            // rollover was successful and we got to generation 3
            assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 3));

            // we recorded the error against the previous write index (generation 2)
            // let's check there's no error recorded against it anymore
            String previousWriteInddex = currentBackingIndices.get(1);
            DataStreamLifecycleService lifecycleService = internalCluster().getCurrentMasterNodeInstance(DataStreamLifecycleService.class);
            assertThat(lifecycleService.getErrorStore().getError(Metadata.DEFAULT_PROJECT_ID, previousWriteInddex), nullValue());
        });

        // the error has been fixed so the health information shouldn't be reported anymore
        assertBusy(() -> {
            FetchHealthInfoCacheAction.Response healthNodeResponse = client().execute(
                FetchHealthInfoCacheAction.INSTANCE,
                new FetchHealthInfoCacheAction.Request()
            ).get();
            DataStreamLifecycleHealthInfo dslHealthInfoOnHealthNode = healthNodeResponse.getHealthInfo().dslHealthInfo();
            assertThat(dslHealthInfoOnHealthNode, is(DataStreamLifecycleHealthInfo.NO_DSL_ERRORS));
        });

        healthResponse = client().execute(GetHealthAction.INSTANCE, new GetHealthAction.Request(true, 1000)).actionGet();
        masterIsStableIndicator = healthResponse.findIndicator(StableMasterHealthIndicatorService.NAME);
        // if the cluster doesn't have a stable master we'll avoid asserting on the health report API as some indicators will not
        // be computed
        if (masterIsStableIndicator.status() == HealthStatus.GREEN) {
            assertThat(healthResponse.getStatus(), is(HealthStatus.GREEN));
        }
    }

    public void testErrorRecordingOnRetention() throws Exception {
        // starting with a lifecycle without retention so we can rollover the data stream and manipulate the second generation index such
        // that its retention execution fails
        DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.Template.DATA_DEFAULT;

        /*
         * We set index.auto_expand_replicas to 0-1 so that if we get a single-node cluster it is not yellow. The cluster being yellow
         * could result in data stream lifecycle's automatic forcemerge failing, which would result in an unexpected error in the error
         * store.
         */
        putComposableIndexTemplate(
            "id1",
            null,
            List.of("metrics-foo*"),
            Settings.builder().put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1").build(),
            null,
            lifecycle,
            false
        );

        String dataStreamName = "metrics-foo";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            dataStreamName
        );
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        indexDocs(dataStreamName, 1);

        // let's allow one rollover to go through
        List<String> dsBackingIndices = waitForDataStreamBackingIndices(dataStreamName, 2);
        String firstGenerationIndex = dsBackingIndices.get(0);
        String secondGenerationIndex = dsBackingIndices.get(1);

        // mark the first generation index as read-only so deletion fails when we enable the retention configuration
        updateIndexSettings(Settings.builder().put(READ_ONLY.settingName(), true), firstGenerationIndex);
        try {
            updateLifecycle(dataStreamName, TimeValue.timeValueSeconds(1));

            assertBusy(() -> {
                List<String> backingIndices = getDataStreamBackingIndexNames(dataStreamName);
                assertThat(backingIndices.size(), equalTo(2));
                assertThat(backingIndices.getLast(), backingIndexEqualTo(dataStreamName, 2));

                DataStreamLifecycleService lifecycleService = internalCluster().getCurrentMasterNodeInstance(
                    DataStreamLifecycleService.class
                );

                ErrorEntry recordedRetentionExecutionError = lifecycleService.getErrorStore()
                    .getError(Metadata.DEFAULT_PROJECT_ID, firstGenerationIndex);
                assertThat(recordedRetentionExecutionError, is(notNullValue()));
                assertThat(recordedRetentionExecutionError.retryCount(), greaterThanOrEqualTo(3));
                assertThat(recordedRetentionExecutionError.error(), containsString("blocked by: [FORBIDDEN/5/index read-only (api)"));
            });

            // DSL should signal to the health node that there's an error in the store that's been retried at least 3 times
            assertBusy(() -> {
                FetchHealthInfoCacheAction.Response healthNodeResponse = client().execute(
                    FetchHealthInfoCacheAction.INSTANCE,
                    new FetchHealthInfoCacheAction.Request()
                ).get();
                DataStreamLifecycleHealthInfo dslHealthInfoOnHealthNode = healthNodeResponse.getHealthInfo().dslHealthInfo();
                assertThat(dslHealthInfoOnHealthNode, is(not(DataStreamLifecycleHealthInfo.NO_DSL_ERRORS)));
                assertThat(dslHealthInfoOnHealthNode.dslErrorsInfo().size(), is(1));
                DslErrorInfo errorInfo = dslHealthInfoOnHealthNode.dslErrorsInfo().get(0);
                assertThat(errorInfo.retryCount(), greaterThanOrEqualTo(3));
                assertThat(errorInfo.indexName(), equalTo(firstGenerationIndex));
            });

            GetHealthAction.Response healthResponse = client().execute(GetHealthAction.INSTANCE, new GetHealthAction.Request(true, 1000))
                .actionGet();
            HealthIndicatorResult masterIsStableIndicator = healthResponse.findIndicator(StableMasterHealthIndicatorService.NAME);
            // if the cluster doesn't have a stable master we'll avoid asserting on the health report API as some indicators will not
            // be computed
            if (masterIsStableIndicator.status() == HealthStatus.GREEN) {
                // the dsl indicator should turn the overall status yellow
                assertThat(healthResponse.getStatus(), is(HealthStatus.YELLOW));
                HealthIndicatorResult dslIndicator = healthResponse.findIndicator(DataStreamLifecycleHealthIndicatorService.NAME);
                assertThat(dslIndicator.status(), is(HealthStatus.YELLOW));
                assertThat(dslIndicator.impacts(), is(STAGNATING_INDEX_IMPACT));
                assertThat(
                    dslIndicator.symptom(),
                    is("A backing index has repeatedly encountered errors whilst trying to advance in its lifecycle")
                );

                Diagnosis diagnosis = dslIndicator.diagnosisList().get(0);
                assertThat(diagnosis.definition(), is(STAGNATING_BACKING_INDICES_DIAGNOSIS_DEF));
                assertThat(diagnosis.affectedResources().get(0).getValues(), contains(firstGenerationIndex));
            }

            // let's mark the index as writeable and make sure it's deleted and the error store is empty
            updateIndexSettings(Settings.builder().put(READ_ONLY.settingName(), false), firstGenerationIndex);

            assertBusy(() -> {
                List<String> backingIndices = getDataStreamBackingIndexNames(dataStreamName);
                // data stream only has one index now
                assertThat(backingIndices.size(), equalTo(1));

                // error store doesn't contain anything for the first generation index anymore
                DataStreamLifecycleService lifecycleService = internalCluster().getCurrentMasterNodeInstance(
                    DataStreamLifecycleService.class
                );
                assertThat(lifecycleService.getErrorStore().getError(Metadata.DEFAULT_PROJECT_ID, firstGenerationIndex), nullValue());
            });

            // health info for DSL should be EMPTY as everything's healthy
            assertBusy(() -> {
                FetchHealthInfoCacheAction.Response healthNodeResponse = client().execute(
                    FetchHealthInfoCacheAction.INSTANCE,
                    new FetchHealthInfoCacheAction.Request()
                ).get();
                DataStreamLifecycleHealthInfo dslHealthInfoOnHealthNode = healthNodeResponse.getHealthInfo().dslHealthInfo();
                assertThat(dslHealthInfoOnHealthNode, is(DataStreamLifecycleHealthInfo.NO_DSL_ERRORS));
            });

            healthResponse = client().execute(GetHealthAction.INSTANCE, new GetHealthAction.Request(true, 1000)).actionGet();
            masterIsStableIndicator = healthResponse.findIndicator(StableMasterHealthIndicatorService.NAME);
            // if the cluster doesn't have a stable master we'll avoid asserting on the health report API as some indicators will not
            // be computed
            if (masterIsStableIndicator.status() == HealthStatus.GREEN) {
                // the dsl indicator should turn the overall status yellow
                assertThat(healthResponse.getStatus(), is(HealthStatus.GREEN));
                HealthIndicatorResult dslIndicator = healthResponse.findIndicator(DataStreamLifecycleHealthIndicatorService.NAME);
                assertThat(dslIndicator.status(), is(HealthStatus.GREEN));
                assertThat(dslIndicator.impacts().size(), is(0));
                assertThat(dslIndicator.symptom(), is("Data streams are executing their lifecycles without issues"));
                assertThat(dslIndicator.diagnosisList().size(), is(0));
            }
        } finally {
            // when the test executes successfully this will not be needed however, otherwise we need to make sure the index is
            // "delete-able" for test cleanup
            try {
                updateIndexSettings(Settings.builder().put(READ_ONLY.settingName(), false), firstGenerationIndex);
            } catch (Exception e) {
                // index would be deleted if the test is successful
            }
        }
    }

    public void testDataLifecycleServiceConfiguresTheMergePolicy() throws Exception {
        DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.Template.DATA_DEFAULT;

        putComposableIndexTemplate(
            "id1",
            null,
            List.of("metrics-foo*"),
            Settings.builder().put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1").build(),
            null,
            lifecycle,
            false
        );

        String dataStreamName = "metrics-foo";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            dataStreamName
        );
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        indexDocs(dataStreamName, 1);

        // let's allow one rollover to go through
        List<String> backingIndices = waitForDataStreamBackingIndices(dataStreamName, 2);
        String firstGenerationIndex = backingIndices.get(0);
        assertThat(firstGenerationIndex, backingIndexEqualTo(dataStreamName, 1));
        String writeIndex = backingIndices.get(1);
        assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 2));

        ClusterGetSettingsAction.Response response = client().execute(
            ClusterGetSettingsAction.INSTANCE,
            new ClusterGetSettingsAction.Request(TEST_REQUEST_TIMEOUT)
        ).get();
        Settings clusterSettings = response.persistentSettings();

        Integer targetFactor = DATA_STREAM_MERGE_POLICY_TARGET_FACTOR_SETTING.get(clusterSettings);
        ByteSizeValue targetFloor = DATA_STREAM_MERGE_POLICY_TARGET_FLOOR_SEGMENT_SETTING.get(clusterSettings);

        assertBusy(() -> {
            GetSettingsRequest getSettingsRequest = new GetSettingsRequest(TEST_REQUEST_TIMEOUT).indices(firstGenerationIndex)
                .includeDefaults(true);
            GetSettingsResponse getSettingsResponse = client().execute(GetSettingsAction.INSTANCE, getSettingsRequest).actionGet();
            assertThat(
                getSettingsResponse.getSetting(firstGenerationIndex, MergePolicyConfig.INDEX_MERGE_POLICY_MERGE_FACTOR_SETTING.getKey()),
                is(targetFactor.toString())
            );
            assertThat(
                getSettingsResponse.getSetting(firstGenerationIndex, MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING.getKey()),
                is(targetFloor.getStringRep())
            );
        });

        // let's configure the data stream lifecycle service to configure a different merge policy for indices
        updateClusterSettings(
            Settings.builder()
                .put(DATA_STREAM_MERGE_POLICY_TARGET_FACTOR_SETTING.getKey(), 5)
                .put(DATA_STREAM_MERGE_POLICY_TARGET_FLOOR_SEGMENT_SETTING.getKey(), ByteSizeValue.ofMb(5))
        );

        // rollover to assert the second generation is configured with the new setting values (note that the first index _might_ pick up
        // the new settings as well if the data stream lifecycle runs often enough - every second in tests - and the index has not yet been
        // forcemerged)

        indexDocs(dataStreamName, 1);

        // let's allow one rollover to go through
        backingIndices = waitForDataStreamBackingIndices(dataStreamName, 3);
        String secondGenerationIndex = backingIndices.get(1);

        // check the 2nd generation index picked up the new setting values
        assertBusy(() -> {
            GetSettingsRequest getSettingsRequest = new GetSettingsRequest(TEST_REQUEST_TIMEOUT).indices(secondGenerationIndex)
                .includeDefaults(true);
            GetSettingsResponse getSettingsResponse = client().execute(GetSettingsAction.INSTANCE, getSettingsRequest).actionGet();
            assertThat(
                getSettingsResponse.getSetting(secondGenerationIndex, MergePolicyConfig.INDEX_MERGE_POLICY_MERGE_FACTOR_SETTING.getKey()),
                is("5")
            );
            assertThat(
                getSettingsResponse.getSetting(secondGenerationIndex, MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING.getKey()),
                is(ByteSizeValue.ofMb(5).getStringRep())
            );
        });
    }

    public void testReenableDataStreamLifecycle() throws Exception {
        // start with a lifecycle that's not enabled
        DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.dataLifecycleBuilder().enabled(false).buildTemplate();

        putComposableIndexTemplate("id1", null, List.of("metrics-foo*"), null, null, lifecycle, false);
        String dataStreamName = "metrics-foo";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            dataStreamName
        );
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        indexDocs(dataStreamName, 10);
        List<String> backingIndices = getDataStreamBackingIndexNames(dataStreamName);
        {
            // backing index should not be managed
            String writeIndex = backingIndices.get(0);

            ExplainDataStreamLifecycleAction.Response dataStreamLifecycleExplainResponse = client().execute(
                ExplainDataStreamLifecycleAction.INSTANCE,
                new ExplainDataStreamLifecycleAction.Request(TEST_REQUEST_TIMEOUT, new String[] { writeIndex })
            ).actionGet();
            assertThat(dataStreamLifecycleExplainResponse.getIndices().size(), is(1));
            for (ExplainIndexDataStreamLifecycle index : dataStreamLifecycleExplainResponse.getIndices()) {
                assertThat(index.isManagedByLifecycle(), is(false));
            }
        }

        {
            // data stream has only one backing index
            assertThat(backingIndices.size(), equalTo(1));
            String writeIndex = backingIndices.get(0);
            assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 1));
        }

        // let's enable the data stream lifecycle and witness the rollover as we ingested 10 documents already

        client().execute(
            PutDataStreamLifecycleAction.INSTANCE,
            new PutDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStreamName },
                null,
                true
            )
        );

        List<String> currentBackingIndices = waitForDataStreamBackingIndices(dataStreamName, 2);
        String backingIndex = currentBackingIndices.get(0);
        assertThat(backingIndex, backingIndexEqualTo(dataStreamName, 1));
        String writeIndex = currentBackingIndices.get(1);
        assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 2));
    }

    public void testLifecycleAppliedToFailureStore() throws Exception {
        DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.failuresLifecycleBuilder()
            .dataRetention(TimeValue.timeValueSeconds(20))
            .buildTemplate();

        putComposableIndexTemplate("id1", """
            {
              "properties": {
                 "@timestamp": {
                   "type": "date",
                   "format": "epoch_millis"
                 },
                 "flag": {
                   "type": "boolean"
                 }
             }
            }""", List.of("metrics-fs*"), Settings.builder().put("index.number_of_replicas", 0).build(), null, null, lifecycle, true);

        String dataStreamName = "metrics-fs";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            dataStreamName
        );
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        indexInvalidFlagDocs(dataStreamName, 1);

        // Let's verify the rollover
        List<String> failureIndices = waitForDataStreamIndices(dataStreamName, 2, true);
        String firstGenerationIndex = failureIndices.get(0);
        String secondGenerationIndex = failureIndices.get(1);

        // Let's verify the merge settings
        ClusterGetSettingsAction.Response response = client().execute(
            ClusterGetSettingsAction.INSTANCE,
            new ClusterGetSettingsAction.Request(TEST_REQUEST_TIMEOUT)
        ).get();
        Settings clusterSettings = response.persistentSettings();

        Integer targetFactor = DATA_STREAM_MERGE_POLICY_TARGET_FACTOR_SETTING.get(clusterSettings);
        ByteSizeValue targetFloor = DATA_STREAM_MERGE_POLICY_TARGET_FLOOR_SEGMENT_SETTING.get(clusterSettings);

        assertBusy(() -> {
            GetSettingsRequest getSettingsRequest = new GetSettingsRequest(TEST_REQUEST_TIMEOUT).indices(firstGenerationIndex)
                .includeDefaults(true);
            GetSettingsResponse getSettingsResponse = client().execute(GetSettingsAction.INSTANCE, getSettingsRequest).actionGet();
            assertThat(
                getSettingsResponse.getSetting(firstGenerationIndex, MergePolicyConfig.INDEX_MERGE_POLICY_MERGE_FACTOR_SETTING.getKey()),
                is(targetFactor.toString())
            );
            assertThat(
                getSettingsResponse.getSetting(firstGenerationIndex, MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING.getKey()),
                is(targetFloor.getStringRep())
            );
        });

        updateFailureStoreConfiguration(dataStreamName, true, TimeValue.timeValueSeconds(1));

        // And finally apply retention
        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStreamName }
            );
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getName(), equalTo(dataStreamName));
            List<Index> backingIndices = getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices();
            assertThat(backingIndices.size(), equalTo(1));
            List<Index> retrievedFailureIndices = getDataStreamResponse.getDataStreams().get(0).getDataStream().getFailureIndices();
            assertThat(retrievedFailureIndices.size(), equalTo(1));
            assertThat(retrievedFailureIndices.get(0).getName(), equalTo(secondGenerationIndex));
        });
    }

    static void indexDocs(String dataStream, int numDocs) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            String value = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
            bulkRequest.add(
                new IndexRequest(dataStream).opType(DocWriteRequest.OpType.CREATE)
                    .source(String.format(Locale.ROOT, "{\"%s\":\"%s\"}", DEFAULT_TIMESTAMP_FIELD, value), XContentType.JSON)
            );
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.getItems().length, equalTo(numDocs));
        String backingIndexPrefix = DataStream.BACKING_INDEX_PREFIX + dataStream;
        for (BulkItemResponse itemResponse : bulkResponse) {
            assertThat(itemResponse.getFailureMessage(), nullValue());
            assertThat(itemResponse.status(), equalTo(RestStatus.CREATED));
            assertThat(itemResponse.getIndex(), startsWith(backingIndexPrefix));
        }
        indicesAdmin().refresh(new RefreshRequest(dataStream)).actionGet();
    }

    static void indexInvalidFlagDocs(String dataStream, int numDocs) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            String value = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
            bulkRequest.add(
                new IndexRequest(dataStream).opType(DocWriteRequest.OpType.CREATE)
                    .source(
                        String.format(Locale.ROOT, "{\"%s\":\"%s\",\"flag\":\"invalid\"}", DEFAULT_TIMESTAMP_FIELD, value),
                        XContentType.JSON
                    )
            );
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.getItems().length, equalTo(numDocs));
        String failureIndexPrefix = DataStream.FAILURE_STORE_PREFIX + dataStream;
        for (BulkItemResponse itemResponse : bulkResponse) {
            assertThat(itemResponse.getFailureMessage(), nullValue());
            assertThat(itemResponse.status(), equalTo(RestStatus.CREATED));
            assertThat(itemResponse.getIndex(), startsWith(failureIndexPrefix));
        }
        indicesAdmin().refresh(new RefreshRequest(dataStream)).actionGet();
    }

    static void putComposableIndexTemplate(
        String id,
        @Nullable String mappings,
        List<String> patterns,
        @Nullable Settings settings,
        @Nullable Map<String, Object> metadata,
        @Nullable DataStreamLifecycle.Template dataLifecycle,
        boolean withFailureStore
    ) throws IOException {
        putComposableIndexTemplate(id, mappings, patterns, settings, metadata, dataLifecycle, null, withFailureStore);
    }

    static void putComposableIndexTemplate(
        String id,
        @Nullable String mappings,
        List<String> patterns,
        @Nullable Settings settings,
        @Nullable Map<String, Object> metadata,
        @Nullable DataStreamLifecycle.Template dataLifecycle,
        @Nullable DataStreamLifecycle.Template failuresLifecycle,
        boolean withFailureStore
    ) throws IOException {
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(id);
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(patterns)
                .template(
                    Template.builder()
                        .settings(settings)
                        .mappings(mappings == null ? null : CompressedXContent.fromJSON(mappings))
                        .lifecycle(dataLifecycle)
                        .dataStreamOptions(
                            new DataStreamOptions.Template(new DataStreamFailureStore.Template(withFailureStore, failuresLifecycle))
                        )
                )
                .metadata(metadata)
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();
    }

    static void updateLifecycle(String dataStreamName, TimeValue dataRetention) {
        PutDataStreamLifecycleAction.Request putDataLifecycleRequest = new PutDataStreamLifecycleAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            new String[] { dataStreamName },
            dataRetention
        );
        assertAcked(client().execute(PutDataStreamLifecycleAction.INSTANCE, putDataLifecycleRequest));
    }

    static void updateFailureStoreConfiguration(String dataStreamName, boolean enabled, TimeValue retention) {
        PutDataStreamOptionsAction.Request putDataOptionsRequest = new PutDataStreamOptionsAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            new String[] { dataStreamName },
            new DataStreamFailureStore(enabled, DataStreamLifecycle.failuresLifecycleBuilder().dataRetention(retention).build())
        );
        assertAcked(client().execute(PutDataStreamOptionsAction.INSTANCE, putDataOptionsRequest));
    }

    /*
     * This test plugin adds `.system-test` as a known system data stream. The data stream is not created by this plugin. But if it is
     * created, it will be a system data stream.
     */
    public static class TestSystemDataStreamPlugin extends Plugin implements SystemIndexPlugin {
        public static final String SYSTEM_DATA_STREAM_NAME = ".system-test";
        public static final int SYSTEM_DATA_STREAM_RETENTION_DAYS = 100;

        @Override
        public String getFeatureName() {
            return "test";
        }

        @Override
        public String getFeatureDescription() {
            return "test";
        }

        @Override
        public Collection<SystemDataStreamDescriptor> getSystemDataStreamDescriptors() {
            return List.of(
                new SystemDataStreamDescriptor(
                    SYSTEM_DATA_STREAM_NAME,
                    "test",
                    SystemDataStreamDescriptor.Type.INTERNAL,
                    ComposableIndexTemplate.builder()
                        .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                        .indexPatterns(List.of(DataStream.BACKING_INDEX_PREFIX + SYSTEM_DATA_STREAM_NAME + "*"))
                        .template(
                            Template.builder()
                                .settings(Settings.EMPTY)
                                .lifecycle(
                                    DataStreamLifecycle.dataLifecycleBuilder()
                                        .dataRetention(TimeValue.timeValueDays(SYSTEM_DATA_STREAM_RETENTION_DAYS))
                                )
                        )
                        .build(),
                    Map.of(),
                    List.of(),
                    "test",
                    ExecutorNames.DEFAULT_SYSTEM_INDEX_THREAD_POOLS
                )
            );
        }
    }
}
