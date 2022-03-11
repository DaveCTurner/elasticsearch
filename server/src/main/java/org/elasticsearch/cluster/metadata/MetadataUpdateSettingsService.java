/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsClusterStateUpdateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.ShardLimitValidator;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.BiFunction;

import static org.elasticsearch.index.IndexSettings.same;

/**
 * Service responsible for submitting update index settings requests
 */
public class MetadataUpdateSettingsService {
    private static final Logger logger = LogManager.getLogger(MetadataUpdateSettingsService.class);

    private final ClusterService clusterService;
    private final AllocationService allocationService;
    private final IndexScopedSettings indexScopedSettings;
    private final IndicesService indicesService;
    private final ShardLimitValidator shardLimitValidator;
    private final MetadataUpdateSettingsTaskExecutor executor;

    public MetadataUpdateSettingsService(
        ClusterService clusterService,
        AllocationService allocationService,
        IndexScopedSettings indexScopedSettings,
        IndicesService indicesService,
        ShardLimitValidator shardLimitValidator
    ) {
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.indexScopedSettings = indexScopedSettings;
        this.indicesService = indicesService;
        this.shardLimitValidator = shardLimitValidator;
        this.executor = new MetadataUpdateSettingsTaskExecutor();
    }

    public void updateSettings(final UpdateSettingsClusterStateUpdateRequest request, final ActionListener<AcknowledgedResponse> listener) {
        final Settings normalizedSettings = Settings.builder()
            .put(request.settings())
            .normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX)
            .build();
        Settings.Builder settingsForClosedIndices = Settings.builder();
        Settings.Builder settingsForOpenIndices = Settings.builder();
        final Set<String> skippedSettings = new HashSet<>();

        indexScopedSettings.validate(
            normalizedSettings.filter(s -> Regex.isSimpleMatchPattern(s) == false), // don't validate wildcards
            false, // don't validate values here we check it below never allow to change the number of shards
            true
        ); // validate internal or private index settings
        for (String key : normalizedSettings.keySet()) {
            Setting<?> setting = indexScopedSettings.get(key);
            boolean isWildcard = setting == null && Regex.isSimpleMatchPattern(key);
            assert setting != null // we already validated the normalized settings
                || (isWildcard && normalizedSettings.hasValue(key) == false)
                : "unknown setting: " + key + " isWildcard: " + isWildcard + " hasValue: " + normalizedSettings.hasValue(key);
            settingsForClosedIndices.copy(key, normalizedSettings);
            if (isWildcard || setting.isDynamic()) {
                settingsForOpenIndices.copy(key, normalizedSettings);
            } else {
                skippedSettings.add(key);
            }
        }
        final Settings closedSettings = settingsForClosedIndices.build();
        final Settings openSettings = settingsForOpenIndices.build();
        final boolean preserveExisting = request.isPreserveExisting();

        // assertions supporting refactoring: we used to look up these settings in normalizedSettings but they're dynamic so that's
        // equivalent to looking at openSettings
        assert IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.exists(
            normalizedSettings
        ) == IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.exists(openSettings);
        assert IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.exists(
            normalizedSettings
        ) == IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.exists(openSettings);

        clusterService.submitStateUpdateTask(
            "update-settings " + Arrays.toString(request.indices()),
            new MetadataUpdateSettingsTask(
                request.indices(),
                openSettings,
                closedSettings,
                skippedSettings,
                preserveExisting,
                request.ackTimeout(),
                listener
            ),
            ClusterStateTaskConfig.build(Priority.URGENT, request.masterNodeTimeout()),
            this.executor
        );
    }

    public static void updateIndexSettings(
        Set<Index> indices,
        Metadata.Builder metadataBuilder,
        BiFunction<Index, Settings.Builder, Boolean> settingUpdater,
        Boolean preserveExisting,
        IndexScopedSettings indexScopedSettings
    ) {
        for (Index index : indices) {
            IndexMetadata indexMetadata = metadataBuilder.getSafe(index);
            Settings.Builder indexSettings = Settings.builder().put(indexMetadata.getSettings());
            if (settingUpdater.apply(index, indexSettings)) {
                if (preserveExisting) {
                    indexSettings.put(indexMetadata.getSettings());
                }
                /*
                 * The setting index.number_of_replicas is special; we require that this setting has a value
                 * in the index. When creating the index, we ensure this by explicitly providing a value for
                 * the setting to the default (one) if there is a not value provided on the source of the
                 * index creation. A user can update this setting though, including updating it to null,
                 * indicating that they want to use the default value. In this case, we again have to
                 * provide an explicit value for the setting to the default (one).
                 */
                if (IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.exists(indexSettings) == false) {
                    indexSettings.put(
                        IndexMetadata.SETTING_NUMBER_OF_REPLICAS,
                        IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(Settings.EMPTY)
                    );
                }
                Settings finalSettings = indexSettings.build();
                indexScopedSettings.validate(finalSettings.filter(k -> indexScopedSettings.isPrivateSetting(k) == false), true);
                metadataBuilder.put(IndexMetadata.builder(indexMetadata).settings(finalSettings));
            }
        }
    }

    private record MetadataUpdateSettingsTask(
        Index[] indices,
        Settings openSettings,
        Settings closedSettings,
        Set<String> skippedSettings,
        boolean preserveExisting,
        TimeValue ackTimeout,
        ActionListener<AcknowledgedResponse> listener
    ) implements ClusterStateAckListener, ClusterStateTaskListener {

        private Index[] getIndices() {
            return indices;
        }

        @Override
        public boolean mustAck(DiscoveryNode discoveryNode) {
            return true;
        }

        @Override
        public void onAllNodesAcked(@Nullable Exception e) {
            listener.onResponse(AcknowledgedResponse.of(e == null));
        }

        @Override
        public void onAckTimeout() {
            listener.onResponse(AcknowledgedResponse.of(false));
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    private class MetadataUpdateSettingsTaskExecutor implements ClusterStateTaskExecutor<MetadataUpdateSettingsTask> {
        @Override
        public ClusterState execute(ClusterState currentState, List<TaskContext<MetadataUpdateSettingsTask>> taskContexts)
            throws Exception {
            ClusterState state = currentState;
            for (final var taskContext : taskContexts) {
                try {
                    final var task = taskContext.getTask();

                    RoutingTable.Builder routingTableBuilder = null;
                    Metadata.Builder metadataBuilder = Metadata.builder(state.metadata());

                    // allow to change any settings to a closed index, and only allow dynamic settings to be changed
                    // on an open index
                    Set<Index> openIndices = new HashSet<>();
                    Set<Index> closedIndices = new HashSet<>();
                    final var indices = task.getIndices();
                    final String[] actualIndices = new String[indices.length];
                    for (int i = 0; i < indices.length; i++) {
                        Index index = indices[i];
                        actualIndices[i] = index.getName();
                        final IndexMetadata metadata = metadataBuilder.getSafe(index);
                        if (metadata.getState() == IndexMetadata.State.OPEN) {
                            openIndices.add(index);
                        } else {
                            closedIndices.add(index);
                        }
                    }

                    if (task.skippedSettings.isEmpty() == false && openIndices.isEmpty() == false) {
                        throw new IllegalArgumentException(
                            String.format(
                                Locale.ROOT,
                                "Can't update non dynamic settings [%s] for open indices %s",
                                task.skippedSettings,
                                openIndices
                            )
                        );
                    }

                    if (task.preserveExisting == false && IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.exists(task.openSettings)) {
                        final int updatedNumberOfReplicas = IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(task.openSettings);

                        // Verify that this won't take us over the cluster shard limit.
                        shardLimitValidator.validateShardLimitOnReplicaUpdate(state, indices, updatedNumberOfReplicas);

                        /*
                         * We do not update the in-sync allocation IDs as they will be removed upon the first index operation
                         * which makes these copies stale.
                         *
                         * TODO: should we update the in-sync allocation IDs once the data is deleted by the node?
                         */
                        routingTableBuilder = RoutingTable.builder(state.routingTable());
                        routingTableBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
                        metadataBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
                        logger.info("updating number_of_replicas to [{}] for indices {}", updatedNumberOfReplicas, actualIndices);
                    }

                    updateIndexSettings(
                        openIndices,
                        metadataBuilder,
                        (index, indexSettings) -> indexScopedSettings.updateDynamicSettings(
                            task.openSettings,
                            indexSettings,
                            Settings.builder(),
                            index.getName()
                        ),
                        task.preserveExisting,
                        indexScopedSettings
                    );

                    updateIndexSettings(
                        closedIndices,
                        metadataBuilder,
                        (index, indexSettings) -> indexScopedSettings.updateSettings(
                            task.closedSettings,
                            indexSettings,
                            Settings.builder(),
                            index.getName()
                        ),
                        task.preserveExisting,
                        indexScopedSettings
                    );

                    if (IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.exists(task.openSettings)
                        || IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.exists(task.openSettings)) {
                        for (String index : actualIndices) {
                            final Settings settings = metadataBuilder.get(index).getSettings();
                            MetadataCreateIndexService.validateTranslogRetentionSettings(settings);
                            MetadataCreateIndexService.validateStoreTypeSetting(settings);
                        }
                    }
                    boolean changed = false;
                    // increment settings versions
                    for (final String index : actualIndices) {
                        if (same(state.metadata().index(index).getSettings(), metadataBuilder.get(index).getSettings()) == false) {
                            changed = true;
                            final IndexMetadata.Builder builder = IndexMetadata.builder(metadataBuilder.get(index));
                            builder.settingsVersion(1 + builder.settingsVersion());
                            metadataBuilder.put(builder);
                        }
                    }

                    final ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(state.blocks());
                    boolean changedBlocks = false;
                    for (final var block : IndexMetadata.APIBlock.values()) {
                        if (block.setting.exists(task.openSettings)) {
                            final boolean updateBlock = block.setting.get(task.openSettings);
                            for (String index : actualIndices) {
                                if (updateBlock) {
                                    if (blocks.hasIndexBlock(index, block.block) == false) {
                                        blocks.addIndexBlock(index, block.block);
                                        changedBlocks = true;
                                    }
                                } else {
                                    if (blocks.hasIndexBlock(index, block.block)) {
                                        blocks.removeIndexBlock(index, block.block);
                                        changedBlocks = true;
                                    }
                                }
                            }
                        }
                    }
                    changed |= changedBlocks;

                    if (changed) {
                        ClusterState updatedState = ClusterState.builder(state)
                            .metadata(metadataBuilder)
                            .routingTable(routingTableBuilder == null ? state.routingTable() : routingTableBuilder.build())
                            .blocks(changedBlocks ? blocks.build() : state.blocks())
                            .build();
                        try {
                            for (Index index : openIndices) {
                                final IndexMetadata currentMetadata = state.metadata().getIndexSafe(index);
                                final IndexMetadata updatedMetadata = updatedState.metadata().getIndexSafe(index);
                                indicesService.verifyIndexMetadata(currentMetadata, updatedMetadata);
                            }
                            for (Index index : closedIndices) {
                                final IndexMetadata currentMetadata = state.metadata().getIndexSafe(index);
                                final IndexMetadata updatedMetadata = updatedState.metadata().getIndexSafe(index);
                                // Verifies that the current index settings can be updated with the updated dynamic settings.
                                indicesService.verifyIndexMetadata(currentMetadata, updatedMetadata);
                                // Now check that we can create the index with the updated settings (dynamic and non-dynamic).
                                // This step is mandatory since we allow to update non-dynamic settings on closed indices.
                                indicesService.verifyIndexMetadata(updatedMetadata, updatedMetadata);
                            }
                        } catch (IOException ex) {
                            throw ExceptionsHelper.convertToElastic(ex);
                        }
                        state = updatedState;
                    }

                    taskContext.success(new LegacyClusterTaskResultActionListener(task, currentState), task);
                } catch (Exception e) {
                    taskContext.onFailure(e);
                }
            }
            if (state != currentState) {
                // reroute in case things change that require it (like number of replicas)
                state = allocationService.reroute(state, "settings update");
            }
            return state;
        }

    }

}
