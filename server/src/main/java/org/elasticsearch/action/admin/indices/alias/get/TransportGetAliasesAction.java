/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.alias.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.SystemIndices.SystemIndexAccessLevel;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.Transports;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Predicate;

/**
 * NB prior to 8.12 this was a TransportMasterNodeReadAction so for BwC it must be registered with the TransportService (i.e. a
 * HandledTransportAction) until we no longer need to support {@link org.elasticsearch.TransportVersions#CLUSTER_FEATURES_ADDED} and
 * earlier.
 */
public class TransportGetAliasesAction extends HandledTransportAction<GetAliasesRequest, GetAliasesResponse> {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(TransportGetAliasesAction.class);

    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final SystemIndices systemIndices;
    private final Executor managementExecutor;
    private final ThreadContext threadContext;

    @Inject
    public TransportGetAliasesAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SystemIndices systemIndices
    ) {
        // TODO replace DIRECT_EXECUTOR_SERVICE when removing workaround for https://github.com/elastic/elasticsearch/issues/97916
        super(GetAliasesAction.NAME, transportService, actionFilters, GetAliasesRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.systemIndices = systemIndices;
        this.managementExecutor = clusterService.threadPool().executor(ThreadPool.Names.MANAGEMENT);
        this.threadContext = clusterService.threadPool().getThreadContext();
    }

    private void ensureNotBlocked(GetAliasesRequest request, ClusterState state) {
        // Resolve with system index access since we're just checking blocks
        final var clusterBlockException = state.blocks()
            .indicesBlockedException(
                ClusterBlockLevel.METADATA_READ,
                indexNameExpressionResolver.concreteIndexNamesWithSystemIndexAccess(state, request)
            );
        if (clusterBlockException != null) {
            throw clusterBlockException;
        }
    }

    @Override
    protected void doExecute(Task task, GetAliasesRequest request, ActionListener<GetAliasesResponse> listener) {
        // Workaround for https://github.com/elastic/elasticsearch/issues/97916 - TODO remove this when we can
        managementExecutor.execute(ActionRunnable.wrap(listener, l -> doExecuteForked((CancellableTask) task, request, l)));
    }

    private void doExecuteForked(CancellableTask task, GetAliasesRequest request, ActionListener<GetAliasesResponse> listener) {
        assert Transports.assertNotTransportThread("no need to avoid the context switch and may be expensive if there are many aliases");
        task.ensureNotCancelled();
        final var state = clusterService.state();
        ensureNotBlocked(request, state);
        // resolve all concrete indices upfront and warn/error later
        final String[] concreteIndices = indexNameExpressionResolver.concreteIndexNamesWithSystemIndexAccess(state, request);
        final SystemIndexAccessLevel systemIndexAccessLevel = indexNameExpressionResolver.getSystemIndexAccessLevel();
        Map<String, List<AliasMetadata>> aliases = state.metadata().findAliases(request.aliases(), concreteIndices);
        task.ensureNotCancelled();
        listener.onResponse(
            new GetAliasesResponse(
                postProcess(request, concreteIndices, aliases, state, systemIndexAccessLevel, threadContext, systemIndices),
                postProcess(indexNameExpressionResolver, request, state)
            )
        );
    }

    /**
     * Fills alias result with empty entries for requested indices when no specific aliases were requested.
     */
    static Map<String, List<AliasMetadata>> postProcess(
        GetAliasesRequest request,
        String[] concreteIndices,
        Map<String, List<AliasMetadata>> aliases,
        ClusterState state,
        SystemIndexAccessLevel systemIndexAccessLevel,
        ThreadContext threadContext,
        SystemIndices systemIndices
    ) {
        boolean noAliasesSpecified = request.getOriginalAliases() == null || request.getOriginalAliases().length == 0;
        Map<String, List<AliasMetadata>> mapBuilder = new HashMap<>(aliases);
        for (String index : concreteIndices) {
            IndexAbstraction ia = state.metadata().getIndicesLookup().get(index);
            assert ia.getType() == IndexAbstraction.Type.CONCRETE_INDEX;
            if (ia.getParentDataStream() != null) {
                // Don't include backing indices of data streams,
                // because it is just noise. Aliases can't refer
                // to backing indices directly.
                continue;
            }

            if (aliases.get(index) == null && noAliasesSpecified) {
                List<AliasMetadata> previous = mapBuilder.put(index, Collections.emptyList());
                assert previous == null;
            }
        }
        final Map<String, List<AliasMetadata>> finalResponse = Collections.unmodifiableMap(mapBuilder);
        if (systemIndexAccessLevel != SystemIndexAccessLevel.ALL) {
            checkSystemIndexAccess(systemIndices, state, finalResponse, systemIndexAccessLevel, threadContext);
        }
        return finalResponse;
    }

    static Map<String, List<DataStreamAlias>> postProcess(
        IndexNameExpressionResolver resolver,
        GetAliasesRequest request,
        ClusterState state
    ) {
        Map<String, List<DataStreamAlias>> result = new HashMap<>();
        boolean noAliasesSpecified = request.getOriginalAliases() == null || request.getOriginalAliases().length == 0;
        List<String> requestedDataStreams = resolver.dataStreamNames(state, request.indicesOptions(), request.indices());
        for (String requestedDataStream : requestedDataStreams) {
            List<DataStreamAlias> aliases = state.metadata()
                .dataStreamAliases()
                .values()
                .stream()
                .filter(alias -> alias.getDataStreams().contains(requestedDataStream))
                .filter(alias -> noAliasesSpecified || Regex.simpleMatch(request.aliases(), alias.getName()))
                .toList();
            if (aliases.isEmpty() == false) {
                result.put(requestedDataStream, aliases);
            }
        }
        return result;
    }

    private static void checkSystemIndexAccess(
        SystemIndices systemIndices,
        ClusterState state,
        Map<String, List<AliasMetadata>> aliasesMap,
        SystemIndexAccessLevel systemIndexAccessLevel,
        ThreadContext threadContext
    ) {
        final Predicate<String> systemIndexAccessAllowPredicate;
        if (systemIndexAccessLevel == SystemIndexAccessLevel.NONE) {
            systemIndexAccessAllowPredicate = indexName -> false;
        } else if (systemIndexAccessLevel == SystemIndexAccessLevel.RESTRICTED) {
            systemIndexAccessAllowPredicate = systemIndices.getProductSystemIndexNamePredicate(threadContext);
        } else {
            throw new IllegalArgumentException("Unexpected system index access level: " + systemIndexAccessLevel);
        }

        List<String> netNewSystemIndices = new ArrayList<>();
        List<String> systemIndicesNames = new ArrayList<>();
        aliasesMap.keySet().forEach(indexName -> {
            IndexMetadata index = state.metadata().index(indexName);
            if (index != null && index.isSystem()) {
                if (systemIndexAccessAllowPredicate.test(indexName) == false) {
                    if (systemIndices.isNetNewSystemIndex(indexName)) {
                        netNewSystemIndices.add(indexName);
                    } else {
                        systemIndicesNames.add(indexName);
                    }
                }
            }
        });
        if (systemIndicesNames.isEmpty() == false) {
            deprecationLogger.warn(
                DeprecationCategory.API,
                "open_system_index_access",
                "this request accesses system indices: {}, but in a future major version, direct access to system "
                    + "indices will be prevented by default",
                systemIndicesNames
            );
        }
        if (netNewSystemIndices.isEmpty() == false) {
            throw SystemIndices.netNewSystemIndexAccessException(threadContext, netNewSystemIndices);
        }
    }
}
