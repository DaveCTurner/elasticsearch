/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.repositories.get.TransportGetRepositoriesAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.repositories.GetSnapshotInfoContext;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

/**
 * Transport Action for get snapshots operation
 */
public class TransportGetSnapshotsAction extends TransportMasterNodeAction<GetSnapshotsRequest, GetSnapshotsResponse> {

    private final RepositoriesService repositoriesService;

    @Inject
    public TransportGetSnapshotsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        RepositoriesService repositoriesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetSnapshotsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetSnapshotsRequest::new,
            indexNameExpressionResolver,
            GetSnapshotsResponse::new,
            // Execute this on the management pool because creating the response can become fairly expensive
            // for large repositories in the verbose=false case when there are a lot of indices per snapshot.
            // This is intentionally not using the snapshot_meta pool because that pool is sized rather large
            // to accommodate concurrent IO and could consume excessive CPU resources through concurrent
            // verbose=false requests that are CPU bound only.
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected ClusterBlockException checkBlock(GetSnapshotsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void masterOperation(
        Task task,
        final GetSnapshotsRequest request,
        final ClusterState state,
        final ActionListener<GetSnapshotsResponse> listener
    ) {
        assert task instanceof CancellableTask : task + " not cancellable";

        final var repositoriesResult = TransportGetRepositoriesAction.getRepositories(state, request.repositories());

        new GetSnapshotsOperation(
            (CancellableTask) task,
            request.snapshots(),
            request.ignoreUnavailable(),
            SnapshotPredicates.fromRequest(request),
            request.sort(),
            request.order(),
            request.fromSortValue(),
            request.after(),
            request.offset(),
            request.size(),
            request.verbose(),
            request.includeIndexNames(),
            SnapshotsInProgress.get(state)
        ).start(repositoriesResult.metadata(), repositoriesResult.missing(), request.isSingleRepositoryRequest() == false, listener);
    }

    /**
     * A pair of predicates for the get snapshots action. The {@link #test(SnapshotId, RepositoryData)} predicate is applied to combinations
     * of snapshot id and repository data to determine which snapshots to fully load from the repository and rules out all snapshots that do
     * not match the given {@link GetSnapshotsRequest} that can be ruled out through the information in {@link RepositoryData}.
     * The predicate returned by {@link #test(SnapshotInfo)} predicate is then applied the instances of {@link SnapshotInfo} that were
     * loaded from the repository to filter out those remaining that did not match the request but could not be ruled out without loading
     * their {@link SnapshotInfo}.
     */
    private static final class SnapshotPredicates {

        private static final SnapshotPredicates MATCH_ALL = new SnapshotPredicates(null, null);

        @Nullable // if all snapshot IDs match
        private final BiPredicate<SnapshotId, RepositoryData> preflightPredicate;

        @Nullable // if all snapshots match
        private final Predicate<SnapshotInfo> snapshotPredicate;

        private SnapshotPredicates(
            @Nullable BiPredicate<SnapshotId, RepositoryData> preflightPredicate,
            @Nullable Predicate<SnapshotInfo> snapshotPredicate
        ) {
            this.snapshotPredicate = snapshotPredicate;
            this.preflightPredicate = preflightPredicate;
        }

        boolean test(SnapshotId snapshotId, RepositoryData repositoryData) {
            return preflightPredicate == null || preflightPredicate.test(snapshotId, repositoryData);
        }

        boolean isMatchAll() {
            return snapshotPredicate == null;
        }

        boolean test(SnapshotInfo snapshotInfo) {
            return snapshotPredicate == null || snapshotPredicate.test(snapshotInfo);
        }

        private SnapshotPredicates and(SnapshotPredicates other) {
            return this == MATCH_ALL ? other
                : other == MATCH_ALL ? this
                : new SnapshotPredicates(
                    preflightPredicate == null ? other.preflightPredicate : other.preflightPredicate == null ? preflightPredicate : null,
                    snapshotPredicate == null ? other.snapshotPredicate : other.snapshotPredicate == null ? snapshotPredicate : null
                );
        }

        static SnapshotPredicates fromRequest(GetSnapshotsRequest request) {
            return getSortValuePredicate(request.fromSortValue(), request.sort(), request.order()).and(
                getSlmPredicates(request.policies())
            );
        }

        private static SnapshotPredicates getSlmPredicates(String[] slmPolicies) {
            if (slmPolicies.length == 0) {
                return MATCH_ALL;
            }

            final List<String> includePatterns = new ArrayList<>();
            final List<String> excludePatterns = new ArrayList<>();
            boolean seenWildcard = false;
            boolean matchNoPolicy = false;
            for (String slmPolicy : slmPolicies) {
                if (seenWildcard && slmPolicy.length() > 1 && slmPolicy.startsWith("-")) {
                    excludePatterns.add(slmPolicy.substring(1));
                } else {
                    if (Regex.isSimpleMatchPattern(slmPolicy)) {
                        seenWildcard = true;
                    } else if (GetSnapshotsRequest.NO_POLICY_PATTERN.equals(slmPolicy)) {
                        matchNoPolicy = true;
                    }
                    includePatterns.add(slmPolicy);
                }
            }
            final String[] includes = includePatterns.toArray(Strings.EMPTY_ARRAY);
            final String[] excludes = excludePatterns.toArray(Strings.EMPTY_ARRAY);
            final boolean matchWithoutPolicy = matchNoPolicy;
            return new SnapshotPredicates(((snapshotId, repositoryData) -> {
                final RepositoryData.SnapshotDetails details = repositoryData.getSnapshotDetails(snapshotId);
                final String policy;
                if (details == null || (details.getSlmPolicy() == null)) {
                    // no SLM policy recorded
                    return true;
                } else {
                    final String policyFound = details.getSlmPolicy();
                    // empty string means that snapshot was not created by an SLM policy
                    policy = policyFound.isEmpty() ? null : policyFound;
                }
                return matchPolicy(includes, excludes, matchWithoutPolicy, policy);
            }), snapshotInfo -> {
                final Map<String, Object> metadata = snapshotInfo.userMetadata();
                final String policy;
                if (metadata == null) {
                    policy = null;
                } else {
                    final Object policyFound = metadata.get(SnapshotsService.POLICY_ID_METADATA_FIELD);
                    policy = policyFound instanceof String ? (String) policyFound : null;
                }
                return matchPolicy(includes, excludes, matchWithoutPolicy, policy);
            });
        }

        private static boolean matchPolicy(String[] includes, String[] excludes, boolean matchWithoutPolicy, @Nullable String policy) {
            if (policy == null) {
                return matchWithoutPolicy;
            }
            if (Regex.simpleMatch(includes, policy) == false) {
                return false;
            }
            return excludes.length == 0 || Regex.simpleMatch(excludes, policy) == false;
        }

        private static SnapshotPredicates getSortValuePredicate(String fromSortValue, GetSnapshotsRequest.SortBy sortBy, SortOrder order) {
            if (fromSortValue == null) {
                return MATCH_ALL;
            }

            switch (sortBy) {
                case START_TIME:
                    final long after = Long.parseLong(fromSortValue);
                    return new SnapshotPredicates(order == SortOrder.ASC ? (snapshotId, repositoryData) -> {
                        final long startTime = getStartTime(snapshotId, repositoryData);
                        return startTime == -1 || after <= startTime;
                    } : (snapshotId, repositoryData) -> {
                        final long startTime = getStartTime(snapshotId, repositoryData);
                        return startTime == -1 || after >= startTime;
                    }, filterByLongOffset(SnapshotInfo::startTime, after, order));

                case NAME:
                    return new SnapshotPredicates(
                        order == SortOrder.ASC
                            ? (snapshotId, repositoryData) -> fromSortValue.compareTo(snapshotId.getName()) <= 0
                            : (snapshotId, repositoryData) -> fromSortValue.compareTo(snapshotId.getName()) >= 0,
                        null
                    );

                case DURATION:
                    final long afterDuration = Long.parseLong(fromSortValue);
                    return new SnapshotPredicates(order == SortOrder.ASC ? (snapshotId, repositoryData) -> {
                        final long duration = getDuration(snapshotId, repositoryData);
                        return duration == -1 || afterDuration <= duration;
                    } : (snapshotId, repositoryData) -> {
                        final long duration = getDuration(snapshotId, repositoryData);
                        return duration == -1 || afterDuration >= duration;
                    }, filterByLongOffset(info -> info.endTime() - info.startTime(), afterDuration, order));

                case INDICES:
                    final int afterIndexCount = Integer.parseInt(fromSortValue);
                    return new SnapshotPredicates(
                        order == SortOrder.ASC
                            ? (snapshotId, repositoryData) -> afterIndexCount <= indexCount(snapshotId, repositoryData)
                            : (snapshotId, repositoryData) -> afterIndexCount >= indexCount(snapshotId, repositoryData),
                        null
                    );

                case REPOSITORY:
                    // already handled in #maybeFilterRepositories
                    return MATCH_ALL;

                case SHARDS:
                    return new SnapshotPredicates(
                        null,
                        filterByLongOffset(SnapshotInfo::totalShards, Integer.parseInt(fromSortValue), order)
                    );
                case FAILED_SHARDS:
                    return new SnapshotPredicates(
                        null,
                        filterByLongOffset(SnapshotInfo::failedShards, Integer.parseInt(fromSortValue), order)
                    );
                default:
                    throw new AssertionError("unexpected sort column [" + sortBy + "]");
            }
        }

        private static Predicate<SnapshotInfo> filterByLongOffset(ToLongFunction<SnapshotInfo> extractor, long after, SortOrder order) {
            return order == SortOrder.ASC ? info -> after <= extractor.applyAsLong(info) : info -> after >= extractor.applyAsLong(info);
        }

        private static long getDuration(SnapshotId snapshotId, RepositoryData repositoryData) {
            final RepositoryData.SnapshotDetails details = repositoryData.getSnapshotDetails(snapshotId);
            if (details == null) {
                return -1;
            }
            final long startTime = details.getStartTimeMillis();
            if (startTime == -1) {
                return -1;
            }
            final long endTime = details.getEndTimeMillis();
            if (endTime == -1) {
                return -1;
            }
            return endTime - startTime;
        }

        private static long getStartTime(SnapshotId snapshotId, RepositoryData repositoryData) {
            final RepositoryData.SnapshotDetails details = repositoryData.getSnapshotDetails(snapshotId);
            return details == null ? -1 : details.getStartTimeMillis();
        }

        private static int indexCount(SnapshotId snapshotId, RepositoryData repositoryData) {
            // TODO: this could be made more efficient by caching this number in RepositoryData
            int indexCount = 0;
            for (IndexId idx : repositoryData.getIndices().values()) {
                if (repositoryData.getSnapshots(idx).contains(snapshotId)) {
                    indexCount++;
                }
            }
            return indexCount;
        }
    }

    /**
     * A single invocation of the TransportGetSnapshotsAction
     */
    private class GetSnapshotsOperation {
        final CancellableTask cancellableTask;

        final boolean isMatchAll;
        final String firstPattern;
        final Set<String> exactNames;
        final String[] includePatterns;
        final String[] excludePatterns;
        final boolean includeCurrent;
        final boolean includeOnlyCurrent;

        final boolean ignoreUnavailable;
        final SnapshotPredicates predicates;
        final GetSnapshotsRequest.SortBy sortBy;
        final SortOrder order;
        @Nullable
        final String fromSortValue;
        final GetSnapshotsRequest.After after;
        final int offset;
        final int size;
        final boolean verbose;
        final boolean indices;
        final SnapshotsInProgress snapshotsInProgress;

        // Results
        final Map<String, ElasticsearchException> failuresByRepository = ConcurrentCollections.newConcurrentMap();
        final Queue<List<SnapshotInfo>> allSnapshotInfos = ConcurrentCollections.newQueue();
        final AtomicInteger remaining = new AtomicInteger();
        final AtomicInteger totalCount = new AtomicInteger();

        private GetSnapshotsOperation(
            CancellableTask cancellableTask,
            String[] snapshots,
            boolean ignoreUnavailable,
            SnapshotPredicates predicates,
            GetSnapshotsRequest.SortBy sortBy,
            SortOrder order,
            String fromSortValue,
            GetSnapshotsRequest.After after,
            int offset,
            int size,
            boolean verbose,
            boolean indices,
            SnapshotsInProgress snapshotsInProgress
        ) {
            this.snapshotsInProgress = snapshotsInProgress;
            this.ignoreUnavailable = ignoreUnavailable;
            this.verbose = verbose;
            this.cancellableTask = cancellableTask;
            this.sortBy = sortBy;
            this.after = after;
            this.offset = offset;
            this.size = size;
            this.order = order;
            this.fromSortValue = fromSortValue;
            this.predicates = predicates;
            this.indices = indices;

            this.isMatchAll = TransportGetRepositoriesAction.isMatchAll(snapshots);
            if (isMatchAll) {
                this.exactNames = null;
                this.firstPattern = null;
                this.includePatterns = null;
                this.excludePatterns = null;
                this.includeCurrent = false;
                this.includeOnlyCurrent = false;
            } else if (snapshots.length == 1 && GetSnapshotsRequest.CURRENT_SNAPSHOT.equalsIgnoreCase(snapshots[0])) {
                this.exactNames = Set.of();
                this.firstPattern = null;
                this.includePatterns = null;
                this.excludePatterns = null;
                this.includeCurrent = true;
                this.includeOnlyCurrent = true;
            } else {
                final var exactNamesBuilder = new HashSet<String>();
                final var includePatternsList = new ArrayList<String>(snapshots.length);
                final var excludePatternsList = new ArrayList<String>(snapshots.length);
                boolean hasCurrent = false;
                boolean seenWildcard = false;
                for (final var snapshotOrPattern : snapshots) {
                    if (seenWildcard && snapshotOrPattern.length() > 1 && snapshotOrPattern.startsWith("-")) {
                        excludePatternsList.add(snapshotOrPattern.substring(1));
                    } else {
                        if (Regex.isSimpleMatchPattern(snapshotOrPattern)) {
                            seenWildcard = true;
                            includePatternsList.add(snapshotOrPattern);
                        } else if (GetSnapshotsRequest.CURRENT_SNAPSHOT.equalsIgnoreCase(snapshotOrPattern)) {
                            hasCurrent = true;
                            seenWildcard = true;
                        } else {
                            exactNamesBuilder.add(snapshotOrPattern);
                        }
                    }
                }
                this.firstPattern = snapshots[0];
                this.exactNames = Collections.unmodifiableSet(exactNamesBuilder);
                this.includePatterns = includePatternsList.toArray(Strings.EMPTY_ARRAY);
                this.excludePatterns = excludePatternsList.toArray(Strings.EMPTY_ARRAY);
                this.includeCurrent = hasCurrent;
                this.includeOnlyCurrent = false;
            }
        }

        void start(
            List<RepositoryMetadata> repositories,
            List<String> missingRepositories,
            boolean isMultiRepoRequest,
            ActionListener<GetSnapshotsResponse> listener
        ) {
            for (final var missingRepo : missingRepositories) {
                failuresByRepository.put(missingRepo, new RepositoryMissingException(missingRepo));
            }

            SubscribableListener

                .<Void>newForked(allRepositoriesDoneListener -> {
                    try (var listeners = new RefCountingListener(allRepositoriesDoneListener)) {
                        for (final var repository : repositories) {
                            final var repoName = repository.name();
                            if (sortBy == GetSnapshotsRequest.SortBy.REPOSITORY && fromSortValue != null) {
                                /*
                                 * Filters the list of repositories from which a request will fetch snapshots in the special case of sorting
                                 * by repository name and having a non-null value for GetSnapshotsRequest#fromSortValue on the request to
                                 * exclude repositories outside the sort value range if possible.
                                 */
                                final var comparisonResult = fromSortValue.compareTo(repoName);
                                if (order == SortOrder.ASC) {
                                    if (comparisonResult > 0) {
                                        continue;
                                    }
                                } else {
                                    if (comparisonResult < 0) {
                                        continue;
                                    }
                                }
                            }
                            processRepository(repoName, listeners.acquire((SnapshotsInRepo snapshotsInRepo) -> {
                                allSnapshotInfos.add(snapshotsInRepo.snapshotInfos());
                                remaining.addAndGet(snapshotsInRepo.remaining());
                                totalCount.addAndGet(snapshotsInRepo.totalCount());
                            }).delegateResponse((l, e) -> {
                                if (isMultiRepoRequest && e instanceof ElasticsearchException elasticsearchException) {
                                    failuresByRepository.put(repoName, elasticsearchException);
                                    l.onResponse(SnapshotsInRepo.EMPTY);
                                } else {
                                    l.onFailure(e);
                                }
                            }));
                        }
                    }
                })

                .andThenApply(ignored -> {
                    cancellableTask.ensureNotCancelled();
                    final var sortedSnapshotsInRepos = sortSnapshots(
                        allSnapshotInfos.stream().flatMap(Collection::stream),
                        totalCount.get(),
                        offset,
                        size
                    );
                    final var snapshotInfos = sortedSnapshotsInRepos.snapshotInfos();
                    assert indices || snapshotInfos.stream().allMatch(snapshotInfo -> snapshotInfo.indices().isEmpty());
                    final int finalRemaining = sortedSnapshotsInRepos.remaining() + remaining.get();
                    return new GetSnapshotsResponse(
                        snapshotInfos,
                        failuresByRepository,
                        finalRemaining > 0
                            ? GetSnapshotsRequest.After.from(snapshotInfos.get(snapshotInfos.size() - 1), this.sortBy).asQueryParam()
                            : null,
                        totalCount.get(),
                        finalRemaining
                    );
                })

                .addListener(listener);
        }

        private void processRepository(String repoName, ActionListener<SnapshotsInRepo> listener) {
            SubscribableListener
                // only load the RepositoryData if we care about non-current snapshots
                .<RepositoryData>newForked(l -> {
                    if (includeOnlyCurrent) {
                        l.onResponse(null);
                    } else {
                        repositoriesService.getRepositoryData(repoName, l);
                    }
                })
                .<SnapshotsInRepo>andThen((l, repoData) -> loadSnapshotInfos(repoName, repoData, l))
                .addListener(listener);
        }

        private void loadSnapshotInfos(String repo, @Nullable RepositoryData repositoryData, ActionListener<SnapshotsInRepo> listener) {
            if (cancellableTask.notifyIfCancelled(listener)) {
                return;
            }

            final Map<String, Snapshot> allSnapshotIds = new HashMap<>();
            final List<SnapshotInfo> currentSnapshots = new ArrayList<>();
            for (final var snapshotInProgressEntry : snapshotsInProgress.forRepo(repo)) {
                final var snapshot = snapshotInProgressEntry.snapshot();
                allSnapshotIds.put(snapshot.getSnapshotId().getName(), snapshot);
                currentSnapshots.add(SnapshotInfo.inProgress(snapshotInProgressEntry).maybeWithoutIndices(indices));
            }

            if (repositoryData != null) {
                for (SnapshotId snapshotId : repositoryData.getSnapshotIds()) {
                    if (predicates.test(snapshotId, repositoryData)) {
                        allSnapshotIds.put(snapshotId.getName(), new Snapshot(repo, snapshotId));
                    }
                }
            }

            final Set<Snapshot> toResolve = new HashSet<>();
            if (isMatchAll) {
                toResolve.addAll(allSnapshotIds.values());
            } else {
                final var exactNamesToMatch = ignoreUnavailable ? null : new HashSet<>(exactNames);
                for (Map.Entry<String, Snapshot> entry : allSnapshotIds.entrySet()) {
                    final Snapshot snapshot = entry.getValue();
                    if (ignoreUnavailable == false
                        && exactNamesToMatch.remove(entry.getKey())
                        && Regex.simpleMatch(excludePatterns, entry.getKey()) == false) {
                        toResolve.add(snapshot);
                    } else if (toResolve.contains(snapshot) == false
                        && (exactNames.contains(entry.getKey()) || Regex.simpleMatch(includePatterns, entry.getKey()))
                        && Regex.simpleMatch(excludePatterns, entry.getKey()) == false) {
                            toResolve.add(snapshot);
                        }
                }
                if (includeCurrent) {
                    for (SnapshotInfo snapshotInfo : currentSnapshots) {
                        final Snapshot snapshot = snapshotInfo.snapshot();
                        if (Regex.simpleMatch(excludePatterns, snapshot.getSnapshotId().getName()) == false) {
                            toResolve.add(snapshot);
                        }
                    }
                }
                if (toResolve.isEmpty() && ignoreUnavailable == false && includeCurrent == false) {
                    throw new SnapshotMissingException(repo, firstPattern);
                }
                if (ignoreUnavailable == false && exactNamesToMatch.isEmpty() == false) {
                    throw new SnapshotMissingException(repo, exactNamesToMatch.iterator().next());
                }
            }

            if (verbose) {
                snapshots(repo, toResolve.stream().map(Snapshot::getSnapshotId).toList(), listener);
            } else {
                assert predicates.isMatchAll() : "filtering is not supported in non-verbose mode";
                final SnapshotsInRepo snapshotInfos;
                if (repositoryData != null) {
                    // want non-current snapshots as well, which are found in the repository data
                    snapshotInfos = buildSimpleSnapshotInfos(toResolve, repo, repositoryData, currentSnapshots);
                } else {
                    // only want current snapshots
                    snapshotInfos = sortSnapshotsWithoutOffsetOrLimit(currentSnapshots.stream().map(SnapshotInfo::basic).toList());
                }
                listener.onResponse(snapshotInfos);
            }
        }

        /**
         * Returns a list of snapshots from repository sorted by snapshot creation date
         *
         * @param repositoryName      repository name
         * @param snapshotIds         snapshots for which to fetch snapshot information
         */
        private void snapshots(String repositoryName, Collection<SnapshotId> snapshotIds, ActionListener<SnapshotsInRepo> listener) {
            if (cancellableTask.notifyIfCancelled(listener)) {
                return;
            }
            final Set<SnapshotInfo> snapshotSet = new HashSet<>();
            final Set<SnapshotId> snapshotIdsToIterate = new HashSet<>(snapshotIds);
            // first, look at the snapshots in progress
            final List<SnapshotsInProgress.Entry> entries = SnapshotsService.currentSnapshots(
                snapshotsInProgress,
                repositoryName,
                snapshotIdsToIterate.stream().map(SnapshotId::getName).toList()
            );
            for (SnapshotsInProgress.Entry entry : entries) {
                if (snapshotIdsToIterate.remove(entry.snapshot().getSnapshotId())) {
                    final SnapshotInfo snapshotInfo = SnapshotInfo.inProgress(entry);
                    if (predicates.test(snapshotInfo)) {
                        snapshotSet.add(snapshotInfo.maybeWithoutIndices(indices));
                    }
                }
            }
            // then, look in the repository if there's any matching snapshots left
            final List<SnapshotInfo> snapshotInfos;
            if (snapshotIdsToIterate.isEmpty()) {
                snapshotInfos = Collections.emptyList();
            } else {
                snapshotInfos = Collections.synchronizedList(new ArrayList<>());
            }
            final ActionListener<Void> allDoneListener = listener.safeMap(v -> {
                final ArrayList<SnapshotInfo> snapshotList = new ArrayList<>(snapshotInfos);
                snapshotList.addAll(snapshotSet);
                return sortSnapshotsWithoutOffsetOrLimit(snapshotList);
            });
            if (snapshotIdsToIterate.isEmpty()) {
                allDoneListener.onResponse(null);
                return;
            }
            final Repository repository;
            try {
                repository = repositoriesService.repository(repositoryName);
            } catch (RepositoryMissingException e) {
                listener.onFailure(e);
                return;
            }
            repository.getSnapshotInfo(
                new GetSnapshotInfoContext(
                    snapshotIdsToIterate,
                    ignoreUnavailable == false,
                    cancellableTask::isCancelled,
                    (context, snapshotInfo) -> {
                        if (predicates.test(snapshotInfo)) {
                            snapshotInfos.add(snapshotInfo.maybeWithoutIndices(indices));
                        }
                    },
                    allDoneListener
                )
            );
        }

        private SnapshotsInRepo buildSimpleSnapshotInfos(
            final Set<Snapshot> toResolve,
            final String repoName,
            final RepositoryData repositoryData,
            final List<SnapshotInfo> currentSnapshots
        ) {
            List<SnapshotInfo> snapshotInfos = new ArrayList<>();
            for (SnapshotInfo snapshotInfo : currentSnapshots) {
                if (toResolve.remove(snapshotInfo.snapshot())) {
                    snapshotInfos.add(snapshotInfo.basic());
                }
            }
            Map<SnapshotId, List<String>> snapshotsToIndices = new HashMap<>();
            if (indices) {
                for (IndexId indexId : repositoryData.getIndices().values()) {
                    for (SnapshotId snapshotId : repositoryData.getSnapshots(indexId)) {
                        if (toResolve.contains(new Snapshot(repoName, snapshotId))) {
                            snapshotsToIndices.computeIfAbsent(snapshotId, (k) -> new ArrayList<>()).add(indexId.getName());
                        }
                    }
                }
            }
            for (Snapshot snapshot : toResolve) {
                snapshotInfos.add(
                    new SnapshotInfo(
                        snapshot,
                        snapshotsToIndices.getOrDefault(snapshot.getSnapshotId(), Collections.emptyList()),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        repositoryData.getSnapshotState(snapshot.getSnapshotId())
                    )
                );
            }
            return sortSnapshotsWithoutOffsetOrLimit(snapshotInfos);
        }

        private static final Comparator<SnapshotInfo> BY_START_TIME = Comparator.comparingLong(SnapshotInfo::startTime)
            .thenComparing(SnapshotInfo::snapshotId);

        private static final Comparator<SnapshotInfo> BY_DURATION = Comparator.<SnapshotInfo>comparingLong(
            sni -> sni.endTime() - sni.startTime()
        ).thenComparing(SnapshotInfo::snapshotId);

        private static final Comparator<SnapshotInfo> BY_INDICES_COUNT = Comparator.<SnapshotInfo>comparingInt(sni -> sni.indices().size())
            .thenComparing(SnapshotInfo::snapshotId);

        private static final Comparator<SnapshotInfo> BY_SHARDS_COUNT = Comparator.comparingInt(SnapshotInfo::totalShards)
            .thenComparing(SnapshotInfo::snapshotId);

        private static final Comparator<SnapshotInfo> BY_FAILED_SHARDS_COUNT = Comparator.comparingInt(SnapshotInfo::failedShards)
            .thenComparing(SnapshotInfo::snapshotId);

        private static final Comparator<SnapshotInfo> BY_NAME = Comparator.comparing(sni -> sni.snapshotId().getName());

        private static final Comparator<SnapshotInfo> BY_REPOSITORY = Comparator.comparing(SnapshotInfo::repository)
            .thenComparing(SnapshotInfo::snapshotId);

        private SnapshotsInRepo sortSnapshotsWithoutOffsetOrLimit(List<SnapshotInfo> snapshotInfos) {
            return sortSnapshots(snapshotInfos.stream(), snapshotInfos.size(), 0, GetSnapshotsRequest.NO_LIMIT);
        }

        private SnapshotsInRepo sortSnapshots(Stream<SnapshotInfo> infos, int totalCount, int offset, int size) {
            final Comparator<SnapshotInfo> comparator = switch (sortBy) {
                case START_TIME -> BY_START_TIME;
                case NAME -> BY_NAME;
                case DURATION -> BY_DURATION;
                case INDICES -> BY_INDICES_COUNT;
                case SHARDS -> BY_SHARDS_COUNT;
                case FAILED_SHARDS -> BY_FAILED_SHARDS_COUNT;
                case REPOSITORY -> BY_REPOSITORY;
            };

            if (after != null) {
                assert offset == 0 : "can't combine after and offset but saw [" + after + "] and offset [" + offset + "]";
                infos = infos.filter(buildAfterPredicate(sortBy, after, order));
            }
            infos = infos.sorted(order == SortOrder.DESC ? comparator.reversed() : comparator).skip(offset);
            final List<SnapshotInfo> allSnapshots = infos.toList();
            final List<SnapshotInfo> snapshots;
            if (size != GetSnapshotsRequest.NO_LIMIT) {
                snapshots = allSnapshots.stream().limit(size + 1).toList();
            } else {
                snapshots = allSnapshots;
            }
            final List<SnapshotInfo> resultSet = size != GetSnapshotsRequest.NO_LIMIT && size < snapshots.size()
                ? snapshots.subList(0, size)
                : snapshots;
            return new SnapshotsInRepo(resultSet, totalCount, allSnapshots.size() - resultSet.size());
        }

        private static Predicate<SnapshotInfo> buildAfterPredicate(
            GetSnapshotsRequest.SortBy sortBy,
            GetSnapshotsRequest.After after,
            SortOrder order
        ) {
            final String snapshotName = after.snapshotName();
            final String repoName = after.repoName();
            final String value = after.value();
            return switch (sortBy) {
                case START_TIME -> filterByLongOffset(SnapshotInfo::startTime, Long.parseLong(value), snapshotName, repoName, order);
                case NAME ->
                    // TODO: cover via pre-flight predicate
                    order == SortOrder.ASC
                        ? (info -> compareName(snapshotName, repoName, info) < 0)
                        : (info -> compareName(snapshotName, repoName, info) > 0);
                case DURATION -> filterByLongOffset(
                    info -> info.endTime() - info.startTime(),
                    Long.parseLong(value),
                    snapshotName,
                    repoName,
                    order
                );
                case INDICES ->
                    // TODO: cover via pre-flight predicate
                    filterByLongOffset(info -> info.indices().size(), Integer.parseInt(value), snapshotName, repoName, order);
                case SHARDS -> filterByLongOffset(SnapshotInfo::totalShards, Integer.parseInt(value), snapshotName, repoName, order);
                case FAILED_SHARDS -> filterByLongOffset(
                    SnapshotInfo::failedShards,
                    Integer.parseInt(value),
                    snapshotName,
                    repoName,
                    order
                );
                case REPOSITORY ->
                    // TODO: cover via pre-flight predicate
                    order == SortOrder.ASC
                        ? (info -> compareRepositoryName(snapshotName, repoName, info) < 0)
                        : (info -> compareRepositoryName(snapshotName, repoName, info) > 0);
            };
        }

        private static Predicate<SnapshotInfo> filterByLongOffset(
            ToLongFunction<SnapshotInfo> extractor,
            long after,
            String snapshotName,
            String repoName,
            SortOrder order
        ) {
            return order == SortOrder.ASC ? info -> {
                final long val = extractor.applyAsLong(info);
                return after < val || (after == val && compareName(snapshotName, repoName, info) < 0);
            } : info -> {
                final long val = extractor.applyAsLong(info);
                return after > val || (after == val && compareName(snapshotName, repoName, info) > 0);
            };
        }

        private static int compareRepositoryName(String name, String repoName, SnapshotInfo info) {
            final int res = repoName.compareTo(info.repository());
            if (res != 0) {
                return res;
            }
            return name.compareTo(info.snapshotId().getName());
        }

        private static int compareName(String name, String repoName, SnapshotInfo info) {
            final int res = name.compareTo(info.snapshotId().getName());
            if (res != 0) {
                return res;
            }
            return repoName.compareTo(info.repository());
        }

        private record SnapshotsInRepo(List<SnapshotInfo> snapshotInfos, int totalCount, int remaining) {
            private static final SnapshotsInRepo EMPTY = new SnapshotsInRepo(List.of(), 0, 0);
        }
    }
}
