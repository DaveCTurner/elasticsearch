/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.repositories.IndexMetaDataGenerations;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class SnapshotSortKeyTests extends ESTestCase {

    private record Candidate(String repositoryName, SnapshotId snapshotId, RepositoryData repositoryData) {}

    public void testStartTimeSkipPredicate() {
        final var lastLoaded = snapshotInfo(randomRepoName(), randomSnapshotId(), randomNonExtremalTime(), randomLong());
        assertSkippability(
            SnapshotSortKey.START_TIME,
            candidateWithStartEnd(randomLongBetween(0, lastLoaded.startTime() - 1), randomLong()),
            lastLoaded,
            candidateWithStartEnd(randomLongBetween(lastLoaded.startTime() + 1, Long.MAX_VALUE), randomLong())
        );
    }

    public void testStartTimeSkipPredicateUnknownStartTimeMustLoad() {
        assertCanSkipLoading(
            false,
            SnapshotSortKey.START_TIME.getSkipLoadingPredicate(randomFrom(SortOrder.values())),
            snapshotInfo(randomRepoName(), randomSnapshotId(), randomNonExtremalTime(), randomLong()),
            candidateWithStartEnd(UNKNOWN_MILLIS, randomLong())
        );
    }

    public void testStartTimeSkipPredicateMissingDetailsMustLoad() {
        assertCanSkipLoading(
            false,
            SnapshotSortKey.START_TIME.getSkipLoadingPredicate(randomFrom(SortOrder.values())),
            snapshotInfo(randomRepoName(), randomSnapshotId(), randomNonExtremalTime(), randomLong()),
            new Candidate(randomRepoName(), randomSnapshotId(), RepositoryData.EMPTY)
        );
    }

    public void testNameSkipPredicate() {
        final var snapshotIds = getThreeSnapshotIdsInOrder();
        assertSkippability(
            SnapshotSortKey.NAME,
            new Candidate(randomRepoName(), snapshotIds.getFirst(), RepositoryData.EMPTY),
            snapshotInfo(randomRepoName(), snapshotIds.get(1), randomLong(), randomLong()),
            new Candidate(randomRepoName(), snapshotIds.getLast(), RepositoryData.EMPTY)
        );
    }

    public void testDurationSkipPredicate() {
        final long lastLoadedDuration = randomNonExtremalTime();
        assertSkippability(
            SnapshotSortKey.DURATION,
            candidateWithDuration(randomLongBetween(0, lastLoadedDuration - 1)),
            snapshotInfoWithDuration(lastLoadedDuration),
            candidateWithDuration(randomLongBetween(lastLoadedDuration + 1, Long.MAX_VALUE))
        );
    }

    public void testDurationSkipPredicateUnknownStartOrEndMustLoad() {
        assertCanSkipLoading(
            false,
            SnapshotSortKey.DURATION.getSkipLoadingPredicate(randomFrom(SortOrder.values())),
            snapshotInfoWithDuration(randomNonExtremalTime()),
            randomBoolean() ? candidateWithStartEnd(UNKNOWN_MILLIS, UNKNOWN_MILLIS)
                : randomBoolean() ? candidateWithStartEnd(randomNonNegativeLong(), UNKNOWN_MILLIS)
                : candidateWithStartEnd(UNKNOWN_MILLIS, randomNonNegativeLong())
        );
    }

    public void testDurationSkipPredicateMissingDetailsMustLoad() {
        assertCanSkipLoading(
            false,
            SnapshotSortKey.DURATION.getSkipLoadingPredicate(randomFrom(SortOrder.values())),
            snapshotInfoWithDuration(randomNonExtremalTime()),
            new Candidate(randomRepoName(), randomSnapshotId(), RepositoryData.EMPTY)
        );
    }

    public void testRepositorySkipPredicate() {
        final var repoNames = getThreeRepositoryNames();
        assertSkippability(
            SnapshotSortKey.REPOSITORY,
            new Candidate(repoNames.getFirst(), randomSnapshotId(), RepositoryData.EMPTY),
            snapshotInfo(repoNames.get(1), randomSnapshotId(), randomLong(), randomLong()),
            new Candidate(repoNames.getLast(), randomSnapshotId(), RepositoryData.EMPTY)
        );
    }

    public void testTieBreakOnSnapshotName() {
        final var snapshotIds = getThreeSnapshotIdsInOrder();

        final var lastLoadedDuration = randomNonExtremalTime();
        final var lastLoadedStartTime = randomLongBetween(1, Long.MAX_VALUE - lastLoadedDuration - 1);
        final var lastLoadedEndTime = lastLoadedStartTime + lastLoadedDuration;
        final var lastLoadedSnapshotInfo = snapshotInfo(randomRepoName(), snapshotIds.get(1), lastLoadedStartTime, lastLoadedEndTime);

        final var firstCandidate = new Candidate(
            lastLoadedSnapshotInfo.repository(),
            snapshotIds.getFirst(),
            repositoryDataWithStartEnd(snapshotIds.getFirst(), lastLoadedStartTime, lastLoadedEndTime)
        );
        final var lastCandidate = new Candidate(
            lastLoadedSnapshotInfo.repository(),
            snapshotIds.getLast(),
            repositoryDataWithStartEnd(snapshotIds.getLast(), lastLoadedStartTime, lastLoadedEndTime)
        );

        for (final var sortKey : List.of(SnapshotSortKey.values())) {
            if (Arrays.stream(SortOrder.values())
                .anyMatch(order -> sortKey.getSkipLoadingPredicate(order) != SnapshotSortKey.SkipLoadingPredicate.NEVER_SKIP)) {
                assertSkippability(sortKey, firstCandidate, lastLoadedSnapshotInfo, lastCandidate);
            }
        }
    }

    private static Collection<String> getThreeDistinctStrings(Supplier<String> stringSupplier) {
        final var set = new TreeSet<String>();
        while (set.size() < 3) {
            set.add(stringSupplier.get());
        }
        return set;
    }

    private static List<SnapshotId> getThreeSnapshotIdsInOrder() {
        return getThreeDistinctStrings(ESTestCase::randomSnapshotName).stream().map(n -> new SnapshotId(n, randomUUID())).toList();
    }

    private static List<String> getThreeRepositoryNames() {
        return getThreeDistinctStrings(ESTestCase::randomRepoName).stream().toList();
    }

    public void testNeverSkipPredicates() {
        for (var sortKey : List.of(SnapshotSortKey.INDICES, SnapshotSortKey.SHARDS, SnapshotSortKey.FAILED_SHARDS)) {
            for (var order : SortOrder.values()) {
                assertThat(sortKey.getSkipLoadingPredicate(order), sameInstance(SnapshotSortKey.SkipLoadingPredicate.NEVER_SKIP));
            }
        }
        final var snapshotId = randomSnapshotId();
        assertThat(
            SnapshotSortKey.SkipLoadingPredicate.NEVER_SKIP.canSkipLoading(
                randomRepoName(),
                snapshotId,
                randomBoolean() ? RepositoryData.EMPTY
                    : randomBoolean() ? repositoryDataWithStartEnd(snapshotId, randomNonNegativeLong(), randomNonNegativeLong())
                    : repositoryDataWithStartEnd(snapshotId, UNKNOWN_MILLIS, UNKNOWN_MILLIS),
                snapshotInfo(randomRepoName(), randomSnapshotId(), randomLong(), randomLong())
            ),
            equalTo(false)
        );
    }

    private static final int UNKNOWN_MILLIS = -1;

    private static void assertCanSkipLoading(
        boolean expectSkippable,
        SnapshotSortKey.SkipLoadingPredicate predicate,
        SnapshotInfo lastLoaded,
        Candidate candidate
    ) {
        assertThat(
            predicate.canSkipLoading(candidate.repositoryName(), candidate.snapshotId(), candidate.repositoryData(), lastLoaded),
            equalTo(expectSkippable)
        );
    }

    private static void assertSkippability(SnapshotSortKey sortKey, Candidate first, SnapshotInfo lastLoaded, Candidate last) {
        final var ascendingPredicate = sortKey.getSkipLoadingPredicate(SortOrder.ASC);
        assertCanSkipLoading(false, ascendingPredicate, lastLoaded, first);
        assertCanSkipLoading(true, ascendingPredicate, lastLoaded, last);
        final var descendingPredicate = sortKey.getSkipLoadingPredicate(SortOrder.DESC);
        assertCanSkipLoading(false, descendingPredicate, lastLoaded, last);
        assertCanSkipLoading(true, descendingPredicate, lastLoaded, first);
    }

    private static SnapshotId randomSnapshotId() {
        return new SnapshotId(randomSnapshotName(), randomUUID());
    }

    private static SnapshotInfo snapshotInfo(String repo, SnapshotId snapshotId, long startTime, long endTime) {
        return new SnapshotInfo(
            new Snapshot(ProjectId.DEFAULT, repo, snapshotId),
            List.of(),
            List.of(),
            List.of(),
            null,
            IndexVersion.current(),
            startTime,
            endTime,
            0,
            0,
            List.of(),
            null,
            null,
            SnapshotState.SUCCESS,
            Map.of()
        );
    }

    private static SnapshotInfo snapshotInfoWithDuration(long duration) {
        final var startTime = randomLongBetween(0, Long.MAX_VALUE - duration);
        return snapshotInfo(randomRepoName(), randomSnapshotId(), startTime, startTime + duration);
    }

    private static RepositoryData repositoryDataWithStartEnd(SnapshotId snapshotId, long startTimeMillis, long endTimeMillis) {
        return new RepositoryData(
            RepositoryData.MISSING_UUID,
            RepositoryData.EMPTY_REPO_GEN,
            Map.of(snapshotId.getUUID(), snapshotId),
            Map.of(
                snapshotId.getUUID(),
                new RepositoryData.SnapshotDetails(SnapshotState.SUCCESS, IndexVersion.current(), startTimeMillis, endTimeMillis, null)
            ),
            Map.of(),
            ShardGenerations.EMPTY,
            IndexMetaDataGenerations.EMPTY,
            RepositoryData.MISSING_UUID
        );
    }

    private static Candidate candidateWithStartEnd(long startTimeMillis, long endTimeMillis) {
        final var snapshotId = randomSnapshotId();
        return new Candidate(randomRepoName(), snapshotId, repositoryDataWithStartEnd(snapshotId, startTimeMillis, endTimeMillis));
    }

    private static Candidate candidateWithDuration(long duration) {
        final var startTime = randomLongBetween(0, Long.MAX_VALUE - duration);
        return candidateWithStartEnd(startTime, startTime + duration);
    }

    private static long randomNonExtremalTime() {
        return randomLongBetween(1, Long.MAX_VALUE - 1);
    }
}
