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
import org.elasticsearch.repositories.IndexMetaDataGenerations;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotInfoTestUtils;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class SnapshotInfoCollectorTests extends ESTestCase {

    /**
     * Naive, inefficient, but obviously correct implementation: store all items, sort and possibly limit on demand.
     */
    private static final class NaiveSnapshotInfoCollector {
        private final List<SnapshotInfo> snapshotInfos = new ArrayList<>();
        private final Comparator<SnapshotInfo> comparator;
        private final int size;
        private final int offset;

        NaiveSnapshotInfoCollector(Comparator<SnapshotInfo> comparator, int size, int offset) {
            this.comparator = comparator;
            this.size = size;
            this.offset = offset;
        }

        void add(SnapshotInfo snapshotInfo) {
            snapshotInfos.add(snapshotInfo);
        }

        List<SnapshotInfo> getSnapshotInfos() {
            snapshotInfos.sort(comparator);
            if (offset >= snapshotInfos.size()) {
                return List.of();
            }
            return snapshotInfos.subList(offset, isLastPage() ? snapshotInfos.size() : offset + size);
        }

        int getRemaining() {
            return isLastPage() ? 0 : snapshotInfos.size() - offset - size;
        }

        private boolean isLastPage() {
            return size == GetSnapshotsRequest.NO_LIMIT
                || offset + size > snapshotInfos.size()
                || (offset + size == snapshotInfos.size() && randomBoolean() /* shouldn't matter if we're right on the boundary */);
        }
    }

    private static SnapshotInfo randomSnapshotInfo() {
        final var startTime = randomNonNegativeLong();
        final var endTime = randomLongBetween(startTime, Long.MAX_VALUE);
        return new SnapshotInfo(
            new Snapshot(ProjectId.DEFAULT, randomRepoName(), new SnapshotId(randomSnapshotName(), randomUUID())),
            randomList(0, 10, ESTestCase::randomIndexName),
            List.of(),
            List.of(),
            null,
            endTime,
            randomInt(),
            Collections.emptyList(),
            null,
            SnapshotInfoTestUtils.randomUserMetadata(),
            startTime,
            SnapshotInfoTestUtils.randomIndexSnapshotDetails()
        );
    }

    public void testMatchesNaiveImplementation() {
        final var sortBy = randomFrom(SnapshotSortKey.values());
        final var order = randomFrom(SortOrder.values());
        final var comparator = sortBy.getSnapshotInfoComparator(order);
        final var size = randomBoolean() ? GetSnapshotsRequest.NO_LIMIT : between(1, 20);
        final var offset = between(0, 20);
        final var oracle = new NaiveSnapshotInfoCollector(comparator, size, offset);
        final var production = SnapshotInfoCollector.create(comparator, size, offset, sortBy.getSkipLoadingPredicate(order));

        final var snapshotInfos = randomList(0, 100, SnapshotInfoCollectorTests::randomSnapshotInfo);
        for (SnapshotInfo info : snapshotInfos) {
            oracle.add(info);
        }

        runInParallel(snapshotInfos.size(), i -> {
            final var snapshotInfo = snapshotInfos.get(i);
            final var snapshotId = snapshotInfo.snapshotId();
            if (production.canSkipLoading(
                snapshotInfo.repository(),
                snapshotId,
                randomBoolean() ? RepositoryData.EMPTY : repositoryDataWithSingleSnapshotDetails(snapshotInfo)
            )) {
                production.addSkipped();
            } else {
                production.add(snapshotInfo);
            }
        });

        final var expected = oracle.getSnapshotInfos();
        final var actual = production.getSnapshotInfos();
        assertThat(actual.size(), equalTo(expected.size()));
        assertThat(production.getRemaining(), equalTo(oracle.getRemaining()));
        for (int i = 0; i < expected.size(); i++) {
            assertThat("value at " + i, actual.get(i), sameInstance(expected.get(i)));
        }
    }

    public void testPreflightSkipping() {
        final var sortBy = randomValueOtherThanMany(
            k -> Arrays.stream(SortOrder.values())
                .allMatch(o -> k.getSkipLoadingPredicate(o) == SnapshotSortKey.SkipLoadingPredicate.NEVER_SKIP),
            () -> randomFrom(SnapshotSortKey.values())
        );
        final var order = randomFrom(SortOrder.values());
        final var comparator = sortBy.getSnapshotInfoComparator(order);
        final var size = between(1, 20);
        final var offset = between(0, 20);
        final var collector = SnapshotInfoCollector.create(comparator, size, offset, sortBy.getSkipLoadingPredicate(order));

        final var snapshotInfos = randomList(size + offset + 1, 100, SnapshotInfoCollectorTests::randomSnapshotInfo);
        snapshotInfos.sort(comparator);
        for (final var snapshotInfo : snapshotInfos.subList(0, size + offset)) {
            collector.add(snapshotInfo);
        }

        assertThat(collector.getRemaining(), equalTo(0));

        for (final var snapshotInfo : snapshotInfos.subList(size + offset, snapshotInfos.size())) {
            assertTrue(
                collector.canSkipLoading(
                    snapshotInfo.repository(),
                    snapshotInfo.snapshotId(),
                    repositoryDataWithSingleSnapshotDetails(snapshotInfo)
                )
            );
            collector.addSkipped();
        }

        final var expected = snapshotInfos.subList(offset, offset + size);
        final var actual = collector.getSnapshotInfos();
        assertThat(actual.size(), equalTo(expected.size()));
        assertThat(collector.getRemaining(), equalTo(snapshotInfos.size() - size - offset));
        for (int i = 0; i < expected.size(); i++) {
            assertThat("value at " + i, actual.get(i), sameInstance(expected.get(i)));
        }
    }

    private static RepositoryData repositoryDataWithSingleSnapshotDetails(SnapshotInfo snapshotInfo) {
        final var snapshotId = snapshotInfo.snapshotId();
        return new RepositoryData(
            randomUUID(),
            randomNonNegativeLong(),
            Map.of(snapshotId.getUUID(), snapshotId),
            Map.of(snapshotId.getUUID(), RepositoryData.SnapshotDetails.fromSnapshotInfo(snapshotInfo)),
            Map.of(),
            ShardGenerations.EMPTY,
            IndexMetaDataGenerations.EMPTY,
            randomUUID()
        );
    }

    public void testOverflow() {
        final var offset = between(1, Integer.MAX_VALUE - 1);
        final var size = Integer.MAX_VALUE - between(0, offset - 1);
        expectThrows(
            IllegalArgumentException.class,
            () -> SnapshotInfoCollector.create(
                (ignoredSnapshotInfo1, ignoredSnapshotInfo2) -> fail(null, "not called"),
                size,
                offset,
                (ignoredRepoName, ignoredSnapshotId, ignoredRepositoryData, ignoredWorstSnapshotInfo) -> fail(null, "not called")
            )
        );
    }

    private static Comparator<SnapshotInfo> randomSnapshotInfoComparator() {
        return randomFrom(SnapshotSortKey.values()).getSnapshotInfoComparator(randomFrom(SortOrder.values()));
    }
}
