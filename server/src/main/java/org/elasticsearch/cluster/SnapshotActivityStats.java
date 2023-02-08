/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

public record SnapshotActivityStats(
    int snapshotsInProgressCount,
    int deletionsInProgressCount,
    int cleanupsInProgressCount,
    List<SnapshotRepositoryActivityStats> statsByRepository
) implements ToXContentObject, Writeable {

    public static SnapshotActivityStats EMPTY = new SnapshotActivityStats(0, 0, 0, List.of());

    public static SnapshotActivityStats of(ClusterState clusterState, long currentTimeMillis) {
        return of(
            clusterState.metadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY),
            clusterState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY),
            clusterState.custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY),
            clusterState.custom(RepositoryCleanupInProgress.TYPE, RepositoryCleanupInProgress.EMPTY),
            currentTimeMillis
        );
    }

    private static SnapshotActivityStats of(
        RepositoriesMetadata repositoriesMetadata,
        SnapshotsInProgress snapshotsInProgress,
        SnapshotDeletionsInProgress snapshotDeletionsInProgress,
        RepositoryCleanupInProgress repositoryCleanupInProgress,
        long currentTimeMillis
    ) {

        final var snapshotsInProgressCount = snapshotsInProgress.count();
        final var deletionsInProgressCount = snapshotDeletionsInProgress.getEntries().size();
        final var cleanupsInProgressCount = repositoryCleanupInProgress.entries().size();
        final var perRepositoryStats = new ArrayList<SnapshotRepositoryActivityStats>(repositoriesMetadata.repositories().size());

        for (RepositoryMetadata repository : repositoriesMetadata.repositories()) {

            final var repositoryName = repository.name();
            final var repositoryType = repository.type();

            var snapshotCount = 0;
            var cloneCount = 0;
            var finalizationsCount = 0;
            var totalShards = 0;
            var completeShards = 0;
            final var shardStatesAccumulator = new EnumMap<>(
                Arrays.stream(SnapshotsInProgress.ShardState.values())
                    .collect(Collectors.toMap(Function.identity(), ignored -> new AtomicInteger()))
            );
            var deletionsCount = 0;
            var snapshotDeletionsCount = 0;
            var activeDeletionsCount = 0;
            var firstStartTimeMillis = currentTimeMillis;

            for (SnapshotsInProgress.Entry entry : snapshotsInProgress.forRepo(repositoryName)) {
                firstStartTimeMillis = Math.min(firstStartTimeMillis, entry.startTime());

                if (entry.state().completed()) {
                    finalizationsCount += 1;
                }

                if (entry.isClone()) {
                    cloneCount += 1;
                } else {
                    snapshotCount += 1;
                    totalShards += entry.shards().size();

                    for (SnapshotsInProgress.ShardSnapshotStatus value : entry.shards().values()) {
                        if (value.state().completed()) {
                            completeShards += 1;
                        }

                        shardStatesAccumulator.get(value.state()).incrementAndGet();
                    }
                }
            }

            for (SnapshotDeletionsInProgress.Entry entry : snapshotDeletionsInProgress.getEntries()) {
                if (entry.repository().equals(repositoryName)) {
                    firstStartTimeMillis = Math.min(firstStartTimeMillis, entry.getStartTime());
                    deletionsCount += 1;
                    snapshotDeletionsCount += entry.getSnapshots().size();
                    if (entry.state() == SnapshotDeletionsInProgress.State.STARTED) {
                        activeDeletionsCount += 1;
                    }
                }
            }

            perRepositoryStats.add(
                new SnapshotRepositoryActivityStats(
                    repositoryName,
                    repositoryType,
                    snapshotCount,
                    cloneCount,
                    finalizationsCount,
                    totalShards,
                    completeShards,
                    deletionsCount,
                    snapshotDeletionsCount,
                    activeDeletionsCount,
                    firstStartTimeMillis,
                    new EnumMap<SnapshotsInProgress.ShardState, Integer>(
                        shardStatesAccumulator.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()))
                    )
                )
            );
        }

        return new SnapshotActivityStats(
            snapshotsInProgressCount,
            deletionsInProgressCount,
            cleanupsInProgressCount,
            List.copyOf(perRepositoryStats)
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.startObject();

        builder.startObject("in_progress_counts");
        builder.field("snapshots", snapshotsInProgressCount);
        builder.field("snapshot_deletions", deletionsInProgressCount);
        builder.field("concurrent_operations", snapshotsInProgressCount + deletionsInProgressCount);
        // NB cleanups are not "concurrent operations", not counted here ^
        builder.field("cleanups", cleanupsInProgressCount);
        builder.endObject();

        builder.startObject("repositories");
        for (SnapshotRepositoryActivityStats perRepositoryStats : statsByRepository) {
            perRepositoryStats.toXContent(builder, params);
        }
        builder.endObject();

        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(snapshotsInProgressCount);
        out.writeVInt(deletionsInProgressCount);
        out.writeVInt(cleanupsInProgressCount);
        out.writeList(statsByRepository);
    }

    public static SnapshotActivityStats readFrom(StreamInput in) throws IOException {
        return new SnapshotActivityStats(
            in.readVInt(),
            in.readVInt(),
            in.readVInt(),
            in.readList(SnapshotRepositoryActivityStats::readFrom)
        );
    }

    record SnapshotRepositoryActivityStats(
        String repositoryName,
        String repositoryType,
        int snapshotCount,
        int cloneCount,
        int finalizationsCount,
        int totalShards,
        int completeShards,
        int deletionsCount,
        int snapshotDeletionsCount,
        int activeDeletionsCount,
        long firstStartTimeMillis,
        EnumMap<SnapshotsInProgress.ShardState, Integer> shardStates
    ) implements ToXContentFragment, Writeable {
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(repositoryName);
            builder.field("type", repositoryType);

            builder.startObject("totals");
            builder.field("snapshots", snapshotCount);
            builder.field("clones", cloneCount);
            builder.field("finalizations", finalizationsCount);
            builder.field("deletions", deletionsCount);
            builder.field("snapshot_deletions", snapshotDeletionsCount);
            builder.field("active_deletions", activeDeletionsCount);
            builder.endObject();

            builder.startObject("shards");
            builder.field("total", totalShards);
            builder.field("complete", completeShards);
            builder.field("incomplete", totalShards - completeShards);
            builder.startObject("states");
            for (Map.Entry<SnapshotsInProgress.ShardState, Integer> entry : shardStates.entrySet()) {
                builder.field(entry.getKey().toString(), entry.getValue());
            }
            builder.endObject();
            builder.endObject();

            builder.timeField("oldest_start_time_millis", "oldest_start_time", firstStartTimeMillis);

            return builder.endObject();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(repositoryName);
            out.writeString(repositoryType);
            out.writeVInt(snapshotCount);
            out.writeVInt(cloneCount);
            out.writeVInt(finalizationsCount);
            out.writeVInt(totalShards);
            out.writeVInt(completeShards);
            out.writeVInt(deletionsCount);
            out.writeVInt(snapshotDeletionsCount);
            out.writeVInt(activeDeletionsCount);
            out.writeVLong(firstStartTimeMillis);
            out.writeMap(shardStates, (o, state) -> o.writeString(state.toString()), StreamOutput::writeVInt);
        }

        static SnapshotRepositoryActivityStats readFrom(StreamInput in) throws IOException {
            return new SnapshotRepositoryActivityStats(
                in.readString(),
                in.readString(),
                in.readVInt(),
                in.readVInt(),
                in.readVInt(),
                in.readVInt(),
                in.readVInt(),
                in.readVInt(),
                in.readVInt(),
                in.readVInt(),
                in.readVLong(),
                new EnumMap<SnapshotsInProgress.ShardState, Integer>(
                    in.readMap(i -> SnapshotsInProgress.ShardState.valueOf(in.readString()), StreamInput::readVInt)
                )
            );
        }
    }
}
