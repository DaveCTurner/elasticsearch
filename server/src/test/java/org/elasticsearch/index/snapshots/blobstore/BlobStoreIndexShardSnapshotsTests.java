/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.snapshots.blobstore;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.store.StoreFileMetadata.UNAVAILABLE_WRITER_UUID;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class BlobStoreIndexShardSnapshotsTests extends ESTestCase {

    public void testToFromXContent() throws IOException {
        final FileInfo shared = fileInfo("__0", "_0.cfs");
        final FileInfo onlyFirst = fileInfo("__1", "_0.si");
        final BlobStoreIndexShardSnapshots snapshots = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(
            new SnapshotFiles("snapshot_1", List.of(shared, onlyFirst), "state-1")
        ).withAddedSnapshot(new SnapshotFiles("snapshot_2", List.of(shared), null));

        final BlobStoreIndexShardSnapshots parsed;
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.SMILE)) {
            builder.startObject();
            snapshots.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            try (XContentParser parser = createParser(builder)) {
                parsed = BlobStoreIndexShardSnapshots.fromXContent(parser);
            }
        }

        assertThat(parsed.snapshots().size(), equalTo(2));
        assertSnapshotEquals(snapshots.snapshots().get(0), parsed.snapshots().get(0));
        assertSnapshotEquals(snapshots.snapshots().get(1), parsed.snapshots().get(1));
        assertTrue(parsed.findNameFile("__0").isSame(shared));
        assertTrue(parsed.findNameFile("__1").isSame(onlyFirst));
    }

    public void testDuplicateSnapshotName() {
        final SnapshotFiles snapshotFiles = new SnapshotFiles("snapshot_1", List.of(fileInfo("__0", "_0.cfs")), null);
        final BlobStoreIndexShardSnapshots snapshots = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(snapshotFiles)
            .withAddedSnapshot(snapshotFiles);

        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> {
            try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.SMILE)) {
                builder.startObject();
                snapshots.toXContent(builder, ToXContent.EMPTY_PARAMS);
            }
        });
        assertThat(e.getMessage(), containsString("snapshot_1"));
    }

    private static void assertSnapshotEquals(SnapshotFiles expected, SnapshotFiles actual) {
        assertThat(actual.snapshot(), equalTo(expected.snapshot()));
        assertThat(actual.shardStateIdentifier(), equalTo(expected.shardStateIdentifier()));
        assertTrue(actual.isSame(expected));
    }

    /**
     * The same physical name can correspond to several snapshotted files that differ in content (e.g. a recycled {@code segments_N}), so
     * the lookup must select the one that {@code isSame} the store file, and must not match on name alone.
     */
    public void testGetPhysicalFileMatchesSelectsMatchingFileAmongCollidingNames() {
        final FileInfo v1 = fileInfo("__v1", "segments_1", 100L, "checksum-1");
        final FileInfo v2 = fileInfo("__v2", "segments_1", 100L, "checksum-2");
        final FileInfo v3 = fileInfo("__v3", "segments_1", 200L, "checksum-1");
        final FileInfo unique = fileInfo("__u", "_0.cfs", 100L, "checksum-1");
        final var history = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(new SnapshotFiles("snap-1", List.of(v1, unique), null))
            .withAddedSnapshot(new SnapshotFiles("snap-2", List.of(v2, unique), null))
            .withAddedSnapshot(new SnapshotFiles("snap-3", List.of(v3), null));

        final var matches = physicalFileMatches(
            history,
            List.of(storeFile("segments_1", 100L, "checksum-2"), storeFile("_0.cfs", 100L, "checksum-1"))
        );
        assertThat(matches.existing(), containsInAnyOrder(sameInstance(v2), sameInstance(unique)));
        assertThat(matches.missing(), empty());

        assertOnlyExisting(history, storeFile("segments_1", 100L, "checksum-1"), sameInstance(v1));
        assertOnlyExisting(history, storeFile("segments_1", 200L, "checksum-1"), sameInstance(v3));
    }

    /**
     * Several snapshotted files can be equivalent to the same store file, e.g. the same content uploaded twice under different blob names.
     * The store file must then be matched exactly once, and must not also be reported as missing.
     */
    public void testGetPhysicalFileMatchesMatchesEachTargetOnce() {
        final FileInfo copy1 = fileInfo("__copy1", "_0.cfs", 100L, "checksum-1");
        final FileInfo copy2 = fileInfo("__copy2", "_0.cfs", 100L, "checksum-1");
        final var history = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(new SnapshotFiles("snap-1", List.of(copy1), null))
            .withAddedSnapshot(new SnapshotFiles("snap-2", List.of(copy2), null));

        // which copy wins depends on iteration order, but either is a valid file to reuse
        assertOnlyExisting(history, storeFile("_0.cfs", 100L, "checksum-1"), anyOf(sameInstance(copy1), sameInstance(copy2)));
    }

    public void testGetPhysicalFileMatchesReportsNonMatchesAsMissing() {
        final var history = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(
            new SnapshotFiles("snap-1", List.of(fileInfo("__0", "_0.cfs", 100L, "checksum-1")), null)
        );
        for (StoreFileMetadata unmatched : List.of(
            storeFile("_1.cfs", 100L, "checksum-1"), // unknown name
            storeFile("_0.cfs", 101L, "checksum-1"), // known name, different length
            storeFile("_0.cfs", 100L, "other") // known name, different checksum
        )) {
            final var matches = physicalFileMatches(history, List.of(unmatched));
            assertThat(matches.existing(), empty());
            assertThat(matches.missing(), contains(sameInstance(unmatched)));
        }
    }

    public void testGetPhysicalFileMatchesWithNoTargetsOrNoHistory() {
        final var history = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(
            new SnapshotFiles("snap-1", List.of(fileInfo("__0", "_0.cfs", 100L, "checksum-1")), null)
        );
        final var noTargets = physicalFileMatches(history, List.of());
        assertThat(noTargets.existing(), empty());
        assertThat(noTargets.missing(), empty());

        final var target = storeFile("_0.cfs", 100L, "checksum-1");
        final var noHistory = physicalFileMatches(BlobStoreIndexShardSnapshots.EMPTY, List.of(target));
        assertThat(noHistory.existing(), empty());
        assertThat(noHistory.missing(), contains(sameInstance(target)));
    }

    /** Matched and unmatched files partition the targets, and the unmatched ones keep the order they were passed in. */
    public void testGetPhysicalFileMatchesPartitionsTargets() {
        final FileInfo known1 = fileInfo("__1", "_1.cfs", 100L, "checksum-1");
        final FileInfo known2 = fileInfo("__2", "_2.cfs", 100L, "checksum-2");
        final var history = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(
            new SnapshotFiles("snap-1", List.of(known1, known2), null)
        );

        final StoreFileMetadata matched1 = storeFile("_1.cfs", 100L, "checksum-1");
        final StoreFileMetadata matched2 = storeFile("_2.cfs", 100L, "checksum-2");
        final StoreFileMetadata missingA = storeFile("_a.cfs", 1L, "a");
        final StoreFileMetadata missingB = storeFile("_b.cfs", 2L, "b");
        final StoreFileMetadata missingC = storeFile("_c.cfs", 3L, "c");

        final var matches = physicalFileMatches(history, List.of(matched1, missingA, matched2, missingB, missingC));
        assertThat(matches.existing(), containsInAnyOrder(sameInstance(known1), sameInstance(known2)));
        assertThat(matches.missing(), contains(sameInstance(missingA), sameInstance(missingB), sameInstance(missingC)));
    }

    /** A file whose contents are inlined in the metadata is the same only if the contents are, so a matching name is not enough. */
    public void testGetPhysicalFileMatchesDoesNotMatchInlinedFilesOnNameAlone() throws IOException {
        final FileInfo known = inlinedFile("x");
        FileInfo differentContents;
        do {
            differentContents = inlinedFile("x");
        } while (differentContents.metadata().hash().equals(known.metadata().hash()));
        assertThat(differentContents.physicalName(), equalTo(known.physicalName()));

        final var history = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(new SnapshotFiles("snap-1", List.of(known), null));
        assertOnlyExisting(history, known.metadata(), sameInstance(known));

        final var matches = physicalFileMatches(history, List.of(differentContents.metadata()));
        assertThat(matches.existing(), empty());
        assertThat(matches.missing(), contains(sameInstance(differentContents.metadata())));
    }

    public void testGetPhysicalFileMatchesRejectsDuplicateStoreFileNames() {
        final var history = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(
            new SnapshotFiles("snap-1", List.of(fileInfo("__0", "_0.cfs", 100L, "checksum-1")), null)
        );
        final var e = expectThrows(
            IllegalStateException.class,
            () -> physicalFileMatches(history, List.of(storeFile("_0.cfs", 100L, "checksum-1"), storeFile("_0.cfs", 100L, "checksum-2")))
        );
        assertThat(e.getMessage(), containsString("_0.cfs"));
    }

    /**
     * Many distinct physical names, as in a long-lived churny shard: exactly the targets that some snapshot references are matched, to
     * their own files, and all the others are missing.
     */
    public void testGetPhysicalFileMatchesWithManyUniqueNames() {
        final int fileCount = between(100, 1000);
        final List<FileInfo> all = new ArrayList<>();
        for (int i = 0; i < fileCount; i++) {
            all.add(fileInfo("__" + i, "_" + i + ".fnm", 10L + i, "checksum-" + i));
        }
        var history = BlobStoreIndexShardSnapshots.EMPTY;
        for (int i = 0; i < between(1, 5); i++) {
            history = history.withAddedSnapshot(new SnapshotFiles("snap-" + i, randomSubsetOf(all), null));
        }
        final Set<FileInfo> referenced = identitySet();
        for (SnapshotFiles snapshot : history.snapshots()) {
            referenced.addAll(snapshot.indexFiles());
        }

        final List<FileInfo> wanted = randomSubsetOf(between(1, 20), all);
        final Set<FileInfo> expectedExisting = identitySet();
        final Set<StoreFileMetadata> expectedMissing = identitySet();
        for (FileInfo file : wanted) {
            if (referenced.contains(file)) {
                expectedExisting.add(file);
            } else {
                expectedMissing.add(file.metadata());
            }
        }

        final var matches = physicalFileMatches(history, wanted.stream().map(FileInfo::metadata).toList());
        final Set<FileInfo> actualExisting = identitySet();
        actualExisting.addAll(matches.existing());
        final Set<StoreFileMetadata> actualMissing = identitySet();
        actualMissing.addAll(matches.missing());
        assertThat(matches.existing().size() + matches.missing().size(), equalTo(wanted.size()));
        assertThat(actualExisting, equalTo(expectedExisting));
        assertThat(actualMissing, equalTo(expectedMissing));
    }

    public void testEmptyRetainsNothing() {
        assertThat(BlobStoreIndexShardSnapshots.EMPTY.retainedBlobBytes(), equalTo(0L));
    }

    /**
     * Snapshots share files and some files are inlined in the metadata. The total counts each shared blob once and skips the inlined files,
     * after additions, a clone, and after deletions that drop files only some snapshots referenced.
     */
    public void testRetainedBlobBytesCountsEachSharedBlobOnce() throws IOException {
        final List<FileInfo> pool = new ArrayList<>();
        for (int i = 0; i < between(5, 30); i++) {
            pool.add(randomBoolean() ? blobFile("__" + i) : inlinedFile("__" + i));
        }

        final Map<String, List<FileInfo>> filesBySnapshot = new HashMap<>();
        final List<SnapshotId> snapshotIds = new ArrayList<>();
        var history = BlobStoreIndexShardSnapshots.EMPTY;
        for (int i = 0; i < between(1, 10); i++) {
            final var snapshotId = new SnapshotId("snap-" + i, randomUUID());
            final List<FileInfo> files = randomSubsetOf(between(0, pool.size()), pool);
            history = history.withAddedSnapshot(new SnapshotFiles(snapshotId.getName(), files, null));
            filesBySnapshot.put(snapshotId.getName(), files);
            snapshotIds.add(snapshotId);
            assertThat(history.retainedBlobBytes(), equalTo(expectedRetainedBlobBytes(filesBySnapshot, snapshotIds)));
        }

        final var source = randomFrom(snapshotIds);
        history = history.withClone(source.getName(), "clone");
        filesBySnapshot.put("clone", filesBySnapshot.get(source.getName()));
        snapshotIds.add(new SnapshotId("clone", randomUUID()));
        assertThat(history.retainedBlobBytes(), equalTo(expectedRetainedBlobBytes(filesBySnapshot, snapshotIds)));

        final List<SnapshotId> survivors = randomSubsetOf(between(0, snapshotIds.size()), snapshotIds);
        history = history.withRetainedSnapshots(new HashSet<>(survivors));
        assertThat(history.retainedBlobBytes(), equalTo(expectedRetainedBlobBytes(filesBySnapshot, survivors)));
    }

    public void testOverflowIsReportedNotWrapped() {
        final var huge = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(
            new SnapshotFiles("snap", List.of(blobFile("__a", Long.MAX_VALUE), blobFile("__b", 1)), null)
        );
        expectThrows(ArithmeticException.class, huge::retainedBlobBytes);
    }

    private static long expectedRetainedBlobBytes(Map<String, List<FileInfo>> filesBySnapshot, List<SnapshotId> retained) {
        final Set<String> seen = new HashSet<>();
        long total = 0;
        for (SnapshotId snapshotId : retained) {
            for (FileInfo file : filesBySnapshot.get(snapshotId.getName())) {
                if (file.metadata().hashEqualsContents() == false && seen.add(file.name())) {
                    total += file.length();
                }
            }
        }
        return total;
    }

    private static FileInfo blobFile(String name) {
        return blobFile(name, between(1, 1_000_000));
    }

    private static FileInfo blobFile(String name, long length) {
        final var metadata = new StoreFileMetadata(
            name + ".cfs",
            length,
            randomAlphaOfLength(8),
            Version.LATEST.toString(),
            null,
            UNAVAILABLE_WRITER_UUID
        );
        return new FileInfo(name, metadata, null);
    }

    /** A file whose whole contents are in the metadata, so there is no blob. It needs a valid Lucene footer for the checksum to verify. */
    private static FileInfo inlinedFile(String name) throws IOException {
        final var buffer = new ByteBuffersDataOutput();
        try (var out = new ByteBuffersIndexOutput(buffer, "inlined", name)) {
            CodecUtil.writeHeader(out, "inlined", 0);
            out.writeBytes(randomByteArrayOfLength(between(1, 100)), 0, 1);
            CodecUtil.writeFooter(out);
        }
        final byte[] contents = buffer.toArrayCopy();
        final String checksum = Store.digestToString(CodecUtil.retrieveChecksum(new ByteArrayIndexInput(name, contents)));
        final var metadata = new StoreFileMetadata(
            name + ".si",
            contents.length,
            checksum,
            Version.LATEST.toString(),
            new BytesRef(contents),
            UNAVAILABLE_WRITER_UUID
        );
        return new FileInfo(name, metadata, null);
    }

    private static <T> Set<T> identitySet() {
        return Collections.newSetFromMap(new IdentityHashMap<>());
    }

    private static FileInfo fileInfo(String name, String physicalName) {
        return fileInfo(name, physicalName, 100L, "checksum");
    }

    private static FileInfo fileInfo(String name, String physicalName, long length, String checksum) {
        return new FileInfo(name, storeFile(physicalName, length, checksum), ByteSizeValue.ofBytes(100));
    }

    private static StoreFileMetadata storeFile(String physicalName, long length, String checksum) {
        return new StoreFileMetadata(physicalName, length, checksum, Version.LATEST.toString());
    }

    /** Adapts a list of target files to the iterator-based API, as the snapshot-create path does. */
    private static BlobStoreIndexShardSnapshots.PhysicalFileMatches physicalFileMatches(
        BlobStoreIndexShardSnapshots history,
        List<StoreFileMetadata> targets
    ) {
        // the size hint only affects allocation, so any value (including a wrong one) must give the same result
        return history.getPhysicalFileMatches(randomIntBetween(0, 2 * targets.size()), targets.iterator());
    }

    /** Asserts that the single target is matched by exactly one existing file, which satisfies the matcher, and nothing is missing. */
    private static void assertOnlyExisting(
        BlobStoreIndexShardSnapshots history,
        StoreFileMetadata target,
        Matcher<? super FileInfo> expected
    ) {
        final var matches = physicalFileMatches(history, List.of(target));
        assertThat(matches.existing(), contains(expected));
        assertThat(matches.missing(), empty());
    }
}
