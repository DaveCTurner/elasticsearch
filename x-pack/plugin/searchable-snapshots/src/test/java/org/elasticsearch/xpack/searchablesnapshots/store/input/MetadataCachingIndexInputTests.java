/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.store.input;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Version;
import org.elasticsearch.blobcache.common.BlobCacheBufferedIndexInput;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheFile;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheKey;
import org.elasticsearch.xpack.searchablesnapshots.store.IndexInputStats;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

/**
 * Tests that {@link MetadataCachingIndexInput#fillIndexCache} releases the {@link IndexInputStats#addIndexCacheFill()} releasable on every
 * path, including when the cache file cannot be read and the blob is never stored.
 */
public class MetadataCachingIndexInputTests extends ESTestCase {

    private static final long LENGTH = 64L;

    private static final CacheKey CACHE_KEY = new CacheKey("_snap_uuid", "_snap_index", new ShardId("_name", "_uuid", 0), "_filename");

    private static final CacheFile.ModificationListener NOOP_LISTENER = new CacheFile.ModificationListener() {
        @Override
        public void onCacheFileNeedsFsync(CacheFile cacheFile) {}

        @Override
        public void onCacheFileDelete(CacheFile cacheFile) {}
    };

    /**
     * {@code readIfAvailableOrPending} fails before the reader runs (the cache file is already evicted) and returns a failed future.
     * {@code fillIndexCache} only closes the fill when that future is null.
     */
    public void testFillReleasedWhenCacheFileAlreadyEvicted() throws Exception {
        final TrackingIndexInputStats stats = new TrackingIndexInputStats();
        final MetadataCachingIndexInput input = newIndexInput(stats);
        final CacheFile cacheFile = newCacheFile(LENGTH);
        final CacheFile.EvictionListener listener = ignored -> {};
        cacheFile.acquire(listener);
        cacheFile.startEviction();
        cacheFile.release(listener);

        input.fillIndexCache(cacheFile, ByteRange.of(0L, LENGTH));

        assertFillOpenedAndReleased(stats);
    }

    /**
     * The range is pending when the fill is registered, then the pending population fails. The reader, which is what hands the releasable
     * to {@code putCachedBlob}, is only invoked when the range becomes readable.
     */
    public void testFillReleasedWhenPendingRangeFails() throws Exception {
        final TrackingIndexInputStats stats = new TrackingIndexInputStats();
        final MetadataCachingIndexInput input = newIndexInput(stats);
        final CacheFile cacheFile = newCacheFile(LENGTH);
        final CacheFile.EvictionListener listener = ignored -> {};
        cacheFile.acquire(listener);
        try {
            final ByteRange range = ByteRange.of(0L, LENGTH);
            final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
            final Future<Integer> populateFuture = cacheFile.populateAndRead(
                range,
                range,
                channel -> Math.toIntExact(LENGTH),
                (channel, from, to, progressUpdater) -> {
                    throw new IOException("failed to read blob");
                },
                taskQueue.getThreadPool().generic()
            );

            input.fillIndexCache(cacheFile, range);
            assertThat("fill must be open while the range is still pending", stats.getCurrentIndexCacheFills(), equalTo(1L));

            taskQueue.runAllTasks();
            final ExecutionException failure = expectThrows(ExecutionException.class, populateFuture::get);
            assertThat(failure.getCause(), instanceOf(IOException.class));

            assertFillOpenedAndReleased(stats);
        } finally {
            cacheFile.release(listener);
        }
    }

    /**
     * The range is already present, so the reader runs, but reading the cache file fails before {@code putCachedBlob}. That exception is
     * captured by the range listener and never closes the fill.
     */
    public void testFillReleasedWhenReadFailsBeforeCachingBlob() throws Exception {
        final TrackingIndexInputStats stats = new TrackingIndexInputStats();
        final MetadataCachingIndexInput input = newIndexInput(stats);
        final ByteRange range = ByteRange.of(0L, LENGTH);
        // The tracker considers the range present, but the file on disk is empty, so the reader hits EOF.
        final CacheFile cacheFile = newCacheFile(LENGTH, range);
        final CacheFile.EvictionListener listener = ignored -> {};
        cacheFile.acquire(listener);
        try {
            input.fillIndexCache(cacheFile, range);
            assertFillOpenedAndReleased(stats);
        } finally {
            cacheFile.release(listener);
        }
    }

    private static void assertFillOpenedAndReleased(TrackingIndexInputStats stats) {
        assertThat(stats.fillsStarted, equalTo(1));
        assertThat(
            "onCacheFillComplete must be released even though the blob was not cached",
            stats.getCurrentIndexCacheFills(),
            equalTo(0L)
        );
    }

    private static MetadataCachingIndexInput newIndexInput(IndexInputStats stats) {
        final FileInfo fileInfo = new FileInfo(
            "blob",
            new StoreFileMetadata("test.si", LENGTH, "0", Version.LATEST.toString()),
            ByteSizeValue.ofBytes(LENGTH)
        );
        return new MetadataCachingIndexInput(
            LogManager.getLogger(MetadataCachingIndexInputTests.class),
            "test.si",
            mock(SearchableSnapshotDirectory.class),
            fileInfo,
            IOContext.DEFAULT,
            stats,
            0L,
            0L,
            LENGTH,
            null,
            BlobCacheBufferedIndexInput.BUFFER_SIZE,
            BlobCacheBufferedIndexInput.BUFFER_SIZE,
            ByteRange.EMPTY,
            ByteRange.EMPTY
        ) {
            @Override
            protected void readWithoutBlobCache(ByteBuffer b, long position) {
                throw new AssertionError("not used");
            }

            @Override
            protected MetadataCachingIndexInput doSlice(
                String sliceName,
                long sliceOffset,
                long sliceLength,
                ByteRange sliceHeaderByteRange,
                ByteRange sliceFooterByteRange,
                long sliceCompoundFileOffset
            ) {
                throw new AssertionError("not used");
            }
        };
    }

    private static CacheFile newCacheFile(long length) throws IOException {
        return newCacheFile(length, null);
    }

    private static CacheFile newCacheFile(long length, ByteRange alreadyPresent) throws IOException {
        final Path file = createTempDir().resolve("cache");
        if (alreadyPresent == null) {
            // The file is created on acquire. This constructor requires that it does not exist yet.
            return new CacheFile(CACHE_KEY, length, file, NOOP_LISTENER);
        }
        Files.createFile(file);
        final TreeSet<ByteRange> ranges = new TreeSet<>();
        ranges.add(alreadyPresent);
        return new CacheFile(CACHE_KEY, length, file, ranges, NOOP_LISTENER);
    }

    private static class TrackingIndexInputStats extends IndexInputStats {

        private int fillsStarted;

        TrackingIndexInputStats() {
            super(1L, LENGTH, LENGTH, LENGTH, () -> 0L);
        }

        @Override
        public Releasable addIndexCacheFill() {
            fillsStarted++;
            return super.addIndexCacheFill();
        }
    }
}
