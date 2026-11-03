/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.store.input;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.cache.blob.BlobStoreCacheService;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheFile;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService;
import org.elasticsearch.xpack.searchablesnapshots.store.IndexInputStats;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_BLOB_CACHE_INDEX;
import static org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService.resolveSnapshotCache;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

/**
 * {@link IndexInput#readByte()} on a searchable snapshot reaches {@code MetadataCachingIndexInput#fillIndexCache} when the bytes fall in
 * the blob-cache header and the blob cache misses. These tests fail the fetch after that fill has been opened.
 */
public class MetadataCachingIndexInputTests extends AbstractSearchableSnapshotsTestCase {

    private static final long LENGTH = 64L;
    private static final String FILE_NAME = "cachefill.si";
    private static final String BLOB_NAME = "blob";

    /**
     * The cache file is evicted on the fetch thread before {@code readIfAvailableOrPending} runs, so that call fails before the reader and
     * returns a non-null future.
     */
    public void testReadReleasesIndexCacheFillWhenCacheFileEvictedDuringFetch() throws Exception {
        assertFillReleasedWhenBlobReadFails(true);
    }

    /**
     * The header range is still pending when the fill is registered. The blob read then fails, so the reader that would pass the releasable
     * to {@code putCachedBlob} never runs.
     */
    public void testReadReleasesIndexCacheFillWhenPendingBlobReadFails() throws Exception {
        assertFillReleasedWhenBlobReadFails(false);
    }

    private void assertFillReleasedWhenBlobReadFails(boolean evictCacheFile) throws Exception {
        final SnapshotId snapshotId = new SnapshotId("_name", "_uuid");
        final IndexId indexId = new IndexId("_name", "_uuid");
        final ShardId shardId = new ShardId("_name", "_uuid", 0);
        final AtomicReference<TestDirectory> directoryRef = new AtomicReference<>();
        // Released once the read is blocked in Future.get() with the index-cache fill already registered.
        final CountDownLatch failFetch = new CountDownLatch(1);
        final BlobContainer blobContainer = new FilterBlobContainer(TestUtils.singleBlobContainer(BLOB_NAME, new byte[(int) LENGTH])) {
            @Override
            protected BlobContainer wrapChild(BlobContainer child) {
                return child;
            }

            @Override
            public InputStream readBlob(OperationPurpose purpose, String name, long position, long length) throws IOException {
                if (evictCacheFile) {
                    evict(directoryRef.get());
                } else {
                    try {
                        if (failFetch.await(10, TimeUnit.SECONDS) == false) {
                            throw new IOException("timed out waiting to fail the blob read");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("interrupted waiting to fail the blob read", e);
                    }
                }
                throw new IOException("failed to read blob [" + name + "]");
            }
        };

        final CacheService cacheService = defaultCacheService();
        TestDirectory directory = null;
        try {
            final Path shardDir = randomShardPath(shardId);
            final ShardPath shardPath = new ShardPath(false, shardDir, shardDir, shardId);
            final Path cacheDir = Files.createDirectories(resolveSnapshotCache(shardDir).resolve(snapshotId.getUUID()));
            final Executor cacheFetchExecutor = evictCacheFile
                ? Runnable::run
                : threadPool.executor(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);
            directory = new TestDirectory(
                () -> blobContainer,
                snapshotId,
                indexId,
                shardId,
                cacheService,
                cacheDir,
                shardPath,
                cacheFetchExecutor
            );
            directoryRef.set(directory);
            cacheService.start();

            final PlainActionFuture<Void> prewarm = new PlainActionFuture<>();
            assertTrue(directory.loadSnapshot(createRecoveryState(false), () -> false, prewarm));
            prewarm.actionGet();

            try (IndexInput input = directory.openInput(FILE_NAME, IOContext.DEFAULT)) {
                assertThat(input, instanceOf(CachedBlobContainerIndexInput.class));
                final AtomicReference<Exception> readFailure = new AtomicReference<>();
                // Named as a test thread so cache-fetch assertions accept the direct executor used when evicting.
                final Thread readerThread = new Thread(() -> {
                    try {
                        input.readByte();
                    } catch (Exception e) {
                        readFailure.set(e);
                    }
                }, "TEST-metadata-caching-index-input");
                readerThread.start();
                try {
                    if (evictCacheFile == false) {
                        final TestDirectory openDirectory = directory;
                        assertBusy(() -> {
                            assertThat(openDirectory.stats.getCurrentIndexCacheFills(), equalTo(1L));
                            assertThat(isWaiting(readerThread), equalTo(true));
                        });
                    }
                    failFetch.countDown();
                    readerThread.join(TimeUnit.SECONDS.toMillis(10));
                    assertThat(readFailure.get(), instanceOf(IOException.class));
                } finally {
                    failFetch.countDown();
                    readerThread.join(TimeUnit.SECONDS.toMillis(10));
                }
            }

            assertThat(directory.stats.fillsStarted, equalTo(1));
            assertThat(
                "onCacheFillComplete must be released even though the blob was not cached",
                directory.stats.getCurrentIndexCacheFills(),
                equalTo(0L)
            );
        } finally {
            failFetch.countDown();
            IOUtils.close(directory, cacheService);
            assertThreadPoolNotBusy(threadPool);
        }
    }

    private static void evict(TestDirectory directory) throws IOException {
        try {
            final CacheFile cacheFile = directory.getCacheFile(directory.createCacheKey(FILE_NAME), LENGTH);
            cacheFile.startEviction();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private static boolean isWaiting(Thread thread) {
        final Thread.State state = thread.getState();
        return state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING;
    }

    private final class TestDirectory extends SearchableSnapshotDirectory {

        private final Executor cacheFetchExecutor;
        private final TrackingIndexInputStats stats = new TrackingIndexInputStats(() -> 0L);

        TestDirectory(
            Supplier<BlobContainer> blobContainer,
            SnapshotId snapshotId,
            IndexId indexId,
            ShardId shardId,
            CacheService cacheService,
            Path cacheDir,
            ShardPath shardPath,
            Executor cacheFetchExecutor
        ) {
            super(
                blobContainer,
                () -> new BlobStoreIndexShardSnapshot(
                    snapshotId.getName(),
                    List.of(
                        new FileInfo(
                            BLOB_NAME,
                            new StoreFileMetadata(FILE_NAME, LENGTH, "_checksum", IndexVersion.current().luceneVersion().toString()),
                            ByteSizeValue.ofBytes(LENGTH)
                        )
                    ),
                    0L,
                    0L,
                    0,
                    0L
                ),
                new MissBlobStoreCacheService(),
                "_repo",
                snapshotId,
                indexId,
                shardId,
                Settings.builder()
                    .put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), true)
                    .put(SearchableSnapshots.SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING.getKey(), false)
                    .put(SearchableSnapshotsSettings.SNAPSHOT_PARTIAL_SETTING.getKey(), false)
                    .build(),
                () -> 0L,
                cacheService,
                cacheDir,
                shardPath,
                threadPool,
                null
            );
            this.cacheFetchExecutor = cacheFetchExecutor;
        }

        @Override
        public Executor cacheFetchAsyncExecutor() {
            return cacheFetchExecutor;
        }

        @Override
        protected IndexInputStats createIndexInputStats(long numFiles, long totalSize, long minSize, long maxSize) {
            return stats;
        }
    }

    /**
     * Keeps the real header-range calculation. A cache get fails as an index miss, which {@code fillBlobCacheIndex} still treats as a miss.
     */
    private static final class MissBlobStoreCacheService extends BlobStoreCacheService {

        MissBlobStoreCacheService() {
            super(mock(Client.class), SNAPSHOT_BLOB_CACHE_INDEX);
        }

        @Override
        protected void innerGet(
            org.elasticsearch.action.get.GetRequest request,
            ActionListener<org.elasticsearch.action.get.GetResponse> listener
        ) {
            listener.onFailure(new IndexNotFoundException(request.index()));
        }

        @Override
        protected void innerPut(
            org.elasticsearch.action.index.IndexRequest request,
            ActionListener<org.elasticsearch.action.DocWriteResponse> listener
        ) {
            listener.onFailure(new IndexNotFoundException(request.index()));
        }
    }

    private static final class TrackingIndexInputStats extends IndexInputStats {

        private int fillsStarted;

        TrackingIndexInputStats(LongSupplier currentTimeNanos) {
            super(1L, LENGTH, LENGTH, LENGTH, currentTimeNanos);
        }

        @Override
        public Releasable addIndexCacheFill() {
            fillsStarted++;
            return super.addIndexCacheFill();
        }
    }
}
