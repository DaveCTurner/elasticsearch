/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine.translog;

import org.apache.lucene.internal.hppc.LongArrayList;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.LongsRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

class ShardSyncState {

    private static final Logger logger = LogManager.getLogger(ShardSyncState.class);

    private final ShardId shardId;
    private final long startingPrimaryTerm;
    private final LongSupplier currentPrimaryTerm;
    private final Consumer<LongsRef> persistedSeqNosConsumer;
    private final ThreadContext threadContext;
    private final PriorityQueue<SyncListener> listeners = new PriorityQueue<>();
    private final TreeMap<Long, TranslogReplicator.BlobTranslogFile> translogFiles = new TreeMap<>();
    private long markedTranslogReleaseEndFile = -1;
    private long markedTranslogDeleteGeneration = -1;
    private volatile Translog.Location processedLocation = new Translog.Location(0, 0, 0);
    private volatile Translog.Location syncedLocation = new Translog.Location(0, 0, 0);
    private volatile boolean isClosed = false;
    // Set once the live primary term moves past startingPrimaryTerm. Every marker this state produces carries
    // startingPrimaryTerm, so a sync queued after this can never be completed by notifyListeners.
    private boolean primaryTermAdvanced = false;

    ShardSyncState(
        ShardId shardId,
        long primaryTerm,
        LongSupplier currentPrimaryTerm,
        Consumer<LongsRef> persistedSeqNosConsumer,
        ThreadContext threadContext
    ) {
        this.shardId = shardId;
        this.startingPrimaryTerm = primaryTerm;
        this.currentPrimaryTerm = currentPrimaryTerm;
        this.persistedSeqNosConsumer = persistedSeqNosConsumer;
        this.threadContext = threadContext;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public long getStartingPrimaryTerm() {
        return startingPrimaryTerm;
    }

    static AlreadyClosedException alreadyClosedException(ShardId shardId) {
        return new AlreadyClosedException("The translog for shard [" + shardId + "] is already closed.");
    }

    static AlreadyClosedException primaryTermAdvancedException(ShardId shardId) {
        return new AlreadyClosedException(
            "The translog for shard [" + shardId + "] can no longer be synced because its primary term is stale"
        );
    }

    boolean syncNeeded() {
        return processedLocation.compareTo(syncedLocation) > 0;
    }

    boolean waitForAllSynced(ActionListener<Void> listener) {
        // Single volatile read
        Translog.Location processedLocationCopy = processedLocation;
        Translog.Location syncedLocationCopy = syncedLocation;
        logger.info(
            "TEMP #163242 waitForAllSynced[{}]: processedLocation[{}] syncedLocation[{}]",
            shardId,
            processedLocationCopy,
            syncedLocationCopy
        );
        if (processedLocationCopy.compareTo(syncedLocationCopy) > 0) {
            logger.info(
                "TEMP #163242 waitForAllSynced[{}]: processedLocation[{}] syncedLocation[{}] calling ensureSynced",
                shardId,
                processedLocationCopy,
                syncedLocationCopy
            );
            return ensureSynced(processedLocationCopy, listener);
        } else {
            if (isClosed) {
                logger.info(
                    "TEMP #163242 waitForAllSynced[{}]: processedLocation[{}] syncedLocation[{}] already closed",
                    shardId,
                    processedLocationCopy,
                    syncedLocationCopy
                );
                listener.onFailure(alreadyClosedException(shardId));
            } else {
                logger.info(
                    "TEMP #163242 waitForAllSynced[{}]: processedLocation[{}] syncedLocation[{}] already synced",
                    shardId,
                    processedLocationCopy,
                    syncedLocationCopy
                );
                listener.onResponse(null);
            }
            return true;
        }
    }

    boolean ensureSynced(Translog.Location location, ActionListener<Void> listener) {
        assert location.compareTo(processedLocation) <= 0;
        boolean completeListener = true;
        boolean alreadyClosed = false;
        boolean termAdvanced = false;
        List<ActionListener<Void>> supersededListeners = null;
        final var syncedLocationCopy1 = syncedLocation;
        if (location.compareTo(syncedLocationCopy1) > 0) {
            synchronized (listeners) {
                if (isClosed) {
                    alreadyClosed = true;
                } else {
                    final var syncedLocationCopy2 = syncedLocation;
                    if (location.compareTo(syncedLocationCopy2) > 0) {
                        if (primaryTermAdvanced || startingPrimaryTerm != currentPrimaryTerm()) {
                            // Same lock that failListenersOnNewPrimaryTerm uses to publish primaryTermAdvanced, so a listener
                            // cannot be queued after the primary term has moved and then wait for a notification that will never arrive.
                            primaryTermAdvanced = true;
                            termAdvanced = true;
                            supersededListeners = new ArrayList<>(listeners);
                            listeners.clear();
                        } else {
                            logger.info(
                                "TEMP #163242 ensureSynced[{}]: location[{}] syncedLocation[{}][{}] registering listener",
                                shardId,
                                location,
                                syncedLocationCopy1,
                                syncedLocationCopy2
                            );
                            ContextPreservingActionListener<Void> contextPreservingActionListener = ContextPreservingActionListener
                                .wrapPreservingContext(listener, threadContext);
                            listeners.add(new SyncListener(location, contextPreservingActionListener));
                            completeListener = false;
                        }
                    }
                }
            }
        }

        if (completeListener) {
            if (alreadyClosed) {
                logger.info(
                    "TEMP #163242 ensureSynced[{}]: location[{}] syncedLocation[{}] already closed",
                    shardId,
                    location,
                    syncedLocationCopy1
                );
                listener.onFailure(alreadyClosedException(shardId));
            } else if (termAdvanced) {
                try {
                    listener.onFailure(primaryTermAdvancedException(shardId));
                } finally {
                    ActionListener.onFailure(supersededListeners, primaryTermAdvancedException(shardId));
                }
            } else {
                logger.info(
                    "TEMP #163242 ensureSynced[{}]: location[{}] syncedLocation[{}] already synced",
                    shardId,
                    location,
                    syncedLocationCopy1
                );
                listener.onResponse(null);
            }
            return true;
        } else {
            return false;
        }
    }

    public void markSyncStarting(long primaryTerm, TranslogReplicator.BlobTranslogFile translogFile) {
        // A marker from an older primary term cannot satisfy sync listeners. They are failed when cluster-state validation
        // finishes; see failListenersOnNewPrimaryTerm.
        if (primaryTerm == currentPrimaryTerm.getAsLong()) {
            synchronized (translogFiles) {
                // Since this is call before initiating an upload, the marked translog release end file should never be less than the
                // recovery start file
                assert translogFile.generation() >= markedTranslogReleaseEndFile
                    : translogFile.generation()
                        + " < "
                        + markedTranslogReleaseEndFile
                        + " for shard "
                        + shardId
                        + " of primaryTerm ["
                        + primaryTerm
                        + "] and translogFile ["
                        + translogFile
                        + "]";
                assert translogFiles.keySet().stream().allMatch(l -> l < translogFile.generation());
                if (isClosed == false) {
                    // Add if the shard is open. Ignore if the shard is closed. We cannot safely decrement as we don't know if the file will
                    // be needed for a different recovery
                    translogFiles.put(translogFile.generation(), translogFile);
                }
            }
        } else {
            // Just decrement since this was sync was generated in a different primary term
            translogFile.decRef();
        }
    }

    public boolean markSyncFinished(SyncMarker syncMarker) {
        // Every marker this state produces carries startingPrimaryTerm, so once the live term has moved on, notifyListeners
        // will not run for the queued syncs. The caller fails them via failListenersOnNewPrimaryTerm.
        if (syncMarker.primaryTerm() == currentPrimaryTerm()) {
            assert syncMarker.location().compareTo(syncedLocation) > 0;
            // We mark the seqNos of persisted before exposing the synced location. This matches what we do in the TranlogWriter.
            // Some assertions in TransportVerifyShardBeforeCloseAction depend on the seqNos marked as persisted before the sync is exposed.
            final LongArrayList syncedSeqNos = syncMarker.syncedSeqNos();
            persistedSeqNosConsumer.accept(new LongsRef(syncedSeqNos.buffer, 0, syncedSeqNos.size()));
            syncedLocation = syncMarker.location();
            return true;
        } else {
            return false;
        }
    }

    public void markCommitUploaded(long translogReleaseEndFile) {
        synchronized (translogFiles) {
            if (isClosed == false) {
                if (translogReleaseEndFile > markedTranslogReleaseEndFile) {
                    for (TranslogReplicator.BlobTranslogFile file : translogFiles.subMap(
                        markedTranslogReleaseEndFile,
                        translogReleaseEndFile
                    ).values()) {
                        file.decRef();
                    }
                    markedTranslogReleaseEndFile = translogReleaseEndFile;
                }
            }
        }
    }

    public void markTranslogDeleted(long translogGeneration) {
        synchronized (translogFiles) {
            if (isClosed == false) {
                markedTranslogDeleteGeneration = Math.max(translogGeneration, markedTranslogDeleteGeneration);
                if (markedTranslogDeleteGeneration == translogGeneration) {
                    // Remove all files prior to this generation. There is a possibility that lower generation files may still be
                    // referenced by other shards. For example imagine these translog files: [0, 1]. Shard A has not committed yet
                    // and has ops in generation 0. Shard B has committed with a translogStartFile=2 and has ops in [0, 1]. Shard B has
                    // decremented its references to 0 and 1. With this file 1 is deleted and cluster consistency passed. Shard B can now
                    // remove its reference to both file 0 and 1, even though file 0 is hanging around for Shard A.
                    NavigableMap<Long, TranslogReplicator.BlobTranslogFile> toRemove = translogFiles.headMap(translogGeneration, true);
                    toRemove.clear();
                } else {
                    assert translogFiles.headMap(translogGeneration, true).isEmpty();
                }
            }
        }
    }

    public long currentPrimaryTerm() {
        return currentPrimaryTerm.getAsLong();
    }

    void notifyListeners() {
        var toComplete = new ArrayList<ActionListener<Void>>();
        logger.info("TEMP #163242 notifyListeners[{}] up to [{}]", shardId, syncedLocation);
        synchronized (listeners) {
            SyncListener listener;
            while ((listener = listeners.peek()) != null && syncedLocation.compareTo(listener.location) >= 0) {
                logger.info("TEMP #163242 notifyListeners[{}] completing listener at [{}]", shardId, listener.location);
                toComplete.add(listener);
                listeners.poll();
            }
        }
        ActionListener.onResponse(toComplete, null);
    }

    void failPendingListeners(Exception e) {
        logger.info("TEMP #163242 failPendingListeners[{}] start", shardId);
        var toFail = new ArrayList<ActionListener<Void>>();
        synchronized (listeners) {
            toFail.addAll(listeners);
            listeners.clear();
        }
        logger.info("TEMP #163242 failPendingListeners[{}] after clearing listeners", shardId);
        ActionListener.onFailure(toFail, e);
    }

    // Fails every queued sync listener and rejects syncs registered later. Called when an upload's primary term no longer matches
    // the live term, which means notifyListeners will not complete those listeners.
    void failListenersOnNewPrimaryTerm() {
        synchronized (listeners) {
            primaryTermAdvanced = true;
        }
        failPendingListeners(primaryTermAdvancedException(shardId));
    }

    public void updateProcessedLocation(Translog.Location newProcessedLocation) {
        assert newProcessedLocation.compareTo(processedLocation) > 0;
        processedLocation = newProcessedLocation;
    }

    public TranslogMetadata.Directory createDirectory(long generation, long currentOperations) {
        final int[] referencedTranslogFileOffsets;
        long estimatedOps = currentOperations;
        synchronized (translogFiles) {
            referencedTranslogFileOffsets = new int[translogFiles.size()];
            int i = 0;
            for (TranslogReplicator.BlobTranslogFile referencedFile : translogFiles.values()) {
                estimatedOps += referencedFile.getTotalOps(shardId);
                referencedTranslogFileOffsets[i] = Math.toIntExact(generation - referencedFile.generation());
                assert referencedTranslogFileOffsets[i] > 0 : generation + " " + referencedFile.generation();
                ++i;
            }
        }

        return new TranslogMetadata.Directory(estimatedOps, referencedTranslogFileOffsets);
    }

    public boolean isClosed() {
        return isClosed;
    }

    public void close() {
        isClosed = true;
        synchronized (translogFiles) {
            // The relocation hand-off forces a flush while holding the operation permits to a clean relocation should fully release the
            // files. If we get here and there are still referenced files we must mark them as unsafe for delete. This is because this shard
            // may be closed exceptionally and the files will still be necessary for a recovery.
            translogFiles.tailMap(markedTranslogReleaseEndFile, true).forEach((ignored, blobTranslogFile) -> {
                blobTranslogFile.markUnsafeForDelete(shardId);
                blobTranslogFile.decRef();
            });
            translogFiles.clear();
        }
        failPendingListeners(alreadyClosedException(shardId));
    }

    private record SyncListener(Translog.Location location, ActionListener<Void> listener)
        implements
            ActionListener<Void>,
            Comparable<SyncListener> {

        @Override
        public void onResponse(Void unused) {
            listener.onResponse(unused);
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }

        @Override
        public int compareTo(SyncListener o) {
            return location.compareTo(o.location);
        }
    }

    record SyncMarker(long primaryTerm, Translog.Location location, LongArrayList syncedSeqNos) {}

}
