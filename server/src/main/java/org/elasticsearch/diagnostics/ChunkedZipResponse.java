/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.diagnostics;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

final class ChunkedZipResponse {

    private static final Logger logger = LogManager.getLogger(ChunkedZipResponse.class);

    private record ChunkedZipEntry(ZipEntry zipEntry, ChunkedRestResponseBodyPart firstBodyPart, Releasable releasable) {}

    private BytesStream target;

    private final OutputStream out = new OutputStream() {
        @Override
        public void write(int b) throws IOException {
            assert target != null;
            target.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            assert target != null;
            target.write(b, off, len);
        }
    };

    private final ZipOutputStream zipOutputStream = new ZipOutputStream(out, StandardCharsets.UTF_8);

    private final String filename;
    private final RestChannel restChannel;
    private final Queue<ChunkedZipEntry> entryQueue = new LinkedBlockingQueue<>();
    private final AtomicInteger queueLength = new AtomicInteger();
    private final RefCounted queueRefs = AbstractRefCounted.of(this::clearQueue);
    private final AtomicBoolean isRestResponseFinished = new AtomicBoolean();

    @Nullable // if the first part hasn't been sent yet
    private SubscribableListener<ChunkedRestResponseBodyPart> continuationListener;

    @Nullable // if not currently sending an entry
    private Releasable releasable;

    ChunkedZipResponse(String filename, RestChannel restChannel) {
        this.filename = filename;
        this.restChannel = restChannel;
    }

    public void restResponseFinished() {
        assert Transports.assertTransportThread();
        if (isRestResponseFinished.compareAndSet(false, true)) {
            queueRefs.decRef();
        }
    }

    private void clearQueue() {
        assert isRestResponseFinished.get();
        assert queueRefs.hasReferences() == false;
        while (queueLength.get() > 0) {
            final var newQueueLength = queueLength.decrementAndGet();
            assert newQueueLength >= 0;
            final var entry = entryQueue.poll();
            assert entry != null;
            Releasables.closeExpectNoException(entry.releasable());
        }
        assert entryQueue.isEmpty();
        Releasables.closeExpectNoException(releasable);
    }

    public <T extends ToXContent> ActionListener<T> newXContentListener(String entryName, ActionListener<Void> listener) {
        final var completionListener = new SubscribableListener<Void>();
        completionListener.addListener(listener);
        return newRawListener(entryName, completionListener).map(response -> {
            if (response instanceof RefCounted refCounted) {
                refCounted.mustIncRef();
                completionListener.addListener(ActionListener.releasing(refCounted::decRef));
            }
            return ChunkedRestResponseBodyPart.fromXContent(op -> ChunkedToXContentHelper.singleChunk((b, p) -> {
                b.humanReadable(true);
                final var isFragment = response.isFragment();
                if (isFragment) {
                    b.startObject();
                }
                response.toXContent(b, p);
                if (isFragment) {
                    b.endObject();
                }
                return b;
            }), restChannel.request(), restChannel);
        });
    }

    public <T extends ChunkedToXContent> ActionListener<T> newChunkedXContentListener(String entryName, ActionListener<Void> listener) {
        final var completionListener = new SubscribableListener<Void>();
        completionListener.addListener(listener);
        return newRawListener(entryName, completionListener).map(response -> {
            if (response instanceof RefCounted refCounted) {
                refCounted.mustIncRef();
                completionListener.addListener(ActionListener.releasing(refCounted::decRef));
            }
            return ChunkedRestResponseBodyPart.fromXContent(new ChunkedToXContentObject() {
                @Override
                public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
                    final var isFragment = response.isFragment();
                    return Iterators.concat(
                        ChunkedToXContentHelper.singleChunk((b, p) -> b.humanReadable(true)),
                        isFragment ? ChunkedToXContentHelper.singleChunk((b, p) -> b.startObject()) : Collections.emptyIterator(),
                        response.toXContentChunked(params),
                        isFragment ? ChunkedToXContentHelper.singleChunk((b, p) -> b.endObject()) : Collections.emptyIterator()
                    );
                }

                @Override
                public Iterator<? extends ToXContent> toXContentChunkedV7(ToXContent.Params params) {
                    final var isFragment = response.isFragment();
                    return Iterators.concat(
                        ChunkedToXContentHelper.singleChunk((b, p) -> b.humanReadable(true)),
                        isFragment ? ChunkedToXContentHelper.singleChunk((b, p) -> b.startObject()) : Collections.emptyIterator(),
                        response.toXContentChunkedV7(params),
                        isFragment ? ChunkedToXContentHelper.singleChunk((b, p) -> b.endObject()) : Collections.emptyIterator()
                    );
                }
            }, restChannel.request(), restChannel);
        });
    }

    public ActionListener<ChunkedRestResponseBodyPart> newRawListener(String entryName, ActionListener<Void> listener) {
        final var zipEntry = new ZipEntry(filename + "/" + entryName);
        return ActionListener.assertOnce(new ActionListener<>() {
            @Override
            public void onResponse(ChunkedRestResponseBodyPart firstBodyPart) {
                try {
                    enqueueEntry(
                        zipEntry,
                        firstBodyPart,
                        ActionListener.runAfter(listener, () -> zipEntry.setComment("testing comment set after write"))
                    );
                } catch (Exception e) {
                    enqueueFailureEntry(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                enqueueFailureEntry(e);
            }

            private void enqueueFailureEntry(Exception e) {
                try {
                    enqueueEntry(zipEntry, ChunkedRestResponseBodyPart.fromXContent(op -> ChunkedToXContentHelper.singleChunk((b, p) -> {
                        b.humanReadable(true);
                        b.startObject();
                        ElasticsearchException.generateFailureXContent(
                            b,
                            new ToXContent.DelegatingMapParams(Map.of(ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE, "false"), p),
                            e,
                            true
                        );
                        b.field("status", ExceptionsHelper.status(e).getStatus());
                        b.endObject();
                        return b;
                    }), restChannel.request(), restChannel), listener);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    logger.error(Strings.format("failure when encoding failure response for entry [%s]", entryName), e);
                }
            }
        });
    }

    public void finish(ActionListener<Void> listener) {
        // TODO maybe do this with ref-counting instead?
        enqueueEntry(null, null, listener);
    }

    private void enqueueEntry(ZipEntry zipEntry, ChunkedRestResponseBodyPart firstBodyPart, ActionListener<Void> listener) {
        if (isRestResponseFinished.get() == false && queueRefs.tryIncRef()) {
            try {
                entryQueue.add(new ChunkedZipEntry(zipEntry, firstBodyPart, () -> listener.onResponse(null)));
                if (queueLength.getAndIncrement() == 0) {
                    final var nextEntry = entryQueue.poll();
                    assert nextEntry != null;
                    final var continuation = new QueueConsumer(nextEntry.zipEntry(), nextEntry.firstBodyPart());
                    assert releasable == null;
                    releasable = nextEntry.releasable();
                    final var currentContinuationListener = continuationListener;
                    continuationListener = new SubscribableListener<>();
                    if (currentContinuationListener == null) {
                        final var restResponse = RestResponse.chunked(RestStatus.OK, continuation, this::restResponseFinished);
                        restResponse.addHeader("content-disposition", Strings.format("attachment; filename=\"%s.zip\"", filename));
                        restChannel.sendResponse(restResponse);
                    } else {
                        currentContinuationListener.onResponse(continuation);
                    }
                }
            } finally {
                queueRefs.decRef();
            }
        } else {
            listener.onFailure(new AlreadyClosedException("response already closed"));
        }
    }

    private void transferReleasable(Collection<Releasable> releasables) {
        if (releasable != null && isRestResponseFinished.get() == false && queueRefs.tryIncRef()) {
            try {
                releasables.add(releasable);
                releasable = null;
            } finally {
                queueRefs.decRef();
            }
        }
    }

    private final class QueueConsumer implements ChunkedRestResponseBodyPart {

        private ZipEntry zipEntry;
        private ChunkedRestResponseBodyPart bodyPart;
        private boolean isPartComplete;
        private boolean isLastPart;
        private Consumer<ActionListener<ChunkedRestResponseBodyPart>> getNextPart;

        QueueConsumer(ZipEntry zipEntry, ChunkedRestResponseBodyPart bodyPart) {
            this.zipEntry = zipEntry;
            this.bodyPart = bodyPart;
        }

        @Override
        public boolean isPartComplete() {
            return isPartComplete;
        }

        @Override
        public boolean isLastPart() {
            return isLastPart;
        }

        @Override
        public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
            assert getNextPart != null;
            getNextPart.accept(listener);
        }

        @Override
        public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
            final List<Releasable> releasables = new ArrayList<>();
            try {
                final RecyclerBytesStreamOutput chunkStream = new RecyclerBytesStreamOutput(recycler);
                assert target == null;
                target = chunkStream;

                do {
                    try {
                        if (bodyPart == null) {
                            // no more entries
                            assert zipEntry == null;
                            zipOutputStream.finish();
                            isPartComplete = true;
                            isLastPart = true;
                            transferReleasable(releasables);
                        } else if (zipEntry != null) {
                            // new entry, so write the entry header
                            zipOutputStream.putNextEntry(zipEntry);
                            zipEntry = null;
                        } else {
                            // writing entry body
                            if (bodyPart.isPartComplete() == false) {
                                try (var innerChunk = bodyPart.encodeChunk(sizeHint, recycler)) {
                                    final var iterator = innerChunk.iterator();
                                    BytesRef bytesRef;
                                    while ((bytesRef = iterator.next()) != null) {
                                        zipOutputStream.write(bytesRef.bytes, bytesRef.offset, bytesRef.length);
                                    }
                                }
                            }
                            if (bodyPart.isPartComplete()) {
                                if (bodyPart.isLastPart()) {
                                    zipOutputStream.closeEntry();
                                    transferReleasable(releasables);
                                    final var newQueueLength = queueLength.decrementAndGet();
                                    if (newQueueLength == 0) {
                                        // next entry isn't available yet, so we stop iterating
                                        isPartComplete = true;
                                        assert getNextPart == null;
                                        getNextPart = continuationListener::addListener;
                                    } else {
                                        // next entry is immediately available so start sending its chunks too
                                        if (isRestResponseFinished.get() == false && queueRefs.tryIncRef()) {
                                            try {
                                                final var nextEntry = entryQueue.poll();
                                                assert nextEntry != null;
                                                zipEntry = nextEntry.zipEntry();
                                                bodyPart = nextEntry.firstBodyPart();
                                                releasable = nextEntry.releasable();
                                            } finally {
                                                queueRefs.decRef();
                                            }
                                        }
                                    }
                                } else {
                                    // this body part has a continuation, for which we must wait
                                    isPartComplete = true;
                                    assert getNextPart == null;
                                    getNextPart = l -> bodyPart.getNextPart(l.map(p -> new QueueConsumer(null, p)));
                                }
                            }
                        }
                    } finally {
                        zipOutputStream.flush();
                    }
                } while (isPartComplete == false && chunkStream.size() < sizeHint);

                final Releasable chunkStreamReleasable = () -> Releasables.closeExpectNoException(chunkStream);
                final var result = new ReleasableBytesReference(
                    chunkStream.bytes(),
                    releasables.isEmpty()
                        ? chunkStreamReleasable
                        : Releasables.wrap(Iterators.concat(Iterators.single(chunkStreamReleasable), releasables.iterator()))
                );

                target = null;
                return result;
            } catch (Exception e) {
                logger.error("failure encoding chunk", e);
                throw e;
            } finally {
                if (target != null) {
                    assert false : "failure encoding chunk";
                    IOUtils.closeWhileHandlingException(target, Releasables.wrap(releasables));
                    target = null;
                }
            }
        }

        @Override
        public String getResponseContentTypeString() {
            return "application/zip";
        }
    }
}
