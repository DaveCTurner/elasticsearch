/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.StreamingXContentResponse;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a (possibly-streaming) response to the repository-verify-integrity API.
 */
class RepositoryVerifyIntegrityResponseStream extends AbstractRefCounted {
    // ref-counting discipline:
    // - one ref added at creation in the REST layer and released there by the listener returned from getCompletionListener()
    // - one ref held for every response chunk while it is being added to the fragment queue
    // thus when all refs are released the transport-layer coordinating action is complete and no more trailing fragments can be added,
    // so we can send the last response fragment.

    private static final Logger logger = LogManager.getLogger(RepositoryVerifyIntegrityResponseStream.class);

    private final RestChannel restChannel;

    @Nullable // if still running, or completed without an exception
    private volatile Exception finalException;

    @Nullable // if still running, or completed exceptionally
    private volatile RepositoryVerifyIntegrityResponse finalResult;

    @Nullable // if not yet started
    private volatile StreamingXContentResponse streamingXContentResponse;

    private final AtomicLong anomalyCount = new AtomicLong();

    RepositoryVerifyIntegrityResponseStream(RestChannel restChannel) {
        this.restChannel = restChannel;
    }

    void startResponse(Releasable releasable) throws IOException {
        assert hasReferences();
        assert streamingXContentResponse == null;
        streamingXContentResponse = new StreamingXContentResponse(restChannel, restChannel.request(), () -> {});
        streamingXContentResponse.writeFragment(
            p0 -> ChunkedToXContentHelper.singleChunk((b, p) -> b.startObject().startArray("log")),
            releasable
        );
    }

    void writeChunk(RepositoryVerifyIntegrityResponseChunk chunk, Releasable releasable) {
        assert hasReferences();
        assert streamingXContentResponse != null;
        assert finalResult == null;
        // finalException might be set here due to e.g. a network disconnect

        if (chunk.type() == RepositoryVerifyIntegrityResponseChunk.Type.ANOMALY) {
            anomalyCount.incrementAndGet();
        }
        streamingXContentResponse.writeFragment(
            p0 -> ChunkedToXContentHelper.singleChunk((b, p) -> b.startObject().value(chunk, p).endObject()),
            releasable
        );
    }

    @Override
    protected void closeInternal() {
        try {
            assert (finalResult == null) != (finalException == null);
            assert finalResult == null || streamingXContentResponse != null;

            if (finalResult != null) {
                // success - finish the response with the final results
                assert streamingXContentResponse != null;
                assert finalException == null;
                streamingXContentResponse.writeFragment(
                    p0 -> ChunkedToXContentHelper.singleChunk(
                        (b, p) -> b.endArray()
                            .startObject("results")
                            .field("original_repository_generation", finalResult.originalRepositoryGeneration())
                            .field("final_repository_generation", finalResult.finalRepositoryGeneration())
                            .field("total_anomalies", anomalyCount.get())
                            .field(
                                "result",
                                anomalyCount.get() == 0
                                    ? finalResult.originalRepositoryGeneration() == finalResult.finalRepositoryGeneration()
                                        ? "pass"
                                        : "inconclusive due to concurrent writes"
                                    : "fail"
                            )
                            .endObject()
                            .endObject()
                    ),
                    () -> {}
                );
            } else if (streamingXContentResponse != null) {
                // failure after starting the response - finish the response with a rendering of the final exception
                assert finalResult == null;
                assert finalException != null;
                streamingXContentResponse.writeFragment(
                    p0 -> ChunkedToXContentHelper.singleChunk(
                        (b, p) -> b.endArray()
                            .startObject("exception")
                            .value((bb, pp) -> ElasticsearchException.generateFailureXContent(bb, pp, finalException, true))
                            .field("status", ExceptionsHelper.status(finalException))
                            .endObject()
                            .endObject()
                    ),
                    () -> {}
                );
            } else {
                // didn't even get as far as starting to stream the response, must have hit an early exception (e.g. repo not found) so
                // we can return this exception directly.
                assert streamingXContentResponse == null;
                assert finalResult == null;
                assert finalException != null;
                try {
                    restChannel.sendResponse(new RestResponse(restChannel, finalException));
                } catch (IOException e) {
                    finalException.addSuppressed(e);
                    logger.error("error building error response", finalException);
                    assert false : finalException; // shouldn't actually throw anything here
                    restChannel.request().getHttpChannel().close();
                }
            }
        } finally {
            Releasables.closeExpectNoException(streamingXContentResponse);
        }
    }

    public ActionListener<RepositoryVerifyIntegrityResponse> getCompletionListener() {
        return ActionListener.assertOnce(ActionListener.releaseAfter(new ActionListener<>() {
            @Override
            public void onResponse(RepositoryVerifyIntegrityResponse result) {
                assert hasReferences();
                assert finalException == null && finalResult == null && result != null;
                finalResult = result;
            }

            @Override
            public void onFailure(Exception e) {
                assert hasReferences();
                assert finalException == null && finalResult == null && e != null;
                finalException = e;
            }
        }, this::decRef));
    }
}
