/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.StreamingXContentResponse;

import java.io.IOException;

/**
 * Represents a (possibly-streaming) response to the repository-verify-integrity API.
 */
class RepositoryVerifyIntegrityResponseBuilder extends AbstractRefCounted {

    private static final Logger logger = LogManager.getLogger(RepositoryVerifyIntegrityResponseBuilder.class);

    private final RestChannel restChannel;

    @Nullable // if still running, or completed without an exception
    private volatile Exception finalException;

    @Nullable // if still running, or completed without an exception
    private volatile StreamingXContentResponse streamingXContentResponse;

    RepositoryVerifyIntegrityResponseBuilder(RestChannel restChannel) throws IOException {
        streamingXContentResponse = new StreamingXContentResponse(restChannel, restChannel.request(), () -> {});
        this.restChannel = restChannel;
    }

    void setFinalException(Exception finalException) {
        assert this.finalException == null && finalException != null;
        this.finalException = finalException;
    }

    void writeFragment(ChunkedToXContent fragment, Releasable releasable) throws IOException {
        final var localStreamingXContentResponse = getStreamingXContentResponse();
        localStreamingXContentResponse.writeFragment(fragment, releasable);
    }

    /**
     * @return {@link StreamingXContentResponse} constructed on first write.
     */
    private StreamingXContentResponse getStreamingXContentResponse() throws IOException {
        var localStreamingXContentResponse = streamingXContentResponse;
        if (localStreamingXContentResponse == null) {
            synchronized (this) {
                localStreamingXContentResponse = streamingXContentResponse;
                if (localStreamingXContentResponse == null) {
                    streamingXContentResponse = localStreamingXContentResponse = new StreamingXContentResponse(
                        restChannel,
                        restChannel.request(),
                        () -> {}
                    );
                }
            }
        }
        return localStreamingXContentResponse;
    }

    @Override
    protected void closeInternal() {
        if (streamingXContentResponse != null) {
            // TODO if finalException != null render it here
            streamingXContentResponse.close();
        } else if (finalException != null) {
            try {
                restChannel.sendResponse(new RestResponse(restChannel, finalException));
            } catch (Exception e) {
                finalException.addSuppressed(e);
                logger.error("failure while sending failure response", finalException);
                assert false : finalException; // shouldn't actually throw anything here
                restChannel.request().getHttpChannel().close();
            }
        }
    }

    public <T> ActionListener<T> getCompletionListener() {
        return ActionListener.assertOnce(ActionListener.releaseAfter(new ActionListener<T>() {
            @Override
            public void onResponse(T t) {}

            @Override
            public void onFailure(Exception e) {
                assert RepositoryVerifyIntegrityResponseBuilder.this.finalException == null && e != null;
                RepositoryVerifyIntegrityResponseBuilder.this.finalException = e;
            }
        }, this::decRef));
    }
}
