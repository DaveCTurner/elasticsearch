/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.io.IOException;

abstract class OutboundMessage extends NetworkMessage {

    private static final Logger logger = LogManager.getLogger(OutboundMessage.class);

    private final Writeable message;

    OutboundMessage(ThreadContext threadContext, Version version, byte status, long requestId, Writeable message) {
        super(threadContext, version, status, requestId);
        this.message = message;
    }

    ReleasableBytesReference serialize(BytesStreamOutput bytesStream) throws IOException {

        bytesStream.setVersion(version);
        bytesStream.skip(TcpHeader.headerSize(version));

        // The compressible bytes stream will not close the underlying bytes stream
        ReleasableBytesReference reference;
        int variableHeaderLength = -1;
        final long preHeaderPosition = bytesStream.position();

        if (version.onOrAfter(TcpHeader.VERSION_WITH_HEADER_SIZE)) {
            writeVariableHeader(bytesStream);
            variableHeaderLength = Math.toIntExact(bytesStream.position() - preHeaderPosition);
        }

        try (CompressibleBytesOutputStream stream =
                 new CompressibleBytesOutputStream(bytesStream, TransportStatus.isCompress(status))) {
            stream.setVersion(version);
            if (variableHeaderLength == -1) {
                writeVariableHeader(stream);
            }
            reference = writeMessage(stream);
        }

        boolean success = false;
        try {
            bytesStream.seek(0);
            final int contentSize = reference.length() - TcpHeader.headerSize(version);
            TcpHeader.writeHeader(bytesStream, requestId, status, version, contentSize, variableHeaderLength);
            success = true;
            return reference;
        } finally {
            if (success == false) {
                reference.close();
            }
        }
    }

    protected void writeVariableHeader(StreamOutput stream) throws IOException {
        threadContext.writeTo(stream);
    }

    protected ReleasableBytesReference writeMessage(CompressibleBytesOutputStream stream) throws IOException {
        if (message instanceof BytesTransportRequest) {
            boolean success = false;
            BytesTransportRequest bRequest = (BytesTransportRequest) message;
            try {
                bRequest.writeThin(stream);
                final ReleasableBytesReference zeroCopyBuf = bRequest.bytes;
                final ReleasableBytesReference result
                        = new ReleasableBytesReference(CompositeBytesReference.of(stream.materializeBytes(), zeroCopyBuf), () -> {
                    logger.info("--> releasing [{}] after transmission", System.identityHashCode(zeroCopyBuf));
                    zeroCopyBuf.close();
                });
                success = true;
                return result;
            } finally {
                if (success == false) {
                    bRequest.bytes.close();
                }
            }
        }

        if (message instanceof RemoteTransportException) {
            stream.writeException((RemoteTransportException) message);
        } else {
            message.writeTo(stream);
        }
        // we have to call materializeBytes() here before accessing the bytes. A CompressibleBytesOutputStream
        // might be implementing compression. And materializeBytes() ensures that some marker bytes (EOS marker)
        // are written. Otherwise we barf on the decompressing end when we read past EOF on purpose in the
        // #validateRequest method. this might be a problem in deflate after all but it's important to write
        // the marker bytes.
        return ReleasableBytesReference.wrap(stream.materializeBytes());
    }

    static class Request extends OutboundMessage {

        private final String action;

        Request(ThreadContext threadContext, Writeable message, Version version, String action, long requestId,
                boolean isHandshake, boolean compress) {
            super(threadContext, version, setStatus(compress, isHandshake, message), requestId, message);
            this.action = action;
        }

        @Override
        protected void writeVariableHeader(StreamOutput stream) throws IOException {
            super.writeVariableHeader(stream);
            if (version.before(Version.V_8_0_0)) {
                // empty features array
                stream.writeStringArray(Strings.EMPTY_ARRAY);
            }
            stream.writeString(action);
        }

        private static byte setStatus(boolean compress, boolean isHandshake, Writeable message) {
            byte status = 0;
            status = TransportStatus.setRequest(status);
            if (compress && OutboundMessage.canCompress(message)) {
                status = TransportStatus.setCompress(status);
            }
            if (isHandshake) {
                status = TransportStatus.setHandshake(status);
            }

            return status;
        }
    }

    static class Response extends OutboundMessage {

        Response(ThreadContext threadContext, Writeable message, Version version, long requestId, boolean isHandshake, boolean compress) {
            super(threadContext, version, setStatus(compress, isHandshake, message), requestId, message);
        }

        private static byte setStatus(boolean compress, boolean isHandshake, Writeable message) {
            byte status = 0;
            status = TransportStatus.setResponse(status);
            if (message instanceof RemoteTransportException) {
                status = TransportStatus.setError(status);
            }
            if (compress) {
                status = TransportStatus.setCompress(status);
            }
            if (isHandshake) {
                status = TransportStatus.setHandshake(status);
            }

            return status;
        }
    }

    private static boolean canCompress(Writeable message) {
        return message instanceof BytesTransportRequest == false;
    }
}
