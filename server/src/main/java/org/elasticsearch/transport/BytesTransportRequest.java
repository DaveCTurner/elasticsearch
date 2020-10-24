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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A specialized, bytes only request, that can potentially be optimized on the network
 * layer, specifically for the same large buffer send to several nodes.
 */
public class BytesTransportRequest extends TransportRequest {

    private static final Logger logger = LogManager.getLogger(BytesTransportRequest.class);

    ReleasableBytesReference bytes;
    Version version;

    public BytesTransportRequest(StreamInput in) throws IOException {
        super(in);
        bytes = ReleasableBytesReference.wrap(in.readBytesReference());
        version = in.getVersion();
    }

    public BytesTransportRequest(ReleasableBytesReference bytes, Version version) {
        this.bytes = bytes;
        this.version = version;
    }

    public Version version() {
        return this.version;
    }

    public ReleasableBytesReference bytes() {
        return this.bytes;
    }

    /**
     * Writes the data in a "thin" manner, without the actual bytes, assumes
     * the actual bytes will be appended right after this content.
     */
    public void writeThin(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(bytes.length());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBytesReference(bytes);
    }

    /**
     * Copies the underlying bytes ref to a new array and then releases it. Very inefficient, must only be used sparingly.
     */
    public void cloneAndReleaseBytes() {
        final ReleasableBytesReference newBytes = ReleasableBytesReference.wrap(new BytesArray(bytes.toBytesRef()));
        logger.info("----> releasing [{}] on clone as [{}]", System.identityHashCode(bytes), System.identityHashCode(newBytes));
        bytes.close();
        bytes = newBytes;
    }

    @Override
    public void onSendComplete() {
        logger.info("----> releasing [{}] on send complete", System.identityHashCode(bytes));
        bytes.close();
    }
}
