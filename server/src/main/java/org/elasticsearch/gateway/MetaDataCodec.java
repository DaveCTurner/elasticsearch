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
package org.elasticsearch.gateway;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.compressing.CompressingStoredFieldsFormat;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.codecs.lucene80.Lucene80Codec;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

public class MetaDataCodec extends FilterCodec {

    private final CompressingStoredFieldsFormat storedFieldsFormat;

    public MetaDataCodec() {
        super("MetaData", new Lucene80Codec());

        final Decompressor decompressor = new Decompressor() {
            public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes) throws IOException {
                assert offset + length <= originalLength;

                if (bytes.bytes.length < originalLength) {
                    bytes.bytes = new byte[ArrayUtil.oversize(originalLength, 1)];
                }

                in.readBytes(bytes.bytes, 0, offset + length);
                bytes.offset = offset;
                bytes.length = length;
            }

            public Decompressor clone() {
                return this;
            }
        };

        final Compressor compressor = new Compressor() {
            public void compress(byte[] bytes, int off, int len, DataOutput out) throws IOException {
                out.writeBytes(bytes, off, len);
            }

            public void close() {
            }
        };

        storedFieldsFormat = new CompressingStoredFieldsFormat("uncompressed", new CompressionMode() {
            @Override
            public Compressor newCompressor() {
                return compressor;
            }

            @Override
            public Decompressor newDecompressor() {
                return decompressor;
            }
        }, 16384, 128, 1024);
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }
}
