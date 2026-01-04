/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.elasticsearch.common.bytes.BytesReferenceTestUtils.equalBytes;
import static org.elasticsearch.common.unit.ByteSizeUnit.KB;
import static org.elasticsearch.transport.BytesRefRecycler.NON_RECYCLING_INSTANCE;

public class BufferedStreamOutputTests extends ESTestCase {

    public void testRandomWrites() throws IOException {
        final var permitPartialWrites = new AtomicBoolean();
        final var bufferPool = randomByteArrayOfLength(between(KB.toIntBytes(1), KB.toIntBytes(4)));
        final var bufferStart = between(0, bufferPool.length - KB.toIntBytes(1));
        final var bufferLen = between(1, bufferPool.length - bufferStart);
        final var buffer = new BytesRef(bufferPool, bufferStart, bufferLen);
        final var bufferPoolCopy = ArrayUtil.copyArray(bufferPool); // kept so we can check no out-of-bounds writes
        Arrays.fill(bufferPoolCopy, bufferStart, bufferStart + bufferLen, (byte) 0xa5);

        try (
            var expectedStream = new RecyclerBytesStreamOutput(NON_RECYCLING_INSTANCE);
            var actualStream = new RecyclerBytesStreamOutput(NON_RECYCLING_INSTANCE) {
                @Override
                public void write(byte[] b) {
                    fail("buffered stream should not write single bytes");
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    assertTrue(permitPartialWrites.get() || len == bufferLen);
                    super.write(b, off, len);
                }
            };
            var bufferedStream = new BufferedStreamOutput(actualStream, buffer)
        ) {
            final var writers = List.<Supplier<CheckedConsumer<StreamOutput, IOException>>>of(() -> {
                final var b = randomByte();
                return s -> s.writeByte(b);
            }, () -> {
                final var bytes = randomByteArrayOfLength(between(1, bufferLen * 4));
                final var start = between(0, bytes.length - 1);
                final var length = between(0, bytes.length - start - 1);
                return s -> {
                    permitPartialWrites.set(length >= bufferLen); // large writes may bypass the buffer
                    s.writeBytes(bytes, start, length);
                    permitPartialWrites.set(false);
                };
            }, () -> {
                final var value = randomShort();
                return s -> s.writeShort(value);
            }, () -> {
                final var value = randomInt();
                return s -> s.writeInt(value);
            }, () -> {
                final var value = randomInt();
                return s -> s.writeIntLE(value);
            }, () -> {
                final var value = randomLong();
                return s -> s.writeLong(value);
            }, () -> {
                final var value = randomLong();
                return s -> s.writeLongLE(value);
            }, () -> {
                final var value = randomInt();
                return s -> s.writeVInt(value);
            }, () -> {
                final var value = randomInts(between(0, 100)).toArray();
                return s -> s.writeVIntArray(value);
            }, () -> {
                final var value = randomNonNegativeLong();
                return s -> s.writeVLong(value);
            }, () -> {
                final var value = randomLong();
                return s -> s.writeZLong(value);
            }, () -> {
                final var value = randomUnicodeOfLengthBetween(0, 2000);
                return s -> s.writeString(value);
            }, () -> {
                final var value = randomBoolean() ? null : randomUnicodeOfLengthBetween(0, 2000);
                return s -> s.writeOptionalString(value);
            }, () -> {
                final var value = randomUnicodeOfLengthBetween(0, 2000);
                return s -> s.writeGenericString(value);
            });

            final var targetSize = PageCacheRecycler.PAGE_SIZE_IN_BYTES + 1;

            while (actualStream.size() < targetSize) {
                var writer = randomFrom(writers).get();
                writer.accept(bufferedStream);
                writer.accept(expectedStream);
                assertEquals(expectedStream.position(), bufferedStream.position());
            }

            permitPartialWrites.set(true);
            bufferedStream.flush();
            assertThat(actualStream.bytes(), equalBytes(expectedStream.bytes()));
        }

        if (Assertions.ENABLED == false) {
            // in this case the buffer is not trashed on flush so we must trash its contents manually before the corruption check
            Arrays.fill(bufferPool, bufferStart, bufferStart + bufferLen, (byte) 0xa5);
        }
        assertArrayEquals("wrote out of bounds", bufferPoolCopy, bufferPool);
    }
}
