/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.common.bytes.BytesReferenceTestUtils.equalBytes;
import static org.elasticsearch.transport.BytesRefRecycler.NON_RECYCLING_INSTANCE;

public class BufferedStreamOutputTests extends ESTestCase {

    public void testRandomWrites() throws IOException {
        try (
            var expectedStream = new RecyclerBytesStreamOutput(NON_RECYCLING_INSTANCE);
            var actualStream = new RecyclerBytesStreamOutput(NON_RECYCLING_INSTANCE);
            var bufferedStream = new BufferedStreamOutput(actualStream)
        ) {
            for (int i = between(0, 100); i >= 0; i--) {
                switch (between(1, 8)) {
                    case 1 -> {
                        final var b = randomByte();
                        bufferedStream.writeByte(b);
                        expectedStream.writeByte(b);
                    }
                    case 2 -> {
                        final var bytes = randomByteArrayOfLength(between(1, 3000));
                        final var start = between(0, bytes.length - 1);
                        final var length = between(0, bytes.length - start - 1);
                        bufferedStream.writeBytes(bytes, start, length);
                        expectedStream.writeBytes(bytes, start, length);
                    }
                    case 3 -> {
                        final var value = randomNonNegativeInt();
                        bufferedStream.writeVInt(value);
                        expectedStream.writeVInt(value);
                    }
                    case 4 -> {
                        final var value = randomNonNegativeLong();
                        bufferedStream.writeVLong(value);
                        expectedStream.writeVLong(value);
                    }
                    case 5 -> {
                        final var value = randomLong();
                        bufferedStream.writeLong(value);
                        expectedStream.writeLong(value);
                    }
                    case 6 -> {
                        final var value = randomUnicodeOfLengthBetween(0, 2000);
                        bufferedStream.writeString(value);
                        expectedStream.writeString(value);
                    }
                    case 7 -> {
                        final var value = randomBoolean() ? null : randomUnicodeOfLengthBetween(0, 2000);
                        bufferedStream.writeOptionalString(value);
                        expectedStream.writeOptionalString(value);
                    }
                    case 8 -> {
                        final var value = randomUnicodeOfLengthBetween(0, 2000);
                        bufferedStream.writeGenericString(value);
                        expectedStream.writeGenericString(value);
                    }
                }
            }

            bufferedStream.flush();
            assertThat(actualStream.bytes(), equalBytes(expectedStream.bytes()));
        }
    }

}
