/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import org.apache.lucene.util.BitUtil;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.AbstractBigArray;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.ByteUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Objects;

/**
 * Adapts a raw {@link OutputStream} into a rich {@link StreamOutput} for use with {@link Writeable} instances, using a buffer.
 * <p>
 * Similar to {@link OutputStreamStreamOutput} in function, but with different performance characteristics because it requires a buffer to
 * be acquired or allocated up-front. Ignoring the costs of the buffer creation & release a {@link BufferedStreamOutput} is likely more
 * performant than an {@link OutputStreamStreamOutput} because it writes all fields directly to its local buffer and only copies data to the
 * underlying stream when the buffer fills up.
 */
public class BufferedStreamOutput extends StreamOutput {

    private static final int DEFAULT_BUFFER_SIZE = ByteSizeUnit.KB.toIntBytes(1);

    private final OutputStream delegate;
    private final byte[] buffer;
    private int position;

    /**
     * Wrap the given stream, using a freshly-allocated buffer with a size of {@code 1kiB}.
     */
    public BufferedStreamOutput(OutputStream delegate) {
        this(delegate, new byte[DEFAULT_BUFFER_SIZE]);
    }

    /**
     * Wrap the given stream, using the given {@code byte[]} for the buffer. It is the caller's responsibility to make sure that nothing
     * else uses this buffer while this object is active. The buffer must be at least {@code 1kiB}.
     */
    public BufferedStreamOutput(OutputStream delegate, byte[] buffer) {
        this.delegate = Objects.requireNonNull(delegate);
        this.buffer = Objects.requireNonNull(buffer);
        assert buffer.length >= DEFAULT_BUFFER_SIZE : buffer.length + " is too short";
    }

    @Override
    public void writeByte(byte b) throws IOException {
        if (capacity() < 1) {
            flush();
        }
        buffer[position++] = b;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        int initialCopyLength = Math.min(length, capacity());
        if (0 < initialCopyLength && (0 < position || length < buffer.length)) {
            System.arraycopy(b, offset, buffer, position, initialCopyLength);
            position += initialCopyLength;
            offset += initialCopyLength;
            length -= initialCopyLength;
        }

        if (0 < length) {
            flush();
            if (buffer.length < length) {
                delegate.write(b, offset, length);
            } else {
                System.arraycopy(b, offset, buffer, position, length);
                position += length;
            }
        }
    }

    private int capacity() {
        return buffer.length - position;
    }

    @Override
    public void flush() throws IOException {
        if (0 < position) {
            delegate.write(buffer, 0, position);
            position = 0;
        }
        delegate.flush();
        assert assertTrashBuffer(); // ensure nobody else cares about the buffer contents by trashing its contents if assertions enabled
    }

    private boolean assertTrashBuffer() {
        // sequence of 0xa5 == 0b10100101 is not valid as a bool/vInt/vLong/... and unlikely to arise otherwise so might aid debugging
        Arrays.fill(buffer, (byte) 0xa5);
        return true;
    }

    @Override
    public void close() throws IOException {
        flush();
        delegate.close();
    }

    @Override
    public void writeShort(short i) throws IOException {
        if (Short.BYTES <= capacity()) {
            ByteUtils.writeShortBE(i, buffer, position);
            position += Short.BYTES;
        } else {
            writeShortBigEndianWithBoundsChecks(i);
        }
    }

    // slow & cold path extracted to its own method to allow fast & hot path to be inlined
    private void writeShortBigEndianWithBoundsChecks(short i) throws IOException {
        writeByte((byte) (i >> 8));
        writeByte((byte) i);
    }

    @Override
    public void writeInt(int i) throws IOException {
        if (Integer.BYTES <= capacity()) {
            ByteUtils.writeIntBE(i, buffer, position);
            position += Integer.BYTES;
        } else {
            writeIntBigEndianWithBoundsChecks(i);
        }
    }

    // slow & cold path extracted to its own method to allow fast & hot path to be inlined
    private void writeIntBigEndianWithBoundsChecks(int i) throws IOException {
        writeByte((byte) (i >> 24));
        writeByte((byte) (i >> 16));
        writeByte((byte) (i >> 8));
        writeByte((byte) i);
    }

    @Override
    public void writeIntLE(int i) throws IOException {
        if (Integer.BYTES <= capacity()) {
            ByteUtils.writeIntLE(i, buffer, position);
            position += Integer.BYTES;
        } else {
            writeIntLittleEndianWithBoundsChecks(i);
        }
    }

    // slow & cold path extracted to its own method to allow fast & hot path to be inlined
    private void writeIntLittleEndianWithBoundsChecks(int i) throws IOException {
        writeByte((byte) i);
        writeByte((byte) (i >> 8));
        writeByte((byte) (i >> 16));
        writeByte((byte) (i >> 24));
    }

    private static final int MAX_VINT_BYTES = 5;
    private static final int MAX_VLONG_BYTES = 9;
    private static final int MAX_ZLONG_BYTES = 10;
    private static final int MAX_CHAR_BYTES = 3;

    @Override
    public void writeVInt(int i) throws IOException {
        if (25 <= Integer.numberOfLeadingZeros(i)) {
            writeByte((byte) i);
        } else if (MAX_VINT_BYTES <= capacity()) {
            position += putMultiByteVInt(buffer, i, position);
        } else {
            writeVIntWithBoundsChecks(i);
        }
    }

    private void putVInt(int i) {
        position += putVInt(buffer, i, position);
    }

    // slow & cold path extracted to its own method to allow fast & hot path to be inlined
    private void writeVIntWithBoundsChecks(int i) throws IOException {
        while ((i & 0xFFFF_FF80) != 0) {
            writeByte((byte) ((i & 0x7F) | 0x80));
            i >>>= 7;
        }
        writeByte((byte) i);
    }

    @Override
    void writeVLongNoCheck(long i) throws IOException {
        if (MAX_VLONG_BYTES <= capacity()) {
            while ((i & 0xFFFF_FFFF_FFFF_FF80L) != 0) {
                buffer[position++] = ((byte) ((i & 0x7F) | 0x80));
                i >>>= 7;
            }
            buffer[position++] = ((byte) i);
        } else {
            writeVLongWithBoundsChecks(i);
        }
    }

    // slow & cold path extracted to its own method to allow fast & hot path to be inlined
    private void writeVLongWithBoundsChecks(long i) throws IOException {
        while ((i & 0xFFFF_FFFF_FFFF_FF80L) != 0) {
            writeByte((byte) ((i & 0x7F) | 0x80));
            i >>>= 7;
        }
        writeByte((byte) i);
    }

    @Override
    public void writeZLong(long i) throws IOException {
        long value = BitUtil.zigZagEncode(i);
        if (MAX_ZLONG_BYTES <= capacity()) {
            while ((value & 0xFFFF_FFFF_FFFF_FF80L) != 0) {
                buffer[position++] = ((byte) ((value & 0x7F) | 0x80));
                value >>>= 7;
            }
            buffer[position++] = ((byte) value);
        } else {
            writeVLongWithBoundsChecks(value);
        }
    }

    @Override
    public void writeLong(long i) throws IOException {
        if (Long.BYTES <= capacity()) {
            ByteUtils.writeLongBE(i, buffer, position);
            position += Long.BYTES;
        } else {
            writeLongBigEndianWithBoundsChecks(i);
        }
    }

    // slow & cold path extracted to its own method to allow fast & hot path to be inlined
    private void writeLongBigEndianWithBoundsChecks(long i) throws IOException {
        writeByte((byte) (i >> 56));
        writeByte((byte) (i >> 48));
        writeByte((byte) (i >> 40));
        writeByte((byte) (i >> 32));
        writeByte((byte) (i >> 24));
        writeByte((byte) (i >> 16));
        writeByte((byte) (i >> 8));
        writeByte((byte) i);
    }

    @Override
    public void writeLongLE(long i) throws IOException {
        if (Long.BYTES <= capacity()) {
            ByteUtils.writeLongLE(i, buffer, position);
            position += Long.BYTES;
        } else {
            writeLongLittleEndianWithBoundsChecks(i);
        }
    }

    // slow & cold path extracted to its own method to allow fast & hot path to be inlined
    private void writeLongLittleEndianWithBoundsChecks(long i) throws IOException {
        writeByte((byte) i);
        writeByte((byte) (i >> 8));
        writeByte((byte) (i >> 16));
        writeByte((byte) (i >> 24));
        writeByte((byte) (i >> 32));
        writeByte((byte) (i >> 40));
        writeByte((byte) (i >> 48));
        writeByte((byte) (i >> 56));
    }

    @Override
    public void writeString(String str) throws IOException {
        final int charCount = str.length();
        if (MAX_VINT_BYTES + charCount * MAX_CHAR_BYTES <= capacity()) {
            putVInt(charCount);
            for (int i = 0; i < charCount; i++) {
                putCharUtf8(str.charAt(i));
            }
        } else {
            writeStringBoundsChecks(charCount, str);
        }
    }

    @Override
    public void writeOptionalString(String str) throws IOException {
        if (str == null) {
            writeByte((byte) 0);
        } else {
            final int charCount = str.length();
            if (1 + MAX_VINT_BYTES + charCount * MAX_CHAR_BYTES <= capacity()) {
                buffer[position++] = (byte) 1;
                putVInt(charCount);
                for (int i = 0; i < charCount; i++) {
                    putCharUtf8(str.charAt(i));
                }
            } else {
                writeByte((byte) 1);
                writeStringBoundsChecks(charCount, str);
            }
        }
    }

    @Override
    public void writeGenericString(String str) throws IOException {
        final int charCount = str.length();
        if (1 + MAX_VINT_BYTES + charCount * MAX_CHAR_BYTES <= capacity()) {
            buffer[position++] = (byte) 0;
            putVInt(charCount);
            for (int i = 0; i < charCount; i++) {
                putCharUtf8(str.charAt(i));
            }
        } else {
            writeByte((byte) 0);
            writeStringBoundsChecks(charCount, str);
        }
    }

    // slow & cold path extracted to its own method to allow fast & hot path to be inlined
    private void writeStringBoundsChecks(int charCount, String str) throws IOException {
        writeVInt(charCount);
        for (int i = 0; i < charCount; i++) {
            if (MAX_CHAR_BYTES <= capacity()) {
                putCharUtf8(str.charAt(i));
            } else {
                writeCharUtf8(str.charAt(i));
            }
        }
    }

    private void putCharUtf8(int c) {
        if (c <= 0x007F) {
            buffer[position++] = ((byte) c);
        } else if (c > 0x07FF) {
            buffer[position++] = ((byte) (0xE0 | c >> 12 & 0x0F));
            buffer[position++] = ((byte) (0x80 | c >> 6 & 0x3F));
            buffer[position++] = ((byte) (0x80 | c >> 0 & 0x3F));
        } else {
            buffer[position++] = ((byte) (0xC0 | c >> 6 & 0x1F));
            buffer[position++] = ((byte) (0x80 | c >> 0 & 0x3F));
        }
    }

    private void writeCharUtf8(int c) throws IOException {
        if (c <= 0x007F) {
            writeByte((byte) c);
        } else if (c > 0x07FF) {
            writeByte((byte) (0xE0 | c >> 12 & 0x0F));
            writeByte((byte) (0x80 | c >> 6 & 0x3F));
            writeByte((byte) (0x80 | c >> 0 & 0x3F));
        } else {
            writeByte((byte) (0xC0 | c >> 6 & 0x1F));
            writeByte((byte) (0x80 | c >> 0 & 0x3F));
        }
    }
}
