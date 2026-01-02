/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.ByteUtils;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Similar to {@link OutputStreamStreamOutput} except using a 1kiB buffer to coalesce writes to the underlying stream, allowing for more
 * efficient implementations of {@link StreamOutput#writeString(String)} and {@link StreamOutput#writeVInt(int)} and so on.
 */
public class BufferedStreamOutput extends StreamOutput {

    private static final int BUFFER_SIZE = ByteSizeUnit.KB.toIntBytes(1);

    private final OutputStream delegate;
    private final byte[] buffer = new byte[BUFFER_SIZE];
    private int position;

    public BufferedStreamOutput(OutputStream delegate) {
        this.delegate = delegate;
    }

    @Override
    public void writeByte(byte b) throws IOException {
        ensureCapacity(1);
        buffer[position++] = b;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        int initialCopyLength = Math.min(length, BUFFER_SIZE - position);
        if (0 < initialCopyLength && (0 < position || length < BUFFER_SIZE)) {
            System.arraycopy(b, offset, buffer, position, initialCopyLength);
            position += initialCopyLength;
            offset += initialCopyLength;
            length -= initialCopyLength;
        }

        if (0 < length) {
            flush();
            if (BUFFER_SIZE < length) {
                delegate.write(b, offset, length);
            } else {
                System.arraycopy(b, offset, buffer, position, length);
                position += length;
            }
        }
    }

    private void ensureCapacity(int needed) throws IOException {
        if (BUFFER_SIZE - position < needed) {
            flush();
        }
    }

    @Override
    public void flush() throws IOException {
        if (0 < position) {
            delegate.write(buffer, 0, position);
            position = 0;
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        delegate.close();
    }

    @Override
    public void writeVInt(int i) throws IOException {
        ensureCapacity(5);
        position += putVInt(buffer, i, position);
    }

    @Override
    public void writeVLong(long i) throws IOException {
        ensureCapacity(9);
        while ((i & 0xFFFFFFFFFFFFFF80L) != 0) {
            buffer[position++] = ((byte) ((i & 0x7f) | 0x80));
            i >>>= 7;
        }
        buffer[position++] = ((byte) i);
    }

    @Override
    public void writeLong(long i) throws IOException {
        ensureCapacity(Long.BYTES);
        ByteUtils.writeLongBE(i, buffer, position);
        position += Long.BYTES;
    }

    @Override
    public void writeString(String str) throws IOException {
        final int charCount = str.length();
        if (position + 5 + charCount * 3 <= BUFFER_SIZE) {
            position += putVInt(buffer, charCount, position);
            for (int i = 0; i < charCount; i++) {
                writeCharUtf8(str.charAt(i));
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
            if (position + 6 + charCount * 3 <= BUFFER_SIZE) {
                buffer[position++] = (byte) 1;
                position += putVInt(buffer, charCount, position);
                for (int i = 0; i < charCount; i++) {
                    writeCharUtf8(str.charAt(i));
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
        if (position + 6 + charCount * 3 <= BUFFER_SIZE) {
            buffer[position++] = (byte) 0;
            position += putVInt(buffer, charCount, position);
            for (int i = 0; i < charCount; i++) {
                writeCharUtf8(str.charAt(i));
            }
        } else {
            writeByte((byte) 0);
            writeStringBoundsChecks(charCount, str);
        }
    }

    private void writeStringBoundsChecks(int charCount, String str) throws IOException {
        writeVInt(charCount);
        for (int i = 0; i < charCount;) {
            ensureCapacity(3);
            while (i < charCount && position <= BUFFER_SIZE - 3) {
                writeCharUtf8(str.charAt(i++));
            }
        }
    }

    private void writeCharUtf8(int c) {
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
}
