/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.diagnostics;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.startsWith;

public class DiagnosticsActionIT extends ESIntegTestCase {
    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    public void testDiagnostics() throws IOException {
        final var response = getRestClient().performRequest(new Request("GET", "/_internal/diagnostics_bundle"));
        assertEquals("application/zip", response.getHeader("Content-Type"));

        final var blobs = new HashSet<String>();
        final var copyBuffer = new byte[PageCacheRecycler.BYTE_PAGE_SIZE];

        final var contentDisposition = response.getHeader("Content-Disposition");
        final var contentDispositionStart = "attachment; filename=\"elasticsearch-internal-diagnostics-";
        final var contentDispositionEnd = ".zip\"";
        assertThat(contentDisposition, allOf(startsWith(contentDispositionStart), endsWith(contentDispositionEnd)));
        final var timestamp = contentDisposition.substring(
            contentDispositionStart.length(),
            contentDisposition.length() - contentDispositionEnd.length()
        );
        final var pathPrefix = "elasticsearch-internal-diagnostics-" + timestamp + "/";

        logger.info("--> response headers: {}", Arrays.toString(response.getHeaders()));
        try (var zipStream = new ZipInputStream(response.getEntity().getContent())) {
            ZipEntry zipEntry;
            while ((zipEntry = zipStream.getNextEntry()) != null) {
                assertThat(zipEntry.getName(), startsWith(pathPrefix));
                final var name = zipEntry.getName().substring(pathPrefix.length());
                final var size = Math.toIntExact(zipEntry.getSize());

                logger.info("--> read blob [{}] with size [{}]", name, size);
                blobs.add(name);

                final BytesReference entryContent;
                try (var bytesStream = new BytesStreamOutput()) {
                    while (true) {
                        final var readLength = zipStream.read(copyBuffer, 0, copyBuffer.length);
                        if (readLength < 0) {
                            break;
                        }
                        bytesStream.write(copyBuffer, 0, readLength);
                    }
                    entryContent = bytesStream.bytes();
                }

                if (name.endsWith(".json")) {
                    try (
                        var parser = XContentHelper.createParserNotCompressed(
                            XContentParserConfiguration.EMPTY,
                            entryContent,
                            XContentType.JSON
                        )
                    ) {
                        parser.skipChildren();
                    }
                }
            }
        }

        assertThat(blobs, hasItems("cluster_state.json", "cluster_stats.json"));
    }
}
