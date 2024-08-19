/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.blobstore.testkit.SnapshotRepositoryTestKit;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

public class RepositoryVerifyIntegrityIT extends ESIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable HTTP
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(super.nodePlugins(), SnapshotRepositoryTestKit.class);
    }

    public void testResponse() throws IOException {
        final var repoName = randomIdentifier();
        final var client = getRestClient();
        final var request = new Request("POST", "/_snapshot/" + repoName + "/_verify_integrity");
        final var response = client.performRequest(request);
        logger.info("--> response body: {}", new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8));
    }
}
