/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.repositories.RepositoryData;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class RepositoryGenerationsExampleIT extends AbstractSnapshotIntegTestCase {
    private static final String REPO_NAME = "repo";

    public void test() {
        final var repositorySettings = randomRepositorySettings();

        // a newly-registered empty repo (with verify=false) is at UNKNOWN_REPO_GEN
        createRepository(REPO_NAME, "fs", repositorySettings, false);
        assertEquals(RepositoryData.UNKNOWN_REPO_GEN, getRepoGeneration());

        // reading the (empty) repo contents sets its generation
        clusterAdmin().prepareGetSnapshots(REPO_NAME).get();
        assertEquals(RepositoryData.EMPTY_REPO_GEN, getRepoGeneration());

        // creating a snapshot advances the repo gen
        createFullSnapshot(REPO_NAME, "snap");
        final var genAfterSnapshot = getRepoGeneration();
        assertThat(genAfterSnapshot, greaterThanOrEqualTo(0L));

        // re-registering the repo (again with verify=false) sets its gen back to UNKNOWN_REPO_GEN
        deleteRepository(REPO_NAME);
        createRepository(REPO_NAME, "fs", repositorySettings, false);
        assertEquals(RepositoryData.UNKNOWN_REPO_GEN, getRepoGeneration());

        // reading its contents sets its generation again
        getSnapshot(REPO_NAME, "snap");
        assertEquals(genAfterSnapshot, getRepoGeneration());
    }

    private static long getRepoGeneration() {
        return RepositoriesMetadata.get(internalCluster().clusterService().state()).repository(REPO_NAME).generation();
    }
}
