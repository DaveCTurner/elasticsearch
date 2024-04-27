/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.operateAllIndices;

import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.WRITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class DestructiveOperationsIT extends ESIntegTestCase {

    @After
    public void afterTest() {
        updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), (String) null));
    }

    public void testDeleteIndexIsRejected() throws Exception {
        updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), true));
        createIndex("index1", "1index");

        // Should succeed, since no wildcards
        assertAcked(indicesAdmin().prepareDelete(masterNodeTimeout, "1index").get());
        // Special "match none" pattern succeeds, since non-destructive
        assertAcked(indicesAdmin().prepareDelete(masterNodeTimeout, "*", "-*").get());

        expectThrows(IllegalArgumentException.class, indicesAdmin().prepareDelete(masterNodeTimeout, "i*"));
        expectThrows(IllegalArgumentException.class, indicesAdmin().prepareDelete(masterNodeTimeout, "_all"));
    }

    public void testDeleteIndexDefaultBehaviour() throws Exception {
        if (randomBoolean()) {
            updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), false));
        }

        createIndex("index1", "1index");

        if (randomBoolean()) {
            assertAcked(indicesAdmin().prepareDelete(masterNodeTimeout, "_all").get());
        } else {
            assertAcked(indicesAdmin().prepareDelete(masterNodeTimeout, "*").get());
        }

        assertThat(indexExists("_all"), equalTo(false));
    }

    public void testCloseIndexIsRejected() throws Exception {
        updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), true));

        createIndex("index1", "1index");

        // Should succeed, since no wildcards
        assertAcked(indicesAdmin().prepareClose(masterNodeTimeout, "1index").get());
        // Special "match none" pattern succeeds, since non-destructive
        assertAcked(indicesAdmin().prepareClose(masterNodeTimeout, "*", "-*").get());

        expectThrows(IllegalArgumentException.class, indicesAdmin().prepareClose(masterNodeTimeout, "i*"));
        expectThrows(IllegalArgumentException.class, indicesAdmin().prepareClose(masterNodeTimeout, "_all"));
    }

    public void testCloseIndexDefaultBehaviour() throws Exception {
        if (randomBoolean()) {
            updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), false));
        }

        createIndex("index1", "1index");

        if (randomBoolean()) {
            assertAcked(indicesAdmin().prepareClose(masterNodeTimeout, "_all").get());
        } else {
            assertAcked(indicesAdmin().prepareClose(masterNodeTimeout, "*").get());
        }

        ClusterState state = clusterAdmin().prepareState().get().getState();
        for (Map.Entry<String, IndexMetadata> indexMetadataEntry : state.getMetadata().indices().entrySet()) {
            assertEquals(IndexMetadata.State.CLOSE, indexMetadataEntry.getValue().getState());
        }
    }

    public void testOpenIndexIsRejected() throws Exception {
        updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), true));

        createIndex("index1", "1index");
        assertAcked(indicesAdmin().prepareClose(masterNodeTimeout, "1index", "index1").get());

        // Special "match none" pattern succeeds, since non-destructive
        assertAcked(indicesAdmin().prepareOpen(masterNodeTimeout, "*", "-*").get());

        expectThrows(IllegalArgumentException.class, indicesAdmin().prepareOpen(masterNodeTimeout, "i*"));
        expectThrows(IllegalArgumentException.class, indicesAdmin().prepareOpen(masterNodeTimeout, "_all"));
    }

    public void testOpenIndexDefaultBehaviour() throws Exception {
        if (randomBoolean()) {
            updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), false));
        }

        createIndex("index1", "1index");
        assertAcked(indicesAdmin().prepareClose(masterNodeTimeout, "1index", "index1").get());

        if (randomBoolean()) {
            assertAcked(indicesAdmin().prepareOpen(masterNodeTimeout, "_all").get());
        } else {
            assertAcked(indicesAdmin().prepareOpen(masterNodeTimeout, "*").get());
        }

        ClusterState state = clusterAdmin().prepareState().get().getState();
        for (Map.Entry<String, IndexMetadata> indexMetadataEntry : state.getMetadata().indices().entrySet()) {
            assertEquals(IndexMetadata.State.OPEN, indexMetadataEntry.getValue().getState());
        }
    }

    public void testAddIndexBlockIsRejected() throws Exception {
        updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), true));

        createIndex("index1", "1index");

        // Should succeed, since no wildcards
        assertAcked(indicesAdmin().prepareAddBlock(masterNodeTimeout, WRITE, "1index").get());
        // Special "match none" pattern succeeds, since non-destructive
        assertAcked(indicesAdmin().prepareAddBlock(masterNodeTimeout, WRITE, "*", "-*").get());

        expectThrows(IllegalArgumentException.class, indicesAdmin().prepareAddBlock(masterNodeTimeout, WRITE, "i*"));
        expectThrows(IllegalArgumentException.class, indicesAdmin().prepareAddBlock(masterNodeTimeout, WRITE, "_all"));
    }

    public void testAddIndexBlockDefaultBehaviour() throws Exception {
        if (randomBoolean()) {
            updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), false));
        }

        createIndex("index1", "1index");

        if (randomBoolean()) {
            assertAcked(indicesAdmin().prepareAddBlock(masterNodeTimeout, WRITE, "_all").get());
        } else {
            assertAcked(indicesAdmin().prepareAddBlock(masterNodeTimeout, WRITE, "*").get());
        }

        ClusterState state = clusterAdmin().prepareState().get().getState();
        assertTrue("write block is set on index1", state.getBlocks().hasIndexBlock("index1", IndexMetadata.INDEX_WRITE_BLOCK));
        assertTrue("write block is set on 1index", state.getBlocks().hasIndexBlock("1index", IndexMetadata.INDEX_WRITE_BLOCK));
    }
}
