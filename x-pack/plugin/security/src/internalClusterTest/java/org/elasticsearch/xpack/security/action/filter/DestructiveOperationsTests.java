/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.filter;

import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.junit.After;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class DestructiveOperationsTests extends SecurityIntegTestCase {

    @After
    public void afterTest() {
        updateClusterSettings(Settings.builder().putNull(DestructiveOperations.REQUIRES_NAME_SETTING.getKey()));
    }

    public void testDeleteIndexDestructiveOperationsRequireName() {
        createIndex("index1");
        updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), true));
        {
            IllegalArgumentException illegalArgumentException = expectThrows(
                IllegalArgumentException.class,
                () -> indicesAdmin().prepareDelete(masterNodeTimeout, "*").get()
            );
            assertEquals("Wildcard expressions or all indices are not allowed", illegalArgumentException.getMessage());
            String[] indices = indicesAdmin().prepareGetIndex(masterNodeTimeout).setIndices("index1").get().getIndices();
            assertEquals(1, indices.length);
            assertEquals("index1", indices[0]);
        }
        {
            IllegalArgumentException illegalArgumentException = expectThrows(
                IllegalArgumentException.class,
                () -> indicesAdmin().prepareDelete(masterNodeTimeout, "*", "-index1").get()
            );
            assertEquals("Wildcard expressions or all indices are not allowed", illegalArgumentException.getMessage());
            String[] indices = indicesAdmin().prepareGetIndex(masterNodeTimeout).setIndices("index1").get().getIndices();
            assertEquals(1, indices.length);
            assertEquals("index1", indices[0]);
        }
        {
            IllegalArgumentException illegalArgumentException = expectThrows(
                IllegalArgumentException.class,
                () -> indicesAdmin().prepareDelete(masterNodeTimeout, "_all").get()
            );
            assertEquals("Wildcard expressions or all indices are not allowed", illegalArgumentException.getMessage());
            String[] indices = indicesAdmin().prepareGetIndex(masterNodeTimeout).setIndices("index1").get().getIndices();
            assertEquals(1, indices.length);
            assertEquals("index1", indices[0]);
        }

        // the "*,-*" pattern is specially handled because it makes a destructive action non-destructive
        assertAcked(indicesAdmin().prepareDelete(masterNodeTimeout, "*", "-*"));
        assertAcked(indicesAdmin().prepareDelete(masterNodeTimeout, "index1"));
    }

    public void testDestructiveOperationsDefaultBehaviour() {
        if (randomBoolean()) {
            updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), false));
        }
        createIndex("index1", "index2");

        switch (randomIntBetween(0, 2)) {
            case 0 -> {
                assertAcked(indicesAdmin().prepareClose(masterNodeTimeout, "*"));
                assertAcked(indicesAdmin().prepareOpen(masterNodeTimeout, "*"));
                assertAcked(indicesAdmin().prepareDelete(masterNodeTimeout, "*"));
            }
            case 1 -> {
                assertAcked(indicesAdmin().prepareClose(masterNodeTimeout, "_all"));
                assertAcked(indicesAdmin().prepareOpen(masterNodeTimeout, "_all"));
                assertAcked(indicesAdmin().prepareDelete(masterNodeTimeout, "_all"));
            }
            case 2 -> {
                assertAcked(indicesAdmin().prepareClose(masterNodeTimeout, "*", "-index1"));
                assertAcked(indicesAdmin().prepareOpen(masterNodeTimeout, "*", "-index1"));
                assertAcked(indicesAdmin().prepareDelete(masterNodeTimeout, "*", "-index1"));
            }
            default -> throw new UnsupportedOperationException();
        }
    }

    public void testOpenCloseIndexDestructiveOperationsRequireName() {
        updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), true));
        {
            IllegalArgumentException illegalArgumentException = expectThrows(
                IllegalArgumentException.class,
                () -> indicesAdmin().prepareClose(masterNodeTimeout, "*").get()
            );
            assertEquals("Wildcard expressions or all indices are not allowed", illegalArgumentException.getMessage());
        }
        {
            IllegalArgumentException illegalArgumentException = expectThrows(
                IllegalArgumentException.class,
                () -> indicesAdmin().prepareClose(masterNodeTimeout, "*", "-index1").get()
            );
            assertEquals("Wildcard expressions or all indices are not allowed", illegalArgumentException.getMessage());
        }
        {
            IllegalArgumentException illegalArgumentException = expectThrows(
                IllegalArgumentException.class,
                () -> indicesAdmin().prepareClose(masterNodeTimeout, "_all").get()
            );
            assertEquals("Wildcard expressions or all indices are not allowed", illegalArgumentException.getMessage());
        }
        {
            IllegalArgumentException illegalArgumentException = expectThrows(
                IllegalArgumentException.class,
                () -> indicesAdmin().prepareOpen(masterNodeTimeout, "*").get()
            );
            assertEquals("Wildcard expressions or all indices are not allowed", illegalArgumentException.getMessage());
        }
        {
            IllegalArgumentException illegalArgumentException = expectThrows(
                IllegalArgumentException.class,
                () -> indicesAdmin().prepareOpen(masterNodeTimeout, "*", "-index1").get()
            );
            assertEquals("Wildcard expressions or all indices are not allowed", illegalArgumentException.getMessage());
        }
        {
            IllegalArgumentException illegalArgumentException = expectThrows(
                IllegalArgumentException.class,
                () -> indicesAdmin().prepareOpen(masterNodeTimeout, "_all").get()
            );
            assertEquals("Wildcard expressions or all indices are not allowed", illegalArgumentException.getMessage());
        }

        // the "*,-*" pattern is specially handled because it makes a destructive action non-destructive
        assertAcked(indicesAdmin().prepareClose(masterNodeTimeout, "*", "-*"));
        assertAcked(indicesAdmin().prepareOpen(masterNodeTimeout, "*", "-*"));

        createIndex("index1");
        assertAcked(indicesAdmin().prepareClose(masterNodeTimeout, "index1"));
        assertAcked(indicesAdmin().prepareOpen(masterNodeTimeout, "index1"));
    }
}
