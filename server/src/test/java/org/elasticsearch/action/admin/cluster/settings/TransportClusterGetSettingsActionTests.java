/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;

import java.util.Set;

import static org.mockito.Mockito.mock;

public class TransportClusterGetSettingsActionTests extends ESTestCase {
    public void testMasterOperation() throws Exception {
        final var action = new TransportClusterGetSettingsAction(
            mock(TransportService.class),
            null,
            null,
            new SettingsFilter(Set.of("*hidden*")),
            new ActionFilters(Set.of()),
            null
        );

        var state = ClusterState.EMPTY_STATE;
        {
            final var future = new PlainActionFuture<ClusterGetSettingsResponse>();
            action.masterOperation(null, new ClusterGetSettingsRequest(), state, future);
            assertTrue(future.isDone());
            assertEquals(Settings.EMPTY, future.get().persistentSettings());
            assertEquals(Settings.EMPTY, future.get().transientSettings());
            assertEquals(Settings.EMPTY, future.get().combinedSettings());
        }

        final var key = "test.setting";

        state = state.copyAndUpdateMetadata(builder -> builder.persistentSettings(Settings.builder().put(key, "persists").build()));
        {
            final var future = new PlainActionFuture<ClusterGetSettingsResponse>();
            action.masterOperation(null, new ClusterGetSettingsRequest(), state, future);
            assertTrue(future.isDone());
            assertEquals("persists", future.get().persistentSettings().get(key));
            assertEquals(Settings.EMPTY, future.get().transientSettings());
            assertEquals("persists", future.get().combinedSettings().get(key));
        }

        state = state.copyAndUpdateMetadata(builder -> builder.transientSettings(Settings.builder().put(key, "transient").build()));
        {
            final var future = new PlainActionFuture<ClusterGetSettingsResponse>();
            action.masterOperation(null, new ClusterGetSettingsRequest(), state, future);
            assertTrue(future.isDone());
            assertEquals("persists", future.get().persistentSettings().get(key));
            assertEquals("transient", future.get().transientSettings().get(key));
            assertEquals("transient", future.get().combinedSettings().get(key));
        }

        state = state.copyAndUpdateMetadata(builder -> builder.persistentSettings(Settings.EMPTY));
        {
            final var future = new PlainActionFuture<ClusterGetSettingsResponse>();
            action.masterOperation(null, new ClusterGetSettingsRequest(), state, future);
            assertTrue(future.isDone());
            assertEquals(Settings.EMPTY, future.get().persistentSettings());
            assertEquals("transient", future.get().transientSettings().get(key));
            assertEquals("transient", future.get().combinedSettings().get(key));
        }

        state = state.copyAndUpdateMetadata(
            builder -> builder.persistentSettings(Settings.builder().put("test.hidden.setting", "persists").build())
                .transientSettings(Settings.builder().put("test.hidden.setting", "transient").build())
        );
        {
            final var future = new PlainActionFuture<ClusterGetSettingsResponse>();
            action.masterOperation(null, new ClusterGetSettingsRequest(), state, future);
            assertTrue(future.isDone());
            assertEquals(Settings.EMPTY, future.get().persistentSettings());
            assertEquals(Settings.EMPTY, future.get().transientSettings());
            assertEquals(Settings.EMPTY, future.get().combinedSettings());
        }
    }
}
