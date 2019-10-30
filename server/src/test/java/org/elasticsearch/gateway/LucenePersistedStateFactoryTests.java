/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.gateway;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetaData;
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

@TestLogging(reason = "nocommit", value = "org.elasticsearch.gateway:TRACE")
public class LucenePersistedStateFactoryTests extends ESTestCase {

    public void testPersistsAndReloadsTerm() throws IOException {
        try (final NodeEnvironment nodeEnvironment = newNodeEnvironment(createDataPaths())) {
            final LucenePersistedStateFactory persistedStateFactory = new LucenePersistedStateFactory(nodeEnvironment, xContentRegistry());
            final long newTerm = randomNonNegativeLong();

            try (CoordinationState.PersistedState persistedState = loadPersistedState(persistedStateFactory)) {
                assertThat(persistedState.getCurrentTerm(), equalTo(0L));
                persistedState.setCurrentTerm(newTerm);
                assertThat(persistedState.getCurrentTerm(), equalTo(newTerm));
            }

            try (CoordinationState.PersistedState persistedState = loadPersistedState(persistedStateFactory)) {
                assertThat(persistedState.getCurrentTerm(), equalTo(newTerm));
            }
        }
    }

    public void testPersistsAndReloadsGlobalMetadata() throws IOException {
        try (final NodeEnvironment nodeEnvironment = newNodeEnvironment(createDataPaths())) {
            final LucenePersistedStateFactory persistedStateFactory = new LucenePersistedStateFactory(nodeEnvironment, xContentRegistry());
            final String clusterUUID = UUIDs.randomBase64UUID(random());
            final long version = randomLongBetween(1L, Long.MAX_VALUE);

            try (CoordinationState.PersistedState persistedState = loadPersistedState(persistedStateFactory)) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                persistedState.setLastAcceptedState(ClusterState.builder(clusterState)
                    .metaData(MetaData.builder(clusterState.metaData())
                        .clusterUUID(clusterUUID)
                        .clusterUUIDCommitted(true)
                        .version(version))
                    .incrementVersion().build());
                assertThat(persistedState.getLastAcceptedState().metaData().clusterUUID(), equalTo(clusterUUID));
                assertTrue(persistedState.getLastAcceptedState().metaData().clusterUUIDCommitted());
            }

            try (CoordinationState.PersistedState persistedState = loadPersistedState(persistedStateFactory)) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                assertThat(clusterState.metaData().clusterUUID(), equalTo(clusterUUID));
                assertTrue(clusterState.metaData().clusterUUIDCommitted());
            }
        }
    }

    public void testLoadsFreshestState() throws IOException {
        final Path[] dataPaths = createDataPaths();
        final long freshTerm = randomLongBetween(1L, Long.MAX_VALUE);
        final long staleTerm = randomBoolean() ? freshTerm : randomLongBetween(1L, freshTerm);
        final long freshVersion = randomLongBetween(2L, Long.MAX_VALUE);
        final long staleVersion = staleTerm == freshTerm ? randomLongBetween(1L, freshVersion - 1) : randomLongBetween(1L, Long.MAX_VALUE);

        final long staleCurrentTerm = randomLongBetween(1L, Long.MAX_VALUE);
        final long freshCurrentTerm = randomLongBetween(staleCurrentTerm, Long.MAX_VALUE);

        final HashSet<Path> unimportantPaths = Arrays.stream(dataPaths).collect(Collectors.toCollection(HashSet::new));

        try (final NodeEnvironment nodeEnvironment = newNodeEnvironment(dataPaths)) {
            try (CoordinationState.PersistedState persistedState
                     = loadPersistedState(new LucenePersistedStateFactory(nodeEnvironment, xContentRegistry()))) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                persistedState.setCurrentTerm(staleCurrentTerm);
                persistedState.setLastAcceptedState(
                    ClusterState.builder(clusterState).version(staleVersion)
                        .metaData(MetaData.builder(clusterState.metaData()).coordinationMetaData(
                            CoordinationMetaData.builder(clusterState.coordinationMetaData()).term(staleTerm).build())).build());
            }
        }

        try (final NodeEnvironment nodeEnvironment = newNodeEnvironment(new Path[]{randomFrom(dataPaths)})) {
            unimportantPaths.remove(nodeEnvironment.nodeDataPaths()[0]);
            try (CoordinationState.PersistedState persistedState
                     = loadPersistedState(new LucenePersistedStateFactory(nodeEnvironment, xContentRegistry()))) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                persistedState.setLastAcceptedState(
                    ClusterState.builder(clusterState).version(freshVersion)
                        .metaData(MetaData.builder(clusterState.metaData()).coordinationMetaData(
                            CoordinationMetaData.builder(clusterState.coordinationMetaData()).term(freshTerm).build())).build());
            }
        }

        try (final NodeEnvironment nodeEnvironment = newNodeEnvironment(new Path[]{randomFrom(dataPaths)})) {
            unimportantPaths.remove(nodeEnvironment.nodeDataPaths()[0]);
            try (CoordinationState.PersistedState persistedState
                     = loadPersistedState(new LucenePersistedStateFactory(nodeEnvironment, xContentRegistry()))) {
                persistedState.setCurrentTerm(freshCurrentTerm);
            }
        }

        if (randomBoolean() && unimportantPaths.isEmpty() == false) {
            IOUtils.rm(randomFrom(unimportantPaths));
        }

        // verify that the freshest state is chosen
        try (final NodeEnvironment nodeEnvironment = newNodeEnvironment(dataPaths)) {
            try (CoordinationState.PersistedState persistedState
                     = loadPersistedState(new LucenePersistedStateFactory(nodeEnvironment, xContentRegistry()))) {
                assertThat(persistedState.getLastAcceptedState().term(), equalTo(freshTerm));
                assertThat(persistedState.getLastAcceptedState().version(), equalTo(freshVersion));
                assertThat(persistedState.getCurrentTerm(), equalTo(freshCurrentTerm));
            }
        }

        // verify that the freshest state was rewritten to each data path
        try (final NodeEnvironment nodeEnvironment = newNodeEnvironment(new Path[]{randomFrom(dataPaths)})) {
            try (CoordinationState.PersistedState persistedState
                     = loadPersistedState(new LucenePersistedStateFactory(nodeEnvironment, xContentRegistry()))) {
                assertThat(persistedState.getLastAcceptedState().term(), equalTo(freshTerm));
                assertThat(persistedState.getLastAcceptedState().version(), equalTo(freshVersion));
                assertThat(persistedState.getCurrentTerm(), equalTo(freshCurrentTerm));
            }
        }
    }

    public void testFailsOnMismatchedNodeIds() throws IOException {
        final Path[] dataPaths1 = createDataPaths();
        final Path[] dataPaths2 = createDataPaths();

        final String[] nodeIds = new String[2];

        try (final NodeEnvironment nodeEnvironment = newNodeEnvironment(dataPaths1)) {
            nodeIds[0] = nodeEnvironment.nodeId();
            try (CoordinationState.PersistedState persistedState
                     = loadPersistedState(new LucenePersistedStateFactory(nodeEnvironment, xContentRegistry()))) {
                persistedState.setLastAcceptedState(
                    ClusterState.builder(persistedState.getLastAcceptedState()).version(randomLongBetween(1L, Long.MAX_VALUE)).build());
            }
        }

        try (final NodeEnvironment nodeEnvironment = newNodeEnvironment(dataPaths2)) {
            nodeIds[1] = nodeEnvironment.nodeId();
            try (CoordinationState.PersistedState persistedState
                     = loadPersistedState(new LucenePersistedStateFactory(nodeEnvironment, xContentRegistry()))) {
                persistedState.setLastAcceptedState(
                    ClusterState.builder(persistedState.getLastAcceptedState()).version(randomLongBetween(1L, Long.MAX_VALUE)).build());
            }
        }

        for (Path dataPath : dataPaths2) {
            IOUtils.rm(dataPath.resolve(MetaDataStateFormat.STATE_DIR_NAME));
        }

        final Path[] combinedPaths = Stream.concat(Arrays.stream(dataPaths1), Arrays.stream(dataPaths2)).toArray(Path[]::new);

        try (final NodeEnvironment nodeEnvironment = newNodeEnvironment(combinedPaths)) {
            assertThat(expectThrows(IllegalStateException.class,
                () -> loadPersistedState(new LucenePersistedStateFactory(nodeEnvironment, xContentRegistry()))).getMessage(),
                allOf(containsString("unexpected node ID in metadata"), containsString(nodeIds[0]), containsString(nodeIds[1])));
        }
    }

    public void testFailsOnMismatchedCommittedClusterUUIDs() throws IOException {
        final Path[] dataPaths1 = createDataPaths();
        final Path[] dataPaths2 = createDataPaths();
        final Path[] combinedPaths = Stream.concat(Arrays.stream(dataPaths1), Arrays.stream(dataPaths2)).toArray(Path[]::new);

        final String clusterUUID1 = UUIDs.randomBase64UUID(random());
        final String clusterUUID2 = UUIDs.randomBase64UUID(random());

        // first establish consistent node IDs and write initial metadata
        try (final NodeEnvironment nodeEnvironment = newNodeEnvironment(combinedPaths)) {
            try (CoordinationState.PersistedState persistedState
                     = loadPersistedState(new LucenePersistedStateFactory(nodeEnvironment, xContentRegistry()))) {
                assertFalse(persistedState.getLastAcceptedState().metaData().clusterUUIDCommitted());
            }
        }

        try (final NodeEnvironment nodeEnvironment = newNodeEnvironment(dataPaths1)) {
            try (CoordinationState.PersistedState persistedState
                     = loadPersistedState(new LucenePersistedStateFactory(nodeEnvironment, xContentRegistry()))) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                persistedState.setLastAcceptedState(ClusterState.builder(clusterState)
                    .metaData(MetaData.builder(clusterState.metaData())
                        .clusterUUID(clusterUUID1)
                        .clusterUUIDCommitted(true)
                        .version(1))
                    .incrementVersion().build());
            }
        }

        try (final NodeEnvironment nodeEnvironment = newNodeEnvironment(dataPaths2)) {
            try (CoordinationState.PersistedState persistedState
                     = loadPersistedState(new LucenePersistedStateFactory(nodeEnvironment, xContentRegistry()))) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                persistedState.setLastAcceptedState(ClusterState.builder(clusterState)
                    .metaData(MetaData.builder(clusterState.metaData())
                        .clusterUUID(clusterUUID2)
                        .clusterUUIDCommitted(true)
                        .version(1))
                    .incrementVersion().build());
            }
        }

        try (final NodeEnvironment nodeEnvironment = newNodeEnvironment(combinedPaths)) {
            assertThat(expectThrows(IllegalStateException.class,
                () -> loadPersistedState(new LucenePersistedStateFactory(nodeEnvironment, xContentRegistry()))).getMessage(),
                allOf(containsString("mismatched cluster UUIDs in metadata"), containsString(clusterUUID1), containsString(clusterUUID2)));
        }
    }

    @AwaitsFix(bugUrl = "TODO")
    public void testPersistsAndReloadsIndexMetadataIffVersionChanges() throws IOException {
        try (final NodeEnvironment nodeEnvironment = newNodeEnvironment(createDataPaths())) {
            final LucenePersistedStateFactory persistedStateFactory = new LucenePersistedStateFactory(nodeEnvironment, xContentRegistry());
            final String clusterUUID = UUIDs.randomBase64UUID(random());
            final long version = randomLongBetween(1L, Long.MAX_VALUE);

            try (CoordinationState.PersistedState persistedState = loadPersistedState(persistedStateFactory)) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                persistedState.setLastAcceptedState(ClusterState.builder(clusterState)
                    .metaData(MetaData.builder(clusterState.metaData())
                        .clusterUUID(clusterUUID)
                        .clusterUUIDCommitted(true)
                        .version(version))
                    .incrementVersion().build());
                assertThat(persistedState.getLastAcceptedState().metaData().clusterUUID(), equalTo(clusterUUID));
                assertTrue(persistedState.getLastAcceptedState().metaData().clusterUUIDCommitted());
            }

            try (CoordinationState.PersistedState persistedState = loadPersistedState(persistedStateFactory)) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                assertThat(clusterState.metaData().clusterUUID(), equalTo(clusterUUID));
                assertTrue(clusterState.metaData().clusterUUIDCommitted());

                // ensure we do not wastefully persist the same global metadata version
                persistedState.setLastAcceptedState(ClusterState.builder(clusterState)
                    .metaData(MetaData.builder(clusterState.metaData())
                        .clusterUUID("not-the-UUID")
                        .version(version))
                    .incrementVersion().build());

                assertThat(persistedState.getLastAcceptedState().metaData().clusterUUID(), not(equalTo(clusterUUID)));
            }

            try (CoordinationState.PersistedState persistedState = loadPersistedState(persistedStateFactory)) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                assertThat(clusterState.metaData().clusterUUID(), equalTo(clusterUUID));
                assertTrue(clusterState.metaData().clusterUUIDCommitted());
            }
        }
    }

    @Override
    public Settings buildEnvSettings(Settings settings) {
        assertTrue(settings.hasValue(Environment.PATH_DATA_SETTING.getKey()));
        return Settings.builder()
            .put(settings)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath()).build();
    }

    public static Path[] createDataPaths() {
        final Path[] dataPaths = new Path[randomIntBetween(1, 5)];
        for (int i = 0; i < dataPaths.length; i++) {
            dataPaths[i] = createTempDir();
        }
        return dataPaths;
    }

    private NodeEnvironment newNodeEnvironment(Path[] dataPaths) throws IOException {
        return newNodeEnvironment(Settings.builder()
            .putList(Environment.PATH_DATA_SETTING.getKey(), Arrays.stream(dataPaths).map(Path::toString).collect(Collectors.toList()))
            .build());
    }

    private static CoordinationState.PersistedState loadPersistedState(LucenePersistedStateFactory persistedStateFactory) throws IOException {
        return persistedStateFactory.loadPersistedState(LucenePersistedStateFactoryTests::clusterStateFromMetadata);
    }

    private static ClusterState clusterStateFromMetadata(long version, MetaData metaData) {
        return ClusterState.builder(ClusterName.DEFAULT).version(version).metaData(metaData).build();
    }


}
