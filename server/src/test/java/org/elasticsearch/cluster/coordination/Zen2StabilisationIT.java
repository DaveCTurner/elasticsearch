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
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.coordination.Coordinator.Mode;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.cluster.coordination.ClusterBootstrapService.INITIAL_MASTER_NODE_COUNT_SETTING;
import static org.elasticsearch.discovery.zen.ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING;
import static org.elasticsearch.test.discovery.TestZenDiscovery.USE_ZEN2;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoMinMasterNodes = false)
@TestLogging("org.elasticsearch.cluster.coordination:TRACE,org.elasticsearch.discovery:TRACE")
public class Zen2StabilisationIT extends ESIntegTestCase {

    public void testTermDoesNotIncreaseAfterStabilisation() throws Exception {
        int nodeCount = randomIntBetween(2, 5);
        internalCluster().startNodes(nodeCount, Settings.builder()
            .put(INITIAL_MASTER_NODE_COUNT_SETTING.getKey(), nodeCount)
            .put(DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), Integer.MAX_VALUE)
            .put(USE_ZEN2.getKey(), true)
            .build());
        final List<Coordinator> coordinators = StreamSupport.stream(internalCluster().getInstances(Discovery.class).spliterator(), false)
            .map(d -> ((Coordinator) d)).collect(Collectors.toList());

        final AtomicLong term = new AtomicLong();

        assertBusy(() -> {
            final Optional<Coordinator> maybeLeader = coordinators.stream().filter(c -> c.getMode() == Mode.LEADER).findFirst();
            assertTrue(maybeLeader.isPresent());

            final Coordinator leader = maybeLeader.get();
            final Tuple<Long, Set<DiscoveryNode>> termAndVotes = leader.getTermAndVotesIfStableLeader();
            assertNotNull(termAndVotes);

            for (final Coordinator coordinator : coordinators) {
                assertTrue(termAndVotes.v2().contains(coordinator.getLocalNode()));
                assertTrue(coordinator == leader || coordinator.isStableFollowerInTerm(termAndVotes.v1()));
            }

            term.set(termAndVotes.v1());
        });

        createIndex("test");
        ensureGreen("test");

        for (Coordinator coordinator : coordinators) {
            assertEquals(coordinator.getLocalNode().toString(), coordinator.getCurrentTerm(), term.get());
        }
    }
}
