/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.xcontent.XContentBuilder;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class ShardCopyRoleIT extends ESIntegTestCase {

    private enum TestRole implements ShardCopyRole {
        ROLE_1((byte) 1, true, false),
        ROLE_2((byte) 2, true, true),
        ROLE_3((byte) 3, false, true);

        private final byte code;
        private final boolean promotable;
        private final boolean searchable;

        TestRole(byte code, boolean promotable, boolean searchable) {
            this.code = code;
            this.promotable = promotable;
            this.searchable = searchable;
        }

        @Override
        public String getWriteableName() {
            return ShardCopyRole.WRITEABLE_NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(code);
        }

        public static TestRole readFrom(StreamInput in) throws IOException {
            var code = in.readByte();
            for (TestRole value : TestRole.values()) {
                if (value.code == code) {
                    return value;
                }
            }
            assert false : code;
            throw new IllegalArgumentException("unknown shard role with code [" + code + "]");
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field("role", toString());
            // TODO test this is visible in routing table JSON
        }

        @Override
        public boolean isPromotableToPrimary() {
            return promotable;
        }

        @Override
        public boolean isSearchable() {
            return searchable;
        }
    }

    public static class TestPlugin extends Plugin implements ClusterPlugin {
        @Override
        public Writeable.Reader<ShardCopyRole> getShardRoleReader() {
            return TestRole::readFrom;
        }

        @Override
        public ShardCopyRoleFactory getShardRoleFactory() {
            return new ShardCopyRoleFactory() {
                @Override
                public ShardCopyRole newReplicaRole() {
                    return TestRole.ROLE_3;
                }

                @Override
                public ShardCopyRole newRestoredRole(int copyIndex) {
                    return newEmptyRole(copyIndex);
                }

                @Override
                public ShardCopyRole newEmptyRole(int copyIndex) {
                    return switch (copyIndex) {
                        case 0 -> TestRole.ROLE_1;
                        case 1 -> TestRole.ROLE_2;
                        default -> TestRole.ROLE_3;
                    };
                }
            };
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestPlugin.class);
    }

    public void testShardCopyRole() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(3);
        internalCluster().ensureAtMostNumDataNodes(3);

        createIndex(
            "test",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2).build()
        );
        ensureGreen("test");

        logger.info("--> {}", client().admin().cluster().prepareState().get().getState());

        final ClusterStateListener stateListener = new ClusterStateListener() {
            private final AtomicLong lastVersionLogged = new AtomicLong();

            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                final var version = event.state().version();
                if (lastVersionLogged.getAndUpdate(l -> Math.max(version, l)) < version) {
                    logger.info("--> routing nodes for version [{}]\n{}", version, event.state().getRoutingNodes().toString());
                }
            }
        };

        for (ClusterService clusterService : internalCluster().getInstances(ClusterService.class)) {
            clusterService.addListener(stateListener);
        }

        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                ensureStableCluster(internalCluster().getNodeNames().length);
                logger.info("--> node [{}] stopped", nodeName);
                return Settings.EMPTY;
            }
        });
        ensureGreen("test");

        logger.info("--> after restart");

        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );

        for (RoutingNode routingNode : client().admin().cluster().prepareState().get().getState().getRoutingNodes()) {
            for (ShardRouting shardRouting : routingNode) {
                assertThat(shardRouting.getRole(), Matchers.oneOf(TestRole.ROLE_1, TestRole.ROLE_2));
            }
        }
    }

}
