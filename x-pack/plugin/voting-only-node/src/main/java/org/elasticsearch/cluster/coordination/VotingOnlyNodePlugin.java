/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.cluster.coordination;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetaData;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING;

public class VotingOnlyNodePlugin extends Plugin implements NetworkPlugin, ClusterPlugin {

    public static final Setting<Boolean> VOTING_ONLY_NODE_SETTING
        = Setting.boolSetting("node.voting_only", false, Setting.Property.NodeScope);

    private static final String VOTING_ONLY_NODE_ALLOCATOR_NAME = "voting-only-node-allocator";
    private static final String VOTING_ONLY_NODE_ATTRIBUTE_NAME = "voting-only";

    private final Boolean isVotingOnlyNode;
    private final Settings settings;
    private final SetOnce<ThreadPool> threadPool = new SetOnce<>();

    public VotingOnlyNodePlugin(Settings settings) {
        this.settings = settings;
        isVotingOnlyNode = VOTING_ONLY_NODE_SETTING.get(settings);
        if (isVotingOnlyNode && Node.NODE_MASTER_SETTING.get(settings) == false) {
            throw new IllegalStateException("voting-only node must be master-eligible");
        }
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
        this.threadPool.set(threadPool);
        return Collections.emptyList();
    }

    @Override
    public Settings additionalSettings() {
        final Setting<String> attributeSetting = Node.NODE_ATTRIBUTES.getConcreteSettingForNamespace(VOTING_ONLY_NODE_ATTRIBUTE_NAME);
        if (attributeSetting.exists(settings)) {
            throw new IllegalStateException(
                "setting [" + attributeSetting.getKey() + "] is forbidden; set [" + VOTING_ONLY_NODE_SETTING.getKey() + "] instead");
            // NOCOMMIT this is a slight breaking change if anyone is already setting node.attr.voting_only, are we ok with this?
        }

        final Settings.Builder builder = Settings.builder();
        if (isVotingOnlyNode) {
            if (SHARDS_ALLOCATOR_TYPE_SETTING.exists(settings)) {
                throw new IllegalStateException(
                    "setting [" + SHARDS_ALLOCATOR_TYPE_SETTING.getKey() + "] on a voting-only node is forbidden");
            }
            builder.put(SHARDS_ALLOCATOR_TYPE_SETTING.getKey(), VOTING_ONLY_NODE_ALLOCATOR_NAME);
            builder.put(attributeSetting.getKey(), "true");
        }
        return builder.build();
    }

    @Override
    public Map<String, Supplier<ShardsAllocator>> getShardsAllocators(Settings settings, ClusterSettings clusterSettings) {
        return Map.of(VOTING_ONLY_NODE_ALLOCATOR_NAME, VotingOnlyNodeAllocator::new);
    }

    @Override
    public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry namedWriteableRegistry, ThreadContext threadContext) {
        if (isVotingOnlyNode) {
            return Collections.singletonList(new TransportInterceptor() {
                @Override
                public AsyncSender interceptSender(AsyncSender sender) {
                    return new VotingOnlyNodeAsyncSender(sender);
                }
            });
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Collections.singletonList(VOTING_ONLY_NODE_SETTING);
    }

    /**
     * No-op allocator to skip rerouting on this node
     */
    private static class VotingOnlyNodeAllocator implements ShardsAllocator {
        @Override
        public void allocate(RoutingAllocation allocation) {
            // do nothing
        }

        @Override
        public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
            return ShardAllocationDecision.NOT_TAKEN;
        }
    }

    /**
     * Wrapper around a {@link TransportInterceptor.AsyncSender} which suppresses unwanted outgoing messages from a voting-only node
     */
    private class VotingOnlyNodeAsyncSender implements TransportInterceptor.AsyncSender {
        private final TransportInterceptor.AsyncSender inner;

        public VotingOnlyNodeAsyncSender(TransportInterceptor.AsyncSender inner) {
            this.inner = inner;
        }

        @Override
        public <T extends TransportResponse> void sendRequest(Transport.Connection connection, String action,
                                                              TransportRequest request, TransportRequestOptions options,
                                                              TransportResponseHandler<T> handler) {
            switch (action) {
                case PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME: {
                    final DiscoveryNode node = connection.getNode();
                    if (node.isMasterNode() && node.getAttributes().containsKey(VOTING_ONLY_NODE_ATTRIBUTE_NAME) == false) {
                        // NOCOMMIT if the remote node does not have this plugin installed then the user can set the
                        // voting-only attribute by hand. Do we care?
                        inner.sendRequest(connection, action, request, options, new TransportResponseHandler<T>() {
                            @Override
                            public void handleResponse(T response) {
                                handler.handleException(new TransportException(
                                    "voting-only node ignoring successful response ["
                                        + response + "] from [" + node + "]"));
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                handler.handleException(exp);
                            }

                            @Override
                            public String executor() {
                                return handler.executor();
                            }

                            @Override
                            public T read(StreamInput in) throws IOException {
                                return handler.read(in);
                            }
                        });
                    } else {
                        threadPool.get().executor(handler.executor()).execute(() ->
                            handler.handleException(new TransportException(
                                "voting-only node skipping publication to master-ineligible [" + node + "]")));
                    }
                    break;
                }

                case PublicationTransportHandler.COMMIT_STATE_ACTION_NAME: {
                    final DiscoveryNode node = connection.getNode();
                    assert false : "unexpected [" + action + "] with [" + request + "] to [" + node + "]";
                    threadPool.get().executor(handler.executor()).execute(() -> handler.handleException(
                        new TransportException(
                            "unexpected commit request [" + request + "] to [" + node + "] from voting-only node")));
                    break;
                }

                case TransportNodesListGatewayStartedShards.ACTION_NAME:
                case TransportNodesListShardStoreMetaData.ACTION_NAME: {
                    threadPool.get().executor(handler.executor()).execute(() -> handler.handleException(
                        new TransportException(
                            "voting-only node skipping [" + action + "] to [" + connection.getNode() + "]")));
                    break;
                }

                default: {
                    inner.sendRequest(connection, action, request, options, handler);
                    break;
                }
            }
        }
    }
}
