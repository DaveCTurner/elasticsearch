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
package org.elasticsearch.action.admin.cluster.bootstrap;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterState.VotingConfiguration;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;

public class BootstrapWarrant implements ToXContentObject {

    private final List<Node> nodes;

    BootstrapWarrant(List<Node> nodes) {
        this.nodes = nodes;
    }

    public VotingConfiguration resolve(Iterable<DiscoveryNode> discoveredNodes) {
        final Set<DiscoveryNode> selectedNodes = new HashSet<>();
        for (final Node warrantNode : nodes) {
            boolean found = false;
            for (final DiscoveryNode discoveryNode : discoveredNodes) {
                if (discoveryNode.getName().equals(warrantNode.getName())) {
                    if (warrantNode.getId() == null || warrantNode.getId().equals(discoveryNode.getId())) {
                        if (found) {
                            throw new ElasticsearchException("discovered multiple nodes matching {} in {}", warrantNode, discoveredNodes);
                        }
                        found = true;
                        if (selectedNodes.add(discoveryNode)) {
                            throw new ElasticsearchException("multiple nodes matching {} in {}", discoveryNode, this);
                        }
                    } else {
                        throw new ElasticsearchException("node id mismatch comparing {} to {}", warrantNode, discoveryNode);
                    }
                } else if (warrantNode.getId() != null && warrantNode.getId().equals(discoveryNode.getId())) {
                    throw new ElasticsearchException("node name mismatch comparing {} to {}", warrantNode, discoveryNode);
                }
            }
            if (found == false) {
                throw new ElasticsearchException("no node matching {} found in {}", warrantNode, discoveredNodes);
            }
        }

        final Set<String> nodeIds = selectedNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
        assert nodeIds.size() == selectedNodes.size() : selectedNodes + " does not contain distinct IDs";
        return new VotingConfiguration(nodeIds);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("nodes");
        for (final Node warrantNode : nodes) {
            builder.value(warrantNode);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static class Node implements ToXContentObject {
        @Nullable
        private final String id;
        private final String name;

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public Node(@Nullable String id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public String toString() {
            return "Node{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                '}';
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (id != null) {
                builder.field("id", id);
            }
            builder.field("name", name);
            builder.endObject();
            return builder;
        }
    }

    public static class Builder {
        private final List<Node> nodes = new ArrayList<>();

        public void add(DiscoveryNode discoveryNode) {
            add(discoveryNode.getId(), discoveryNode.getName());
        }

        public void add(String name) {
            add(null, name);
        }

        public void add(@Nullable String id, String name) {
            nodes.add(new Node(id, name));
        }

        public BootstrapWarrant build() {
            return new BootstrapWarrant(unmodifiableList(nodes));
        }
    }
}
