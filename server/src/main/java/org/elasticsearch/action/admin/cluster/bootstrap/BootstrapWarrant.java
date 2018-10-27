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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;

public class BootstrapWarrant implements ToXContentObject, Writeable {

    public static final ParseField NODES = new ParseField("nodes");
    public static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("bootstrap_warrant", true, Builder::new);

    static {
        PARSER.declareObjectArray(Builder::addAll, Node.PARSER, NODES);
    }

    private final List<Node> nodes;

    public BootstrapWarrant(List<Node> nodes) {
        this.nodes = nodes;
    }

    public BootstrapWarrant(StreamInput in) throws IOException {
        this.nodes = in.readList(Node::new);
    }

    public VotingConfiguration resolve(Iterable<DiscoveryNode> discoveredNodes) {
        final Set<DiscoveryNode> selectedNodes = new HashSet<>();
        for (final Node warrantNode : nodes) {
            boolean found = false;
            for (final DiscoveryNode discoveryNode : discoveredNodes) {
                assert discoveryNode.isMasterNode() : discoveryNode;
                if (discoveryNode.getName().equals(warrantNode.getName())) {
                    if (warrantNode.getId() == null || warrantNode.getId().equals(discoveryNode.getId())) {
                        if (found) {
                            throw new ElasticsearchException("discovered multiple nodes matching {} in {}", warrantNode, discoveredNodes);
                        }
                        found = true;
                        if (selectedNodes.add(discoveryNode) == false) {
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
        builder.startArray(NODES.getPreferredName());
        for (final Node warrantNode : nodes) {
            builder.value(warrantNode);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public String toString() {
        return "BootstrapWarrant{" +
            "nodes=" + nodes +
            '}';
    }

    public static class Node implements ToXContentObject, Writeable {
        public static final ParseField ID = new ParseField("id");
        public static final ParseField NAME = new ParseField("name");
        public static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("bootstrap_warrant_node", true, Builder::new);

        static {
            PARSER.declareString(Builder::id, ID);
            PARSER.declareString(Builder::name, NAME);
        }

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

        public Node(StreamInput in) throws IOException {
            id = in.readOptionalString();
            name = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(id);
            out.writeString(name);
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
                builder.field(ID.getPreferredName(), id);
            }
            builder.field(NAME.getPreferredName(), name);
            builder.endObject();
            return builder;
        }

        static class Builder {
            private String id;
            private String name;

            public void id(String id) {
                this.id = id;
            }

            public void name(String name) {
                this.name = name;
            }

            public Node build() {
                return new Node(id, name);
            }
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
            add(new Node(id, name));
        }

        private void add(Node.Builder nodeBuilder) {
            add(nodeBuilder.build());
        }

        private void add(Node node) {
            nodes.add(node);
        }

        public BootstrapWarrant build() {
            return new BootstrapWarrant(unmodifiableList(nodes));
        }

        public static void addAll(Builder builder, List<Node.Builder> nodeBuilders) {
            nodeBuilders.forEach(builder::add);
        }
    }
}
