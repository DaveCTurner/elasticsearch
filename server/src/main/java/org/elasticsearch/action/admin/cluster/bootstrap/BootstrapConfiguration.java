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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;

public class BootstrapConfiguration implements Writeable {

    private final List<NodeDescription> nodeDescriptions;

    public BootstrapConfiguration(List<NodeDescription> nodeDescriptions) {
        this.nodeDescriptions = nodeDescriptions;
    }

    public BootstrapConfiguration(StreamInput in) throws IOException {
        this.nodeDescriptions = in.readList(NodeDescription::new);
    }

    public VotingConfiguration resolve(Iterable<DiscoveryNode> discoveredNodes) {
        final Set<DiscoveryNode> selectedNodes = new HashSet<>();
        for (final NodeDescription nodeDescription : nodeDescriptions) {
            final DiscoveryNode discoveredNode = nodeDescription.resolve(discoveredNodes);
            if (selectedNodes.add(discoveredNode) == false) {
                throw new ElasticsearchException("multiple nodeDescriptions matching {} in {}", discoveredNode, this);
            }
        }

        final Set<String> nodeIds = selectedNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
        assert nodeIds.size() == selectedNodes.size() : selectedNodes + " does not contain distinct IDs";
        return new VotingConfiguration(nodeIds);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(nodeDescriptions);
    }

    @Override
    public String toString() {
        return "BootstrapConfiguration{" +
            "nodeDescriptions=" + nodeDescriptions +
            '}';
    }

    public static class NodeDescription implements Writeable {

        @Nullable
        private final String id;
        private final String name;

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public NodeDescription(@Nullable String id, String name) {
            this.id = id;
            this.name = name;
        }

        public NodeDescription(StreamInput in) throws IOException {
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
            return "NodeDescription{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                '}';
        }

        public DiscoveryNode resolve(Iterable<DiscoveryNode> discoveredNodes) {
            NodeDescription nodeDescription = this;
            DiscoveryNode selectedNode = null;
            for (final DiscoveryNode discoveredNode : discoveredNodes) {
                assert discoveredNode.isMasterNode() : discoveredNode;
                if (discoveredNode.getName().equals(nodeDescription.getName())) {
                    if (nodeDescription.getId() == null || nodeDescription.getId().equals(discoveredNode.getId())) {
                        if (selectedNode != null) {
                            throw new ElasticsearchException(
                                "discovered multiple nodes matching {} in {}", nodeDescription, discoveredNodes);
                        }
                        selectedNode = discoveredNode;
                    } else {
                        throw new ElasticsearchException("node id mismatch comparing {} to {}", nodeDescription, discoveredNode);
                    }
                } else if (nodeDescription.getId() != null && nodeDescription.getId().equals(discoveredNode.getId())) {
                    throw new ElasticsearchException("node name mismatch comparing {} to {}", nodeDescription, discoveredNode);
                }
            }
            if (selectedNode == null) {
                throw new ElasticsearchException("no node matching {} found in {}", nodeDescription, discoveredNodes);
            }

            return selectedNode;
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

            public NodeDescription build() {
                return new NodeDescription(id, name);
            }
        }
    }

    public static class Builder {
        private final List<NodeDescription> nodeDescriptions = new ArrayList<>();

        public void add(DiscoveryNode discoveryNode) {
            add(discoveryNode.getId(), discoveryNode.getName());
        }

        public void add(@Nullable String id, String name) {
            add(new NodeDescription(id, name));
        }

        private void add(NodeDescription node) {
            nodeDescriptions.add(node);
        }

        public BootstrapConfiguration build() {
            return new BootstrapConfiguration(unmodifiableList(nodeDescriptions));
        }
    }
}
