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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class BootstrapConfiguration implements Writeable {

    private final List<NodeDescription> nodeDescriptions;

    public BootstrapConfiguration(List<NodeDescription> nodeDescriptions) {
        if (nodeDescriptions.isEmpty()) {
            throw new IllegalArgumentException("cannot create empty bootstrap configuration");
        }
        this.nodeDescriptions = nodeDescriptions;
    }

    public BootstrapConfiguration(StreamInput in) throws IOException {
        this.nodeDescriptions = in.readList(NodeDescription::new);
        if (nodeDescriptions.isEmpty()) {
            throw new IllegalArgumentException("cannot create empty bootstrap configuration");
        }
    }

    public VotingConfiguration resolve(Iterable<DiscoveryNode> discoveredNodes) {
        final Set<DiscoveryNode> selectedNodes = new HashSet<>();
        for (final NodeDescription nodeDescription : nodeDescriptions) {
            final DiscoveryNode discoveredNode = nodeDescription.resolve(discoveredNodes);
            if (selectedNodes.add(discoveredNode) == false) {
                throw new ElasticsearchException("multiple nodes matching {} in {}", discoveredNode, this);
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

        @Nullable
        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public NodeDescription(DiscoveryNode discoveryNode) {
            this.id = discoveryNode.getId();
            this.name = Objects.requireNonNull(discoveryNode.getName());
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
            DiscoveryNode selectedNode = null;
            for (final DiscoveryNode discoveredNode : discoveredNodes) {
                assert discoveredNode.isMasterNode() : discoveredNode;
                if (discoveredNode.getName().equals(name)) {
                    if (id == null || id.equals(discoveredNode.getId())) {
                        if (selectedNode != null) {
                            throw new ElasticsearchException(
                                "discovered multiple nodes matching {} in {}", this, discoveredNodes);
                        }
                        selectedNode = discoveredNode;
                    } else {
                        throw new ElasticsearchException("node id mismatch comparing {} to {}", this, discoveredNode);
                    }
                } else if (id != null && id.equals(discoveredNode.getId())) {
                    throw new ElasticsearchException("node name mismatch comparing {} to {}", this, discoveredNode);
                }
            }
            if (selectedNode == null) {
                throw new ElasticsearchException("no node matching {} found in {}", this, discoveredNodes);
            }

            return selectedNode;
        }
    }
}
