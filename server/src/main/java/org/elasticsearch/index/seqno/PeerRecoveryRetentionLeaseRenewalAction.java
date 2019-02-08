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
package org.elasticsearch.index.seqno;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.index.seqno.PeerRecoveryRetentionLeaseRenewalAction.Response;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * Background action to renew retention leases held to ensure that enough history is retained to perform a peer recovery if needed. This
 * action renews the lease for the sending shard, advancing the corresponding sequence number, and thereby releases any operations that
 * are now contained in a safe commit.
 */
public class PeerRecoveryRetentionLeaseRenewalAction extends Action<Response> {

    public static final String ACTION_NAME = "indices:admin/seq_no/peer_recovery_retention_lease_renewal";
    public static final PeerRecoveryRetentionLeaseRenewalAction INSTANCE = new PeerRecoveryRetentionLeaseRenewalAction();

    public PeerRecoveryRetentionLeaseRenewalAction() {
        super(ACTION_NAME);
    }

    @Override
    public Response newResponse() {
        throw new UnsupportedOperationException("use Writable instead");
    }

    @Override
    public Reader<Response> getResponseReader() {
        return Response::new;
    }

    public static final class Request extends ActionRequest {
        final ShardId shardId;
        final String nodeId;
        final long minimumPeerRecoverySeqNo;

        public Request(final ShardId shardId, String nodeId, long minimumPeerRecoverySeqNo) {
            this.shardId = shardId;
            this.nodeId = nodeId;
            this.minimumPeerRecoverySeqNo = minimumPeerRecoverySeqNo;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            shardId = ShardId.readShardId(in);
            nodeId = in.readString();
            minimumPeerRecoverySeqNo = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeString(nodeId);
            out.writeLong(minimumPeerRecoverySeqNo);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse {
        public Response() {
        }

        public Response(StreamInput in) throws IOException {
            super(in);
        }
    }
}
