/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.xcontent.ToXContentFragment;

public interface ShardCopyRole extends NamedWriteable, ToXContentFragment {
    String WRITEABLE_NAME = "shard_copy_role";

    /**
     * @return whether a shard copy with this role may be promoted from replica to primary. If {@code index.number_of_replicas} is reduced,
     * unpromotable replicas are removed first.
     */
    boolean isPromotableToPrimary();

    /**
     * @return whether a shard copy with this role may be the target of a search.
     */
    boolean isSearchable();
}
