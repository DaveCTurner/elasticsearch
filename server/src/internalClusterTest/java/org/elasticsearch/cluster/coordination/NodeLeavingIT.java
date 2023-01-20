/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;

public class NodeLeavingIT extends ESIntegTestCase {
    public void testNodeLeaving() throws IOException {
        internalCluster().startMasterOnlyNode();
        internalCluster().startMasterOnlyNode();

        for (int i = 0; i < 3; i++) {
            internalCluster().stopCurrentMasterNode();
            internalCluster().startMasterOnlyNode();
        }
    }
}
