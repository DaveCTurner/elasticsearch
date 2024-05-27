/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.elasticsearch.test.ESTestCase;

public class ExcessiveLoggingTests extends ESTestCase {
    public void testExcessiveLogging() {
        for (int i = 0; i < 5000 * 1024; i++) {
            logger.info("message [{}]: {}", i, randomAlphaOfLength(1024));
        }
        logger.info("done");
        fail("boom");
    }
}
