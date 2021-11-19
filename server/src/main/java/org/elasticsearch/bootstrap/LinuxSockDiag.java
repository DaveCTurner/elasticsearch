/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.apache.lucene.util.Constants;

public final class LinuxSockDiag {

    private static final int AF_NETLINK = 16;
    private static final int SOCK_DGRAM = 2;
    private static final int NETLINK_INET_DIAG = 4;

    private LinuxSockDiag() {
        assert false;
    }

    public static void stuff() {
        if (Constants.LINUX == false) {
            return;
        }

        final int fd = JNACLibrary.socket(AF_NETLINK, SOCK_DGRAM, NETLINK_INET_DIAG);
        JNACLibrary.close(fd);
    }
}
