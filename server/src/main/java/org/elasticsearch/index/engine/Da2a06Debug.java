/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.logging.log4j.LogManager;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Debug-session NDJSON logger for SDH E-10233. */
public final class Da2a06Debug {
    public static void log(String hypothesisId, String location, String message, String dataJsonObject) {
        String line = "{\"sessionId\":\"da2a06\",\"hypothesisId\":\""
            + hypothesisId
            + "\",\"location\":\""
            + location
            + "\",\"message\":\""
            + message
            + "\",\"data\":"
            + dataJsonObject
            + ",\"timestamp\":"
            + System.currentTimeMillis()
            + "}\n";
        try {
            LogManager.getLogger("DA2A06").warn(line.trim());
        } catch (Exception ignored) {}
        write(Path.of("/Users/davidturner/src/elasticsearch-9.5/.cursor/debug-da2a06.log"), line);
        write(Path.of(System.getProperty("java.io.tmpdir"), "debug-da2a06.log"), line);
    }

    private static void write(Path path, String line) {
        try {
            Files.writeString(path, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (Exception ignored) {}
    }
}
