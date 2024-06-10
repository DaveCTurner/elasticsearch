/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.operation.hash.murmur3;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class Murmur3HashFunctionTests extends ESTestCase {

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return List.of(
            new Object[] { 0x00000000, "" },
            new Object[] { 0x3fb8ba02, "h" },
            new Object[] { 0xce30a49f, "he" },
            new Object[] { 0x0c7cee6b, "hel" },
            new Object[] { 0x5a0cb7c3, "hell" },
            new Object[] { 0xd7c31989, "hello" },
            new Object[] { 0x22ab2984, "hello w" },
            new Object[] { 0xdf0ca123, "hello wo" },
            new Object[] { 0xe7744d61, "hello wor" },
            new Object[] { 0x51837ec6, "hello worl" },
            new Object[] { 0x64b256a4, "hello world" },
            new Object[] { 0xda1575e5, "hello world!" },
            new Object[] { 0x8ded82be, "Γειά σου Κόσμε" },
            new Object[] { 0x8274606c, "Supplementary planes too 🙂" },
            new Object[] { 0xe07db09c, "The quick brown fox jumps over the lazy dog" },
            new Object[] { 0x4e63d2ad, "The quick brown fox jumps over the lazy cog" }
        );
    }

    private final int expectedHash;
    private final String stringInput;

    public Murmur3HashFunctionTests(@Name("expectedHash") int expectedHash, @Name("stringInput") String stringInput) {
        this.expectedHash = expectedHash;
        this.stringInput = stringInput;
    }

    public void testKnownValue() {
        assertEquals(expectedHash, Murmur3HashFunction.hash(stringInput));
    }
}
