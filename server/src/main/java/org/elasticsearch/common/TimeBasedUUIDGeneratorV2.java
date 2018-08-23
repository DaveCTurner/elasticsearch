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

package org.elasticsearch.common;

public class TimeBasedUUIDGeneratorV2 extends AbstractTimeBasedUUIDGenerator {
    @Override
    protected void populateBytes(int sequenceId, long timestamp, byte[] uuidBytes) {
        int i = 0;

        // Initially we put the remaining bytes, which will likely not be compressed at all.
        uuidBytes[i++] = (byte) (timestamp >>> 8);
        uuidBytes[i++] = (byte) (sequenceId >>> 8);
        uuidBytes[i++] = (byte) timestamp;

        uuidBytes[i++] = (byte) sequenceId;
        // changes every 65k docs, so potentially every second if you have a steady indexing rate
        uuidBytes[i++] = (byte) (sequenceId >>> 16);

        // Now we start focusing on compression and put bytes that should not change too often.
        uuidBytes[i++] = (byte) (timestamp >>> 16); // changes every ~65 secs
        uuidBytes[i++] = (byte) (timestamp >>> 24); // changes every ~4.5h
        uuidBytes[i++] = (byte) (timestamp >>> 32); // changes every ~50 days
        uuidBytes[i++] = (byte) (timestamp >>> 40); // changes every 35 years
        byte[] macAddress = macAddress();
        assert macAddress.length == 6;
        System.arraycopy(macAddress, 0, uuidBytes, i, macAddress.length);

        assert i == uuidBytes.length;
    }
}
