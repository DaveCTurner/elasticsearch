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

import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractTimeBasedUUIDGenerator implements UUIDGenerator {

    private static final byte[] SECURE_MUNGED_ADDRESS = MacAddressProvider.getSecureMungedAddress();
    // We only use bottom 3 bytes for the sequence number.  Paranoia: init with random int so that if JVM/OS/machine goes down, clock slips
    // backwards, and JVM comes back up, we are less likely to be on the same sequenceNumber at the same time:
    private final AtomicInteger sequenceNumber = new AtomicInteger(SecureRandomHolder.INSTANCE.nextInt());
    // Used to ensure clock moves forward:
    private long lastTimestamp;

    static {
        assert SECURE_MUNGED_ADDRESS.length == 6;
    }

    // protected for testing
    protected long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    // protected for testing
    protected byte[] macAddress() {
        return SECURE_MUNGED_ADDRESS;
    }

    @Override
    public String getBase64UUID()  {
        final int sequenceId = sequenceNumber.incrementAndGet() & 0xffffff;
        long timestamp = currentTimeMillis();

        synchronized (this) {
            // Don't let timestamp go backwards, at least "on our watch" (while this JVM is running).  We are still vulnerable if we are
            // shut down, clock goes backwards, and we restart... for this we randomize the sequenceNumber on init to decrease chance of
            // collision:
            timestamp = Math.max(lastTimestamp, timestamp);

            if (sequenceId == 0) {
                // Always force the clock to increment whenever sequence number is 0, in case we have a long time-slip backwards:
                timestamp++;
            }

            lastTimestamp = timestamp;
        }

        final byte[] uuidBytes = new byte[15];
        populateBytes(sequenceId, timestamp, uuidBytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(uuidBytes);
    }

    protected abstract void populateBytes(int sequenceId, long timestamp, byte[] uuidBytes);
}
