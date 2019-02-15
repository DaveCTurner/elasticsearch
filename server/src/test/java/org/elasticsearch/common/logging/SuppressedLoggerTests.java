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
package org.elasticsearch.common.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.filter.RegexFilter;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class SuppressedLoggerTests extends ESTestCase {

    public void testMaxSuppressedMustBePositive() {
        final IllegalArgumentException e0 = expectThrows(IllegalArgumentException.class, () -> new SuppressedLogger(logger, 0));
        assertThat(e0.getMessage(), equalTo("maximumSuppressedExceptions must be positive, but was 0"));

        final int nonpos = randomIntBetween(Integer.MIN_VALUE, 0);
        final IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class, () -> new SuppressedLogger(logger, nonpos));
        assertThat(e1.getMessage(), equalTo("maximumSuppressedExceptions must be positive, but was " + nonpos));
    }

    class MockAppender extends AbstractAppender {
        final List<LogEvent> logEvents = new ArrayList<>();

        MockAppender(final String name) throws IllegalAccessException {
            super(name, RegexFilter.createFilter(".*(\n.*)*", new String[0], false, null, null), null);
        }

        @Override
        public void append(LogEvent event) {
            logEvents.add(event.toImmutable());
        }
    }

    public void testSuppressedLogger() throws IllegalAccessException {
        final MockAppender appender = new MockAppender("trace_appender");
        appender.start();

        final Logger testLogger = LogManager.getLogger(randomAlphaOfLength(10));
        Loggers.addAppender(testLogger, appender);
        Loggers.setLevel(testLogger, Level.INFO);

        final int maxSuppressed = randomIntBetween(1, 5);
        final SuppressedLogger suppressedLogger = new SuppressedLogger(testLogger, maxSuppressed);

        final List<String> suppressedStrings = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final String string = randomAlphaOfLength(10);
            suppressedLogger.log(new ParameterizedMessage(string), new ElasticsearchException(""));
            suppressedStrings.add(string);

            if (suppressedStrings.size() <= maxSuppressed) {
                assertThat(appender.logEvents, empty());
            } else {
                assertThat(appender.logEvents.stream().map(l -> l.getMessage().getFormattedMessage()).collect(Collectors.toList()),
                    equalTo(suppressedStrings));
                appender.logEvents.clear();
                suppressedStrings.clear();
            }
        }

        Loggers.setLevel(testLogger, Level.DEBUG);

        for (int i = 0; i < 10; i++) {
            final String string = randomAlphaOfLength(10);
            suppressedLogger.log(new ParameterizedMessage(string), new ElasticsearchException(""));
            suppressedStrings.add(string);
            assertThat(appender.logEvents.stream().map(l -> l.getMessage().getFormattedMessage()).collect(Collectors.toList()),
                equalTo(suppressedStrings));
        }
    }
}
