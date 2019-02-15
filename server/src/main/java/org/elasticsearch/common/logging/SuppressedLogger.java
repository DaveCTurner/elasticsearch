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
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.collect.Tuple;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;

/**
 * Sometimes a few exceptions are to be expected, for instance during master elections. If we log every exception then the logs are full
 * of stack traces which makes it look like things are desperately broken even when the issues are merely transient. However if we do not
 * log any exceptions at all then we provide no useful feedback if something is persistently broken.
 *
 * This is a wrapper around a logger which helps in this situation. At DEBUG level, it simply logs the provided message and exception.
 * Above DEBUG level, it keeps hold of each provided message and exception until the {@code maximumSuppressedExceptions} limit is reached,
 * at which point it logs all the messages received so far and starts again. If the issues turned out to be transient, call {@code drain()}
 * to silently discard all the suppressed exceptions,
 */
public class SuppressedLogger {

    private final Logger logger;
    private final int maximumSuppressedExceptions;
    private final AtomicReference<List<Tuple<String, Exception>>> suppressedExceptions = new AtomicReference<>(emptyList());

    public SuppressedLogger(Logger logger, int maximumSuppressedExceptions) {
        if (maximumSuppressedExceptions <= 0) {
            throw new IllegalArgumentException("maximumSuppressedExceptions must be positive, but was " + maximumSuppressedExceptions);
        }
        this.logger = logger;
        this.maximumSuppressedExceptions = maximumSuppressedExceptions;
    }

    public void drain() {
        final List<Tuple<String, Exception>> currentSuppressedExceptions = suppressedExceptions.getAndSet(emptyList());
        assert currentSuppressedExceptions.size() <= maximumSuppressedExceptions
            : currentSuppressedExceptions + " longer than " + maximumSuppressedExceptions;
    }

    private void logSuppressed(Level level, Tuple<String, Exception> t) {
        logger.log(level, new ParameterizedMessage("{}", t.v1()), t.v2());
    }

    public void log(ParameterizedMessage parameterizedMessage, Exception exp) {
        if (logger.isDebugEnabled()) {
            final List<Tuple<String, Exception>> currentSuppressedExceptions = suppressedExceptions.getAndSet(emptyList());
            assert currentSuppressedExceptions.size() <= maximumSuppressedExceptions
                : currentSuppressedExceptions + " longer than " + maximumSuppressedExceptions;
            currentSuppressedExceptions.forEach(t -> logSuppressed(Level.DEBUG, t));
            logger.debug(parameterizedMessage, exp);
        } else {
            final Tuple<String, Exception> tuple = Tuple.tuple(parameterizedMessage.getFormattedMessage(), exp);
            final List<Tuple<String, Exception>> currentSuppressedExceptions
                = suppressedExceptions.getAndUpdate(l -> maximumSuppressedExceptions <= l.size()
                ? emptyList() : Stream.concat(l.stream(), Stream.of(tuple)).collect(Collectors.toList()));
            assert currentSuppressedExceptions.size() <= maximumSuppressedExceptions
                : currentSuppressedExceptions + " longer than " + maximumSuppressedExceptions;
            if (maximumSuppressedExceptions <= currentSuppressedExceptions.size()) {
                currentSuppressedExceptions.forEach(t -> logSuppressed(Level.INFO, t));
                logSuppressed(Level.INFO, tuple);
            }
        }
    }
}
