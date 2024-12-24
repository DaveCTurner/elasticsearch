/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package fixture.aws.ec2;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

/**
 * Minimal HTTP handler that emulates the AWS STS server
 */
@SuppressForbidden(reason = "this test uses a HttpServer to emulate the AWS EC2 (DescribeInstances) endpoint")
public class AwsEc2HttpHandler implements HttpHandler {

    private final BiPredicate<String, String> authorizationPredicate;
    private final Supplier<List<String>> transportAddressesSupplier;

    public AwsEc2HttpHandler(BiPredicate<String, String> authorizationPredicate, Supplier<List<String>> transportAddressesSupplier) {
        this.authorizationPredicate = Objects.requireNonNull(authorizationPredicate);
        this.transportAddressesSupplier = Objects.requireNonNull(transportAddressesSupplier);
    }

    @Override
    public void handle(final HttpExchange exchange) throws IOException {
        try (exchange) {
            final var transportAddresses = transportAddressesSupplier.get();
            throw new UnsupportedOperationException("boom");
        }
    }
}
