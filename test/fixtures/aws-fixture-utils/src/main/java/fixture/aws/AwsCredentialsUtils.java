/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package fixture.aws;

import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

public enum AwsCredentialsUtils {
    ;

    public static BiPredicate<String, String> fixedAccessKey(String accessKey) {
        return mutableAccessKey(() -> accessKey);
    }

    public static BiPredicate<String, String> mutableAccessKey(Supplier<String> accessKeySupplier) {
        return (authorizationHeader, sessionTokenHeader) -> authorizationHeader != null
            && authorizationHeader.contains(accessKeySupplier.get());
    }

    public static BiPredicate<String, String> fixedAccessKeyAndToken(String accessKey, String sessionToken) {
        Objects.requireNonNull(sessionToken);
        final var accessKeyPredicate = fixedAccessKey(accessKey);
        return (authorizationHeader, sessionTokenHeader) -> accessKeyPredicate.test(authorizationHeader, sessionTokenHeader)
            && sessionToken.equals(sessionTokenHeader);
    }
}
