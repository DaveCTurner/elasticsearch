/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.fixtures.minio;

import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.fixtures.testcontainers.DockerEnvironmentAwareTestContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.util.List;
import java.util.Random;

public final class MinioTestContainer extends DockerEnvironmentAwareTestContainer {

    private static final int servicePort = 9000;
    public static final String DOCKER_BASE_IMAGE = List.of(
        "minio/minio:RELEASE.2024-12-18T13-15-44Z", // 0, confirmed broken
        "minio/minio:RELEASE.2025-01-18T00-31-37Z", // 1
        "minio/minio:RELEASE.2025-01-20T14-49-07Z", // 2
        "minio/minio:RELEASE.2025-02-03T21-03-04Z", // 3
        "minio/minio:RELEASE.2025-02-07T23-21-09Z", // 4
        "minio/minio:RELEASE.2025-02-18T16-25-55Z", // 5
        "minio/minio:RELEASE.2025-02-28T09-55-16Z", // 6
        "minio/minio:RELEASE.2025-03-12T18-04-18Z", // 7
        "minio/minio:RELEASE.2025-04-03T14-56-28Z", // 8
        "minio/minio:RELEASE.2025-04-08T15-41-24Z"  // 9
    ).get(new Random(Murmur3HashFunction.hash(System.getProperty("tests.seed"))).nextInt(1, 10));

    private final boolean enabled;

    /**
     * for packer caching only
     * see CacheCacheableTestFixtures.
     * */
    protected MinioTestContainer() {
        this(true, "minio", "minio123", "test-bucket");
    }

    public MinioTestContainer(boolean enabled, String accessKey, String secretKey, String bucketName) {
        super(
            new ImageFromDockerfile("es-minio-testfixture").withDockerfileFromBuilder(
                builder -> builder.from(DOCKER_BASE_IMAGE)
                    .env("MINIO_ROOT_USER", accessKey)
                    .env("MINIO_ROOT_PASSWORD", secretKey)
                    .run("mkdir -p /minio/data/" + bucketName)
                    .cmd("server", "/minio/data")
                    .build()
            )
        );
        if (enabled) {
            addExposedPort(servicePort);
            // The following waits for a specific log message as the readiness signal. When the minio docker image
            // gets upgraded in future, we must ensure the log message still exists or update it here accordingly.
            // Otherwise the tests using the minio fixture will fail with timeout on waiting the container to be ready.
            setWaitStrategy(Wait.forLogMessage("API: .*:9000.*", 1));
        }
        this.enabled = enabled;
    }

    private static final Logger logger = LogManager.getLogger(MinioTestContainer.class);

    @Override
    public void start() {
        if (enabled) {
            withLogConsumer(
                frame -> logger.log(
                    frame.getType() == OutputFrame.OutputType.STDOUT ? Level.INFO : Level.WARN,
                    "MINIO LOG: " + frame.getUtf8String()
                )
            );
            super.start();
        }
    }

    public String getAddress() {
        return "http://127.0.0.1:" + getMappedPort(servicePort);
    }
}
