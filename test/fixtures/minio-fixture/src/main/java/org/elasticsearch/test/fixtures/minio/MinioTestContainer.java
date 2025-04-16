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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.fixtures.testcontainers.DockerEnvironmentAwareTestContainer;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.SelinuxContext;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;

public final class MinioTestContainer extends DockerEnvironmentAwareTestContainer {

    private static final int servicePort = 9000;

    static {
        final var allReleases = List.of(
            "RELEASE.2021-03-01T04-20-55Z",
            "RELEASE.2021-03-04T00-53-13Z",
            "RELEASE.2021-03-10T05-11-33Z",
            "RELEASE.2021-03-12T00-00-47Z",
            "RELEASE.2021-03-17T02-33-02Z",
            "RELEASE.2021-03-26T00-00-41Z",
            "RELEASE.2021-04-06T23-11-00Z",
            "RELEASE.2021-04-18T19-26-29Z",
            "RELEASE.2021-04-22T15-44-28Z",
            // "RELEASE.2021-05-11T23-27-41Z", // docker image missing?
            // "RELEASE.2021-05-16T05-32-34Z", // docker image missing?
            // "RELEASE.2021-05-18T00-53-28Z", // docker image missing?
            // "RELEASE.2021-05-20T22-31-44Z", // docker image missing?
            // "RELEASE.2021-05-22T02-34-39Z", // docker image missing?
            // "RELEASE.2021-05-26T00-22-46Z", // docker image missing?
            // "RELEASE.2021-05-27T22-06-31Z", // docker image missing?
            "RELEASE.2021-06-07T21-40-51Z",
            "RELEASE.2021-06-09T18-51-39Z",
            "RELEASE.2021-06-14T01-29-23Z",
            "RELEASE.2021-06-17T00-10-46Z",
            "RELEASE.2021-07-08T01-15-01Z",
            "RELEASE.2021-07-08T19-43-25Z",
            "RELEASE.2021-07-12T02-44-53Z",
            "RELEASE.2021-07-15T22-27-34Z",
            "RELEASE.2021-07-21T22-15-23Z",
            "RELEASE.2021-07-22T05-23-32Z",
            "RELEASE.2021-07-27T02-40-15Z",
            "RELEASE.2021-07-30T00-02-00Z",
            "RELEASE.2021-08-05T22-01-19Z",
            "RELEASE.2021-08-17T20-53-08Z",
            "RELEASE.2021-08-20T18-32-01Z",
            "RELEASE.2021-08-25T00-41-18Z",
            "RELEASE.2021-08-31T05-46-54Z",
            "RELEASE.2021-09-03T03-56-13Z",
            "RELEASE.2021-09-09T21-37-07Z",
            "RELEASE.2021-09-15T04-54-25Z",
            "RELEASE.2021-09-18T18-09-59Z",
            "RELEASE.2021-09-23T04-46-24Z",
            "RELEASE.2021-09-24T00-24-24Z",
            "RELEASE.2021-10-02T16-31-05Z",
            "RELEASE.2021-10-06T23-36-31Z",
            "RELEASE.2021-10-08T23-58-24Z",
            "RELEASE.2021-10-10T16-53-30Z",
            "RELEASE.2021-10-13T00-23-17Z",
            "RELEASE.2021-10-23T03-28-24Z",
            "RELEASE.2021-10-27T16-29-42Z",
            "RELEASE.2021-11-03T03-36-36Z",
            "RELEASE.2021-11-05T09-16-26Z",
            "RELEASE.2021-11-09T03-21-45Z",
            "RELEASE.2021-11-24T23-19-33Z",
            "RELEASE.2021-12-09T06-19-41Z",
            "RELEASE.2021-12-10T23-03-39Z",
            "RELEASE.2021-12-18T04-42-33Z",
            "RELEASE.2021-12-20T22-07-16Z",
            "RELEASE.2021-12-27T07-23-18Z",
            "RELEASE.2021-12-29T06-49-06Z",
            "RELEASE.2022-01-03T18-22-58Z",
            "RELEASE.2022-01-04T07-41-07Z",
            "RELEASE.2022-01-07T01-53-23Z",
            "RELEASE.2022-01-08T03-11-54Z",
            "RELEASE.2022-01-25T19-56-04Z",
            "RELEASE.2022-01-27T03-53-02Z",
            "RELEASE.2022-01-28T02-28-16Z",
            "RELEASE.2022-02-01T18-00-14Z",
            "RELEASE.2022-02-05T04-40-59Z",
            "RELEASE.2022-02-07T08-17-33Z",
            "RELEASE.2022-02-12T00-51-25Z",
            "RELEASE.2022-02-16T00-35-27Z",
            "RELEASE.2022-02-17T23-22-26Z",
            "RELEASE.2022-02-18T01-50-10Z",
            "RELEASE.2022-02-24T22-12-01Z",
            "RELEASE.2022-02-26T02-54-46Z",
            "RELEASE.2022-03-03T21-21-16Z",
            "RELEASE.2022-03-05T06-32-39Z",
            "RELEASE.2022-03-08T22-28-51Z",
            "RELEASE.2022-03-11T11-08-23Z",
            "RELEASE.2022-03-11T23-57-45Z",
            "RELEASE.2022-03-14T18-25-24Z",
            "RELEASE.2022-03-17T02-57-36Z",
            "RELEASE.2022-03-17T06-34-49Z",
            "RELEASE.2022-03-22T02-05-10Z",
            "RELEASE.2022-03-24T00-43-44Z",
            "RELEASE.2022-03-26T06-49-28Z",
            "RELEASE.2022-04-01T03-41-39Z",
            "RELEASE.2022-04-08T19-44-35Z",
            "RELEASE.2022-04-09T15-09-52Z",
            "RELEASE.2022-04-12T06-55-35Z",
            "RELEASE.2022-04-16T04-26-02Z",
            "RELEASE.2022-04-26T01-20-24Z",
            "RELEASE.2022-04-29T01-27-09Z",
            "RELEASE.2022-04-30T22-23-53Z",
            "RELEASE.2022-05-03T20-36-08Z",
            "RELEASE.2022-05-04T07-45-27Z",
            "RELEASE.2022-05-08T23-50-31Z",
            "RELEASE.2022-05-19T18-20-59Z",
            "RELEASE.2022-05-23T18-45-11Z",
            "RELEASE.2022-05-26T05-48-41Z",
            "RELEASE.2022-06-02T02-11-04Z",
            "RELEASE.2022-06-02T16-16-26Z",
            "RELEASE.2022-06-03T01-40-53Z",
            "RELEASE.2022-06-06T23-14-52Z",
            "RELEASE.2022-06-07T00-33-41Z",
            "RELEASE.2022-06-10T16-59-15Z",
            "RELEASE.2022-06-11T19-55-32Z",
            "RELEASE.2022-06-17T02-00-35Z",
            "RELEASE.2022-06-20T23-13-45Z",
            "RELEASE.2022-06-25T15-50-16Z",
            "RELEASE.2022-06-30T20-58-09Z",
            "RELEASE.2022-07-04T21-02-54Z",
            "RELEASE.2022-07-06T20-29-49Z",
            "RELEASE.2022-07-08T00-05-23Z",
            "RELEASE.2022-07-13T23-29-44Z",
            "RELEASE.2022-07-15T03-44-22Z",
            "RELEASE.2022-07-17T15-43-14Z",
            "RELEASE.2022-07-24T01-54-52Z",
            "RELEASE.2022-07-24T17-09-31Z",
            "RELEASE.2022-07-26T00-53-03Z",
            "RELEASE.2022-07-29T19-40-48Z",
            "RELEASE.2022-07-30T05-21-40Z",
            "RELEASE.2022-08-02T23-59-16Z",
            "RELEASE.2022-08-05T23-27-09Z",
            "RELEASE.2022-08-08T18-34-09Z",
            "RELEASE.2022-08-11T04-37-28Z",
            "RELEASE.2022-08-13T21-54-44Z",
            "RELEASE.2022-08-22T23-53-06Z",
            "RELEASE.2022-08-25T07-17-05Z",
            "RELEASE.2022-08-26T19-53-15Z",
            "RELEASE.2022-09-01T23-53-36Z",
            "RELEASE.2022-09-07T22-25-02Z",
            "RELEASE.2022-09-17T00-09-45Z",
            "RELEASE.2022-09-22T18-57-27Z",
            "RELEASE.2022-09-25T15-44-53Z",
            "RELEASE.2022-10-02T19-29-29Z",
            "RELEASE.2022-10-05T14-58-27Z",
            "RELEASE.2022-10-08T20-11-00Z",
            "RELEASE.2022-10-15T19-57-03Z",
            "RELEASE.2022-10-20T00-55-09Z",
            "RELEASE.2022-10-21T22-37-48Z",
            "RELEASE.2022-10-24T18-35-07Z",
            "RELEASE.2022-10-29T06-21-33Z",
            "RELEASE.2022-11-08T05-27-07Z",
            "RELEASE.2022-11-10T18-20-21Z",
            "RELEASE.2022-11-11T03-44-20Z",
            "RELEASE.2022-11-17T23-20-09Z",
            "RELEASE.2022-11-26T22-43-32Z",
            "RELEASE.2022-11-29T23-40-49Z",
            "RELEASE.2022-12-02T19-19-22Z",
            "RELEASE.2022-12-07T00-56-37Z",
            "RELEASE.2022-12-12T19-27-27Z",
            "RELEASE.2023-01-02T09-40-09Z",
            "RELEASE.2023-01-06T18-11-18Z",
            "RELEASE.2023-01-12T02-06-16Z",
            "RELEASE.2023-01-18T04-36-38Z",
            "RELEASE.2023-01-20T02-05-44Z",
            "RELEASE.2023-01-25T00-19-54Z",
            "RELEASE.2023-01-31T02-24-19Z",
            "RELEASE.2023-02-09T05-16-53Z",
            "RELEASE.2023-02-10T18-48-39Z",
            "RELEASE.2023-02-17T17-52-43Z",
            "RELEASE.2023-02-22T18-23-45Z",
            "RELEASE.2023-02-27T18-10-45Z",
            "RELEASE.2023-03-09T23-16-13Z",
            "RELEASE.2023-03-13T19-46-17Z",
            "RELEASE.2023-03-20T20-16-18Z",
            "RELEASE.2023-03-22T06-36-24Z",
            "RELEASE.2023-03-24T21-41-23Z",
            "RELEASE.2023-04-07T05-28-58Z",
            "RELEASE.2023-04-13T03-08-07Z",
            "RELEASE.2023-04-20T17-56-55Z",
            "RELEASE.2023-04-28T18-11-17Z",
            "RELEASE.2023-05-04T21-44-30Z",
            "RELEASE.2023-05-18T00-05-36Z",
            "RELEASE.2023-05-27T05-56-19Z",
            "RELEASE.2023-06-02T23-17-26Z",
            "RELEASE.2023-06-09T07-32-12Z",
            "RELEASE.2023-06-16T02-41-06Z",
            "RELEASE.2023-06-19T19-52-50Z",
            "RELEASE.2023-06-23T20-26-00Z",
            "RELEASE.2023-06-29T05-12-28Z",
            "RELEASE.2023-07-07T07-13-57Z",
            "RELEASE.2023-07-11T21-29-34Z",
            "RELEASE.2023-07-18T17-49-40Z"
            // "RELEASE.2023-07-21T21-12-44Z", // uncontended register operation failed: expected [0] but did not observe any value
            // "RELEASE.2023-08-04T17-40-21Z",
            // "RELEASE.2023-08-09T23-30-22Z",
            // "RELEASE.2023-08-16T20-17-30Z",
            // "RELEASE.2023-08-23T10-07-06Z",
            // "RELEASE.2023-08-29T23-07-35Z",
            // "RELEASE.2023-08-31T15-31-16Z",
            // "RELEASE.2023-09-04T19-57-37Z",
            // "RELEASE.2023-09-07T02-05-02Z",
            // "RELEASE.2023-09-16T01-01-47Z",
            // "RELEASE.2023-09-20T22-49-55Z",
            // "RELEASE.2023-09-23T03-47-50Z",
            // "RELEASE.2023-09-27T15-22-50Z",
            // "RELEASE.2023-09-30T07-02-29Z",
            // "RELEASE.2023-10-07T15-07-38Z",
            // "RELEASE.2023-10-14T05-17-22Z",
            // "RELEASE.2023-10-16T04-13-43Z",
            // "RELEASE.2023-10-24T04-42-36Z",
            // "RELEASE.2023-10-25T06-33-25Z",
            // "RELEASE.2023-11-01T01-57-10Z",
            // "RELEASE.2023-11-01T18-37-25Z",
            // "RELEASE.2023-11-06T22-26-08Z",
            // "RELEASE.2023-11-11T08-14-41Z",
            // "RELEASE.2023-11-15T20-43-25Z",
            // "RELEASE.2023-11-20T22-40-07Z",
            // "RELEASE.2023-12-02T10-51-33Z",
            // "RELEASE.2023-12-06T09-09-22Z",
            // "RELEASE.2023-12-07T04-16-00Z",
            // "RELEASE.2023-12-09T18-17-51Z",
            // "RELEASE.2023-12-13T23-28-55Z",
            // "RELEASE.2023-12-14T18-51-57Z",
            // "RELEASE.2023-12-20T01-00-02Z",
            // "RELEASE.2023-12-23T07-19-11Z",
            // "RELEASE.2024-01-01T16-36-33Z",
            // "RELEASE.2024-01-05T22-17-24Z",
            // "RELEASE.2024-01-11T07-46-16Z",
            // "RELEASE.2024-01-13T07-53-03Z",
            // "RELEASE.2024-01-16T16-07-38Z",
            // "RELEASE.2024-01-18T22-51-28Z",
            // "RELEASE.2024-01-28T22-35-53Z",
            // "RELEASE.2024-01-29T03-56-32Z",
            // "RELEASE.2024-01-31T20-20-33Z",
            // "RELEASE.2024-02-04T22-36-13Z",
            // "RELEASE.2024-02-06T21-36-22Z",
            // "RELEASE.2024-02-09T21-25-16Z",
            // "RELEASE.2024-02-12T21-02-27Z",
            // "RELEASE.2024-02-13T15-35-11Z",
            // "RELEASE.2024-02-14T21-36-02Z",
            // "RELEASE.2024-02-17T01-15-57Z",
            // "RELEASE.2024-02-24T17-11-14Z",
            // "RELEASE.2024-02-26T09-33-48Z",
            // "RELEASE.2024-03-03T17-50-39Z",
            // "RELEASE.2024-03-05T04-48-44Z",
            // "RELEASE.2024-03-07T00-43-48Z",
            // "RELEASE.2024-03-10T02-53-48Z",
            // "RELEASE.2024-03-15T01-07-19Z",
            // "RELEASE.2024-03-21T23-13-43Z",
            // "RELEASE.2024-03-26T22-10-45Z",
            // "RELEASE.2024-03-30T09-41-56Z",
            // "RELEASE.2024-04-06T05-26-02Z",
            // "RELEASE.2024-04-18T19-09-19Z",
            // "RELEASE.2024-04-28T17-53-50Z",
            // "RELEASE.2024-05-01T01-11-10Z",
            // "RELEASE.2024-05-07T06-41-25Z",
            // "RELEASE.2024-05-10T01-41-38Z",
            // "RELEASE.2024-05-27T19-17-46Z",
            // "RELEASE.2024-05-28T17-19-04Z",
            // "RELEASE.2024-06-04T19-20-08Z",
            // "RELEASE.2024-06-06T09-36-42Z",
            // "RELEASE.2024-06-11T03-13-30Z",
            // "RELEASE.2024-06-13T22-53-53Z",
            // "RELEASE.2024-06-22T05-26-45Z",
            // "RELEASE.2024-06-26T01-06-18Z",
            // "RELEASE.2024-06-28T09-06-49Z",
            // "RELEASE.2024-06-29T01-20-47Z",
            // "RELEASE.2024-07-04T14-25-45Z",
            // "RELEASE.2024-07-10T18-41-49Z",
            // "RELEASE.2024-07-13T01-46-15Z",
            // "RELEASE.2024-07-15T19-02-30Z",
            // "RELEASE.2024-07-16T23-46-41Z",
            // "RELEASE.2024-07-26T20-48-21Z",
            // "RELEASE.2024-07-29T22-14-52Z",
            // "RELEASE.2024-07-31T05-46-26Z",
            // "RELEASE.2024-08-03T04-33-23Z",
            // "RELEASE.2024-08-17T01-24-54Z",
            // "RELEASE.2024-08-26T15-33-07Z",
            // "RELEASE.2024-08-29T01-40-52Z",
            // "RELEASE.2024-09-09T16-59-28Z",
            // "RELEASE.2024-09-13T20-26-02Z",
            // "RELEASE.2024-09-22T00-33-43Z",
            // "RELEASE.2024-10-02T17-50-41Z",
            // "RELEASE.2024-10-13T13-34-11Z",
            // "RELEASE.2024-10-29T16-01-48Z",
            // "RELEASE.2024-11-07T00-52-20Z",
            // "RELEASE.2024-12-13T22-19-12Z"
        );
        DOCKER_BASE_IMAGE = "minio/minio:"
            + ESTestCase.randomFrom(new Random(Murmur3HashFunction.hash(System.getProperty("tests.seed"))), allReleases);
    }

    public static final String DOCKER_BASE_IMAGE;

    private final boolean enabled;
    private final String bucketName;

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
        this.bucketName = bucketName;
        if (enabled) {
            addExposedPort(servicePort);
            // The following waits for a specific log message as the readiness signal. When the minio docker image
            // gets upgraded in future, we must ensure the log message still exists or update it here accordingly.
            // Otherwise the tests using the minio fixture will fail with timeout on waiting the container to be ready.
            setWaitStrategy(Wait.forLogMessage(".*API: .*:9000.*", 1));
        }
        this.enabled = enabled;
    }

    private static final Logger logger = LogManager.getLogger(MinioTestContainer.class);

    @Override
    public void start() {
        if (enabled) {
            logger.info("using MinIO package {}", DOCKER_BASE_IMAGE);
            final var minioDataPath = System.getProperty("tests.path.miniodata");
            if (minioDataPath != null) {
                logger.info("bind-mounting minio data path at [{}]", minioDataPath);
                try {
                    Files.createDirectories(Path.of(minioDataPath).resolve(bucketName));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                addFileSystemBind(minioDataPath, "/minio/data", BindMode.READ_WRITE, SelinuxContext.NONE);
            } else {
                logger.info("not bind-mounting minio data path");
            }
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
