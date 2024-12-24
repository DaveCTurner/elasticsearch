/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.discovery.ec2;

import fixture.aws.ec2.AwsEc2HttpFixture;
import fixture.aws.imds.Ec2ImdsHttpFixture;
import fixture.aws.imds.Ec2ImdsServiceBuilder;
import fixture.aws.imds.Ec2ImdsVersion;
import fixture.s3.DynamicS3Credentials;

import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.hasItem;

public class DiscoveryEc2ClusterFormationIT extends ESRestTestCase {

    private static final Logger logger = LogManager.getLogger(DiscoveryEc2ClusterFormationIT.class);

    private static final DynamicS3Credentials dynamicCredentials = new DynamicS3Credentials();

    private static final Ec2ImdsHttpFixture ec2ImdsHttpFixture = new Ec2ImdsHttpFixture(
        new Ec2ImdsServiceBuilder(Ec2ImdsVersion.V2).instanceIdentityDocument(
            (builder, params) -> builder.field("region", randomIdentifier())
        ).newCredentialsConsumer(dynamicCredentials::addValidCredentials)
    );

    private static final AwsEc2HttpFixture ec2ApiFixture = new AwsEc2HttpFixture(
        dynamicCredentials::isAuthorized,
        DiscoveryEc2ClusterFormationIT::getTransportAddresses
    );

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(2)
        .plugin("discovery-ec2")
        .setting(DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING.getKey(), Ec2DiscoveryPlugin.EC2_SEED_HOSTS_PROVIDER_NAME)
        .setting("logger." + AwsEc2SeedHostsProvider.class.getCanonicalName(), "DEBUG")
        .setting(Ec2ClientSettings.ENDPOINT_SETTING.getKey(), ec2ApiFixture::getAddress)
        .systemProperty(Ec2ImdsHttpFixture.ENDPOINT_OVERRIDE_SYSPROP_NAME_SDK2, ec2ImdsHttpFixture::getAddress)
        .build();

    private static List<String> getTransportAddresses() {
        return cluster.getAvailableTransportEndpoints();
    }

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(ec2ImdsHttpFixture).around(ec2ApiFixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testClusterFormation() throws IOException {

        final var expectedAddresses = new HashSet<>(getTransportAddresses());
        final var addressesPattern = Pattern.compile(".* using dynamic transport addresses \\[(.*)]");

        for (int nodeIndex = 0; nodeIndex < cluster.getNumNodes(); nodeIndex++) {
            try (
                var logStream = cluster.getNodeLog(nodeIndex, LogType.SERVER);
                var logReader = new InputStreamReader(logStream, StandardCharsets.UTF_8);
                var bufReader = new BufferedReader(logReader)
            ) {
                do {
                    final var line = bufReader.readLine();
                    if (line == null) {
                        break;
                    }

                    final var matcher = addressesPattern.matcher(line);
                    if (matcher.matches()) {
                        for (final var address : matcher.group(1).split(", ")) {
                            // TODO NOMERGE also add some nodes to the DescribeInstances output which are filtered out, and verify that
                            // we do not see their addresses here
                            assertThat(expectedAddresses, hasItem(address));
                        }
                    }
                } while (true);
            }
        }
    }
}
