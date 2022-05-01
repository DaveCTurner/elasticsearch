/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsResponse;
import org.elasticsearch.action.admin.cluster.settings.RestClusterGetSettingsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestClusterGetSettingsAction extends BaseRestHandler {

    private final Settings localSettings;
    private final ClusterSettings clusterSettings;
    private final SettingsFilter settingsFilter;

    public RestClusterGetSettingsAction(Settings settings, ClusterSettings clusterSettings, SettingsFilter settingsFilter) {
        this.localSettings = settings;
        this.clusterSettings = clusterSettings;
        this.settingsFilter = settingsFilter;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_cluster/settings"));
    }

    @Override
    public String getName() {
        return "cluster_get_settings_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        // ClusterGetSettingsAction was introduced in 8.3; earlier versions just used ClusterStateAction which may be very expensive. For
        // BwC we must start by checking the node versions in the (local) cluster state to see if it's safe to use the new mechanism or not.
        // Once BwC with pre-8.3 versions is no longer required we can drop the whole preflightRequest thing and just invoke
        // ClusterGetSettingsAction directly. TODO clean this up when BwC no longer needed.

        final var preflightRequest = new ClusterStateRequest().clear().nodes(true).blocks(true);
        final var renderDefaults = request.paramAsBoolean("include_defaults", false);
        final var local = request.paramAsBoolean("local", preflightRequest.local());
        final var masterNodeTimeout = request.paramAsTime("master_timeout", preflightRequest.masterNodeTimeout());
        preflightRequest.local(true).masterNodeTimeout(masterNodeTimeout);

        return channel -> client.admin().cluster().state(preflightRequest, new RestActionListener<>(channel) {
            @Override
            protected void processResponse(ClusterStateResponse preflightResponse) {
                final var finalListener = new RestToXContentListener<RestClusterGetSettingsResponse>(channel).<
                    ClusterGetSettingsResponse>map(
                        response -> response(response, renderDefaults, settingsFilter, clusterSettings, localSettings)
                    );

                final var preflightState = preflightResponse.getState();
                if (preflightState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK)
                    || preflightState.nodes().getMinNodeVersion().before(Version.V_8_3_0)) {
                    // See comment above about BwC. We must check STATE_NOT_RECOVERED_BLOCK in case we haven't even joined a cluster yet,
                    // because in that case the min node version doesn't make sense.
                    client.admin()
                        .cluster()
                        .state(
                            new ClusterStateRequest().clear().metadata(true).local(local).masterNodeTimeout(masterNodeTimeout),
                            finalListener.map(clusterStateResponse -> {
                                final var metadata = clusterStateResponse.getState().metadata();
                                return new ClusterGetSettingsResponse(
                                    metadata.persistentSettings(),
                                    metadata.transientSettings(),
                                    metadata.settings()
                                );
                            })
                        );
                } else {
                    client.execute(
                        ClusterGetSettingsAction.INSTANCE,
                        new ClusterGetSettingsRequest().local(local).masterNodeTimeout(masterNodeTimeout),
                        finalListener
                    );
                }

            }
        });
    }

    @Override
    protected Set<String> responseParams() {
        return Settings.FORMAT_PARAMS;
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

    static RestClusterGetSettingsResponse response(
        final ClusterGetSettingsResponse clusterGetSettingsResponse,
        final boolean renderDefaults,
        final SettingsFilter settingsFilter,
        final ClusterSettings clusterSettings,
        final Settings localSettings
    ) {
        return new RestClusterGetSettingsResponse(
            settingsFilter.filter(clusterGetSettingsResponse.persistentSettings()),
            settingsFilter.filter(clusterGetSettingsResponse.transientSettings()),
            renderDefaults
                ? settingsFilter.filter(clusterSettings.diff(clusterGetSettingsResponse.combinedSettings(), localSettings))
                : Settings.EMPTY
        );
    }

}
