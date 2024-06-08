/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.diagnostics;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.util.List;

public class RestDiagnosticsAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "diagnostics";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_internal/diagnostics_bundle"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return restChannel -> client.execute(
            DiagnosticsAction.INSTANCE,
            new DiagnosticsAction.Request(),
            new RestResponseListener<>(restChannel) {
                @Override
                public RestResponse buildResponse(DiagnosticsAction.Response response) {
                    return RestResponse.chunked(RestStatus.OK, response.getFirstBodyPart(), response);
                }
            }
        );
    }
}
