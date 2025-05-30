/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchbusinessrules;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

import static org.elasticsearch.xpack.searchbusinessrules.retriever.PinnedRetrieverBuilder.PINNED_RETRIEVER_FEATURE;

public class SearchBusinessRulesFeatures implements FeatureSpecification {

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of();
    }

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(PINNED_RETRIEVER_FEATURE);
    }
}
