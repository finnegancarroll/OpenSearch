/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.aggs;

import org.opensearch.protobufs.AggregationContainer;
import org.opensearch.search.aggregations.AggregationBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * Handle an aggregation container which wraps an aggregation type.
 */
public class AggregationContainerBuilderProtoUtils {

    /**
     * Private no-op.
     */
    private AggregationContainerBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Maps aggregation types wrapped by AggregationContainer to the appropriate fromProto conversion.
     * @throws IOException if there's an error during parsing
     */
    public static AggregationBuilder fromProto(Map.Entry<String, AggregationContainer> aggEntry) throws IOException {
        String aggregationName = aggEntry.getKey();
        AggregationContainer aggregationContainer = aggEntry.getValue();
        switch (aggregationContainer.getAggregationCase()) {
            case FILTER -> throw new IllegalArgumentException("Top level aggregation is a filter clause. Use the 'query' field to filter all results.");
            case CARDINALITY -> {
                return CardinalityAggregationBuilderProtoUtils.fromProto(aggregationContainer.getCardinality(), aggregationName);
            }
            case MISSING -> {
                return MissingAggregationBuilderProtoUtils.fromProto(aggregationContainer.getMissing(), aggregationName);
            }
            case AGGREGATION_NOT_SET -> {
                return null;
            }
        }
        return null;
    }
}
