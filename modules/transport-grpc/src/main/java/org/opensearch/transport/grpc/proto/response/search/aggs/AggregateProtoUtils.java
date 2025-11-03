/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.search.aggs;

import org.opensearch.protobufs.Aggregate;
import org.opensearch.protobufs.AggregationContainer;
import org.opensearch.protobufs.CardinalityAggregate;
import org.opensearch.protobufs.MissingAggregate;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.missing.InternalMissing;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalCardinality;
import org.opensearch.transport.grpc.proto.request.search.aggs.CardinalityAggregationBuilderProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.aggs.MissingAggregationBuilderProtoUtils;

import java.io.IOException;
import java.util.Map;

/**
 * Handle an aggregate response.
 */
public class AggregateProtoUtils {

    /**
     * Private no-op.
     */
    private AggregateProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Maps aggregate response types to appropriate conversion util.
     * @param name user provided aggregation key in search request/response.
     * @param aggregation aggregation response.
     * @throws IOException for parsing errors and unsupported aggregate types.
     */
    public static void toProto(org.opensearch.protobufs.SearchResponse.Builder builder, String name, Aggregation aggregation) throws IOException {
        /*
        Aggregation child type can only be determined by `instanceof` with downcast.
        Required to select the correct proto conversion util, which are strongly typed.
         */
        if (aggregation instanceof InternalCardinality) {
            CardinalityAggregate.Builder cardBuilder = CardinalityAggregateProtoUtils.toProto((InternalCardinality)aggregation);
            Aggregate protoAggregate = Aggregate.newBuilder().setCardinality(cardBuilder.build()).build();
            builder.putAggregations(name, protoAggregate);
        } else if (aggregation instanceof InternalMissing) {
            MissingAggregate.Builder missingsBuilder = MissingAggregateProtoUtils.toProto((InternalMissing)aggregation);
            Aggregate protoAggregate = Aggregate.newBuilder().setMissing(missingsBuilder).build();
            builder.putAggregations(name, protoAggregate);
        } else {
            throw new UnsupportedOperationException("Unsupported aggregation type: " + aggregation.getClass().getName());
        }
    }
}
