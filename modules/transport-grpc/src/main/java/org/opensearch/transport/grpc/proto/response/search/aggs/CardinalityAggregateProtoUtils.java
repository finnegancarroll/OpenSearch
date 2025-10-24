/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.search.aggs;

import org.opensearch.protobufs.CardinalityAggregate;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.search.aggregations.metrics.InternalCardinality;
import org.opensearch.transport.grpc.proto.response.common.ObjectMapProtoUtils;

import java.util.Map;

/**
 * Converter util for CardinalityAggregation request object.
 */
public class CardinalityAggregateProtoUtils {

    /**
     * Private no-op.
     */
    private CardinalityAggregateProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Convert an OpenSearch cardinality aggregation representation into a protobuf response.
     * Somewhat resembles `doXContentBody()` of {@link org.opensearch.search.aggregations.metrics.InternalCardinality}.
     * @param internalCardinality OpenSeach internal response.
     * @return protobuf cardinality aggregation response.
     */
    protected static CardinalityAggregate.Builder toProto(InternalCardinality internalCardinality) {
        CardinalityAggregate.Builder builder = CardinalityAggregate.newBuilder();

        ObjectMap.Builder objectMap = ObjectMap.newBuilder();
        for (Map.Entry<String, Object> entry : internalCardinality.getMetadata().entrySet()) {
            objectMap.putFields(entry.getKey(), ObjectMapProtoUtils.toProto(entry.getValue()));
        }

        builder.setValue(internalCardinality.getValue());
        return builder;
    }
}
