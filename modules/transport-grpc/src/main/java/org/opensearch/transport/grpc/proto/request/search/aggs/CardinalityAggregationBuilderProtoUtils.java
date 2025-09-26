/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.aggs;

import org.opensearch.protobufs.CardinalityAggregation;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.transport.grpc.proto.request.common.ScriptProtoUtils;
import org.opensearch.transport.grpc.proto.response.common.FieldValueProtoUtils;

import java.io.IOException;

/**
 * Converter utility for CardinalityAggregation protobuf request object.
 */
public class CardinalityAggregationBuilderProtoUtils {

    /**
     * Private no-op.
     */
    private CardinalityAggregationBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts an org.opensearch.protobufs.CardinalityAggregation to an OpenSearch CardinalityAggregationBuilder.
     * Somewhat resembles the cardinality aggregation ObjectParser of
     * {@link org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder}.
     * @param cardinalityAggregation protobuf representation of cardinality aggregation.
     * @return OpenSearch internal cardinality aggregation.
     * @throws IOException if there's an error during parsing
     */
    protected static CardinalityAggregationBuilder fromProto(CardinalityAggregation cardinalityAggregation) throws IOException {
        // TODO: Replace temp name with actual when api spec is updated
        CardinalityAggregationBuilder builder = new CardinalityAggregationBuilder("_name");

        // TODO: Cardinality agg missing object metadata in api spec
//        if (cardinalityAggregation.hasMeta()) {
//            ObjectMap objMap = cardinalityAggregation.getMeta();
//            builder.setMetadata(ObjectMapProtoUtils.fromProto(objMap));
//        }

        if (cardinalityAggregation.hasField()) {
            builder.field(cardinalityAggregation.getField());
        }

        if (cardinalityAggregation.hasMissing()) {
            FieldValue missingFieldValueProto = cardinalityAggregation.getMissing();
            Object missingFieldValueObject = FieldValueProtoUtils.fromProto(missingFieldValueProto, false);
            builder.missing(missingFieldValueObject);
        }

        if (cardinalityAggregation.hasScript()) {
            Script script = ScriptProtoUtils.parseFromProtoRequest(cardinalityAggregation.getScript());
            builder.script(script);
        }

        if (cardinalityAggregation.hasPrecisionThreshold()) {
            builder.precisionThreshold(cardinalityAggregation.getPrecisionThreshold());
        }

        if (cardinalityAggregation.hasExecutionHint()) {
            switch (cardinalityAggregation.getExecutionHint()) {
                case CARDINALITY_EXECUTION_MODE_UNSPECIFIED -> {}
                case CARDINALITY_EXECUTION_MODE_DIRECT ->
                    builder.executionHint("direct");
                case CARDINALITY_EXECUTION_MODE_GLOBAL_ORDINALS ->
                    builder.executionHint("global");
                case UNRECOGNIZED ->
                    throw new UnsupportedOperationException(
                        "CardinalityAggregationBuilderProtoUtils: unrecognized execution hint: " + cardinalityAggregation.getExecutionHint()
                    );
            }
        }

        return builder;
    }
}
