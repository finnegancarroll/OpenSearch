/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.search.aggs;

import org.opensearch.protobufs.CardinalityAggregation;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.transport.grpc.proto.request.common.ScriptProtoUtils;
import org.opensearch.transport.grpc.proto.response.common.FieldValueProtoUtils;

import java.io.IOException;

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


}
