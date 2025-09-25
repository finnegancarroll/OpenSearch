/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.search.aggs;

import org.opensearch.core.common.text.Text;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.MissingAggregation;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.protobufs.StringArray;
import org.opensearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.transport.grpc.proto.request.common.ObjectMapProtoUtils;
import org.opensearch.transport.grpc.proto.response.common.FieldValueProtoUtils;

import java.io.IOException;

/**
 * Converter util for MissingAggregate response object.
 */
public class MissingAggregateProtoUtils {

    /**
     * Private no-op.
     */
    private MissingAggregateProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a HighlightField values (list of objects) to its Protocol Buffer representation.
     * This method is equivalent to the  {@link HighlightField#toXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param fragments The list of HighlightField values to convert
     * @return A Protobuf Value representation
     */
    protected static StringArray toProto(Text[] fragments) {
        StringArray.Builder stringArrayBuilder = StringArray.newBuilder();
        for (Text text : fragments) {
            stringArrayBuilder.addStringArray(text.string());
        }
        return stringArrayBuilder.build();
    }
}
