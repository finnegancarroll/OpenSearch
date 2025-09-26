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
import org.opensearch.protobufs.MissingAggregate;
import org.opensearch.protobufs.MissingAggregation;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.protobufs.StringArray;
import org.opensearch.search.aggregations.bucket.missing.InternalMissing;
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
     * Convert an OpenSearch missing aggregation representation into a protobuf response.
     * Somewhat resembles `doXContentBody()` of {@link org.opensearch.search.aggregations.bucket.InternalSingleBucketAggregation}.
     * @param internalMissing OpenSeach internal response.
     * @return protobuf missinge aggregation response.
     */
    protected static MissingAggregate.Builder toProto(InternalMissing internalMissing) {
        MissingAggregate.Builder builder = MissingAggregate.newBuilder();

        // ...

        return builder;
    }
}
