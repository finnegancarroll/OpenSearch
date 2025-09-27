/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.search.aggs;

import org.opensearch.protobufs.MissingAggregate;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.search.aggregations.bucket.missing.InternalMissing;

import org.opensearch.transport.grpc.proto.response.common.ObjectMapProtoUtils;

import java.util.Map;

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

        ObjectMap.Builder objectMap = ObjectMap.newBuilder();
        for (Map.Entry<String, Object> entry : internalMissing.getMetadata().entrySet()) {
            objectMap.putFields(entry.getKey(), ObjectMapProtoUtils.toProto(entry.getValue()));
        }

        // TODO: Handle sub aggregations...

        builder.setDocCount(internalMissing.getDocCount());
        return builder;
    }
}
