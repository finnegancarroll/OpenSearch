/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.aggs;

import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.MissingAggregation;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.opensearch.transport.grpc.proto.request.common.ObjectMapProtoUtils;
import org.opensearch.transport.grpc.proto.response.common.FieldValueProtoUtils;

import java.io.IOException;

/**
 * Converter util for MissingAggregation request object.
 */
public class MissingAggregationBuilderProtoUtils {

    private MissingAggregationBuilderProtoUtils() {
        // Utility class, no instances
    }

    protected static MissingAggregationBuilder fromProto(MissingAggregation missingAggregation) throws IOException {
        MissingAggregationBuilder builder = new MissingAggregationBuilder(missingAggregation.getName());

        if (missingAggregation.hasMeta()) {
            ObjectMap objMap = missingAggregation.getMeta();
            builder.setMetadata(ObjectMapProtoUtils.fromProto(objMap));
        }

        if (missingAggregation.hasField()) {
            builder.field(missingAggregation.getField());
        }

        if (missingAggregation.hasMissing()) {
            FieldValue missingFieldValueProto = missingAggregation.getMissing();
            Object missingFieldValueObject = FieldValueProtoUtils.fromProto(missingFieldValueProto, false);
            builder.missing(missingFieldValueObject);
        }

        return builder;
    }
}
