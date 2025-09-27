/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.search.aggs;

import org.opensearch.protobufs.ObjectMap;
import org.opensearch.protobufs.TermsAggregateBaseVoidAllOfBuckets;
import org.opensearch.protobufs.UnmappedTermsAggregate;
import org.opensearch.search.aggregations.bucket.terms.InternalTerms;
import org.opensearch.transport.grpc.proto.response.common.ObjectMapProtoUtils;

import java.util.Map;

/**
 * Converter util for TermsAggregation request object.
 */
public class TermsAggregateProtoUtils {

    /**
     * Private no-op.
     */
    private TermsAggregateProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Convert an OpenSearch terms aggregation representation into a protobuf response.
     * Somewhat resembles `doXContentCommon()` of {@link org.opensearch.search.aggregations.bucket.terms.InternalTerms}.
     * @param internalTerms OpenSeach internal response.
     * @return protobuf terms aggregation response.
     */
    protected static UnmappedTermsAggregate.Builder toProto(InternalTerms internalTerms) {
        UnmappedTermsAggregate.Builder builder = UnmappedTermsAggregate.newBuilder();

        ObjectMap.Builder objectMap = ObjectMap.newBuilder();
        for (Map.Entry<String, Object> entry : internalTerms.getMetadata().entrySet()) {
            objectMap.putFields(entry.getKey(), ObjectMapProtoUtils.toProto(entry.getValue()));
        }

        TermsAggregateBaseVoidAllOfBuckets.Builder termsBuilder = TermsAggregateBaseVoidAllOfBuckets.newBuilder();
        

        builder.setDocCountErrorUpperBound(internalTerms.getDocCountError());
        builder.setSumOtherDocCount(internalTerms.getSumOfOtherDocCounts());
        return builder;
    }
}
