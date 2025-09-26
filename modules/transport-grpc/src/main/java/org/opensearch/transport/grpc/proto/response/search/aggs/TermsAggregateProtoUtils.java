/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.search.aggs;

import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.MissingAggregate;
import org.opensearch.protobufs.StringMap;
import org.opensearch.protobufs.TermsAggregation;
import org.opensearch.protobufs.TermsInclude;
import org.opensearch.protobufs.UnmappedTermsAggregate;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.missing.InternalMissing;
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude;
import org.opensearch.search.aggregations.bucket.terms.InternalTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.transport.grpc.proto.request.common.ScriptProtoUtils;
import org.opensearch.transport.grpc.proto.response.common.FieldValueProtoUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opensearch.search.aggregations.BucketOrder.aggregation;
import static org.opensearch.search.aggregations.InternalOrder.COUNT_ASC;
import static org.opensearch.search.aggregations.InternalOrder.COUNT_DESC;
import static org.opensearch.search.aggregations.InternalOrder.KEY_ASC;
import static org.opensearch.search.aggregations.InternalOrder.KEY_DESC;

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

        // ...

        return builder;
    }
}
