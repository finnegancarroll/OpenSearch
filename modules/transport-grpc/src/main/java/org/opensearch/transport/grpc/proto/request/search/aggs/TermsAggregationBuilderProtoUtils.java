/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.aggs;

import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.StringMap;
import org.opensearch.protobufs.TermsInclude;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.protobufs.TermsAggregation;
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
public class TermsAggregationBuilderProtoUtils {

    /**
     * Private no-op.
     */
    private TermsAggregationBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts an org.opensearch.protobufs.TermsAggregation to an OpenSearch TermsAggregationBuilder.
     * Somewhat resembles the terms aggregation ObjectParser of
     * {@link org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder}.
     * @param termsAggregation protobuf representation of terms aggregation.
     * @return OpenSearch internal terms aggregation.
     * @throws IOException if there's an error during parsing
     */
    protected static TermsAggregationBuilder fromProto(TermsAggregation termsAggregation) throws IOException {
        TermsAggregationBuilder builder = new TermsAggregationBuilder(termsAggregation.getName());

        switch (termsAggregation.getCollectMode()) {
            case TERMS_AGGREGATION_COLLECT_MODE_UNSPECIFIED -> { }
            case TERMS_AGGREGATION_COLLECT_MODE_BREADTH_FIRST ->
                builder.collectMode(Aggregator.SubAggCollectionMode.BREADTH_FIRST);
            case TERMS_AGGREGATION_COLLECT_MODE_DEPTH_FIRST ->
                builder.collectMode(Aggregator.SubAggCollectionMode.DEPTH_FIRST);
            case UNRECOGNIZED ->
                throw new UnsupportedOperationException(
                    "TermsAggregationBuilderProtoUtils: unrecognized aggregation collect mode: " + termsAggregation.getCollectMode()
                );
        }

        IncludeExclude includeExcludeTermsOrPartitions = fromProto(
            termsAggregation.getInclude(),
            termsAggregation.getExcludeList().stream().toList()
        );
        if (includeExcludeTermsOrPartitions != null) {
            builder.includeExclude(includeExcludeTermsOrPartitions);
        }

        if (termsAggregation.hasExecutionHint()) {
            switch (termsAggregation.getExecutionHint()) {
                case TERMS_AGGREGATION_EXECUTION_HINT_UNSPECIFIED -> { }
                case TERMS_AGGREGATION_EXECUTION_HINT_GLOBAL_ORDINALS ->
                    builder.executionHint("global_ordinals");
                case TERMS_AGGREGATION_EXECUTION_HINT_MAP ->
                    builder.executionHint("map");
                case UNRECOGNIZED ->
                    throw new UnsupportedOperationException(
                        "TermsAggregationBuilderProtoUtils: unrecognized execution hint: " + termsAggregation.getExecutionHint()
                    );
            }
        }

        if (termsAggregation.hasField()) {
            builder.field(termsAggregation.getField());
        }

        if (termsAggregation.hasMinDocCount()) {
            builder.minDocCount(termsAggregation.getMinDocCount());
        }

        if (termsAggregation.hasMissing()) {
            FieldValue missingFieldValueProto = termsAggregation.getMissing();
            Object missingFieldValueObject = FieldValueProtoUtils.fromProto(missingFieldValueProto, false);
            builder.missing(missingFieldValueObject);
        }

        if (termsAggregation.hasValueType()) {
            ValueType valType = ValueType.lenientParse(termsAggregation.getValueType());
            builder.userValueTypeHint(valType);
        }

        if (termsAggregation.getOrderCount() > 0) {
            List<StringMap> orderMapList = termsAggregation.getOrderList();
            List<BucketOrder> bucketOrderList = fromProto(orderMapList);
            if (bucketOrderList.size() == 1) {
                builder.order(bucketOrderList.getFirst());
            }
        }

        if (termsAggregation.hasScript()) {
            Script script = ScriptProtoUtils.parseFromProtoRequest(termsAggregation.getScript());
            builder.script(script);
        }

        if (termsAggregation.hasShardSize()) {
            builder.shardSize(termsAggregation.getShardSize());
        }

        if (termsAggregation.getShowTermDocCountError()) {
            builder.showTermDocCountError(true);
        }

        if (termsAggregation.hasSize()) {
            builder.size(termsAggregation.getSize());
        }

        if (termsAggregation.hasFormat()) {
            builder.format(termsAggregation.getFormat());
        }

        return builder;
    }

    /**
     * Mirrors {@link org.opensearch.search.aggregations.InternalOrder.Parser}.
     * Note that each map should only contain a single key value pair.
     * Order params are strictly ordered with each subsequent order being a tiebreaker for the previous.
     * @param aggOrderMap list of order key value pairs.
     * @return list of bucket orders for aggregation result.
     */
    private static List<BucketOrder> fromProto(List<StringMap> aggOrderMap) {
        List<BucketOrder> orders = new ArrayList<>(Collections.emptyList());
        if (aggOrderMap.isEmpty()) {
           return orders;
        }

        for (StringMap orderMap : aggOrderMap) {
            Map<String, String> stringMap = orderMap.getStringMapMap();
            for (Map.Entry<String, String> entry : stringMap.entrySet()) {
                switch (entry.getKey()) {
                    case "_term":
                    case "_time":
                    case "_key":
                        if (isAsc(entry.getValue())) {
                            orders.add(KEY_ASC);
                        } else {
                            orders.add(KEY_DESC);
                        }
                    case "_count":
                        if (isAsc(entry.getValue())) {
                            orders.add(COUNT_ASC);
                        } else {
                            orders.add(COUNT_DESC);
                        }
                    default: // assume all other orders are sorting on a sub-aggregation. Validation occurs later.
                        orders.add(aggregation(entry.getKey(), isAsc(entry.getValue())));
                }
            }
        }

        return orders;
    }

    private static final String KEY_ASC_STR = "asc";
    private static final String KEY_DESC_STR = "desc";
    private static boolean isAsc(String orderValue) {
        if (KEY_ASC_STR.equalsIgnoreCase(orderValue)) {
            return true;
        } else if (KEY_DESC_STR.equalsIgnoreCase(orderValue)) {
            return false;
        } else {
            throw new UnsupportedOperationException(
                "TermsAggregationBuilderProtoUtils: unrecognized sort order: " + orderValue
            );
        }
    }

    /**
     * Terms aggregation supports two methods for specifying a subset of result buckets.
     * - Terms include exclude lists for specifying exact terms to return (with exclude given priority).
     * - Partitions where users can split the buckets into even partitions and view a selected partition.
     * @param termsInclude which stores either a list of included terms or specified partitions.
     * @param termsExclude always a list of excluded terms.
     * @return IncludeExclude protobuf encapsulates both objects or null if no include/exclude params provided.
     */
    private static IncludeExclude fromProto(TermsInclude termsInclude, List<String> termsExclude) {
        // Include provided as list of terms
        if (termsInclude.hasStringArray()) {
            String[] termsIncludeArray = termsInclude.getStringArray().getStringArrayList().toArray(new String[0]);
            String[] termsExcludeArray = termsExclude.toArray(new String[0]);
            return new IncludeExclude(
                termsIncludeArray,
                termsExcludeArray
            );
        }

        // Include provided as partitions
        if (termsInclude.hasPartition()) {
            int numPartitions = (int) termsInclude.getPartition().getNumPartitions();
            int partitions = (int) termsInclude.getPartition().getNumPartitions();
            return new IncludeExclude(
                numPartitions,
                partitions
            );
        }

        return null;
    }
}
