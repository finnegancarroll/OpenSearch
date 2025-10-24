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
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.proto.response.common.FieldValueProtoUtils;

import java.io.IOException;
import java.util.Map;

public class MissingAggregationBuilderProtoUtilsTests extends OpenSearchTestCase {

    public void testMissingAggregationBuilderWithBasicFields() throws IOException {
        String aggregationName = "test_missing_agg";
        String fieldName = "status";
        String missingValue = "unknown";

        FieldValue missingFieldValue = FieldValueProtoUtils.toProto(missingValue);

        MissingAggregation.Builder protoBuilder = MissingAggregation.newBuilder()
            .setName(aggregationName)
            .setField(fieldName)
            .setMissing(missingFieldValue);

        MissingAggregationBuilder missingAgg = MissingAggregationBuilderProtoUtils.fromProto(protoBuilder.build());

        assertNotNull("Missing aggregation should not be null", missingAgg);
        assertEquals("Aggregation name should match", aggregationName, missingAgg.getName());
        assertEquals("Aggregation field should match", fieldName, missingAgg.field());
    }

    public void testMissingAggregationBuilderWithMetadata() throws IOException {
        String aggregationName = "test_missing_agg_with_meta";
        String fieldName = "category";
        Integer missingValue = -1;

        FieldValue missingFieldValue = FieldValueProtoUtils.toProto(missingValue);

        ObjectMap metadata = ObjectMap.newBuilder()
            .putFields("description", ObjectMap.Value.newBuilder().setString("Test missing aggregation").build())
            .putFields("version", ObjectMap.Value.newBuilder().setInt32(1).build())
            .build();

        MissingAggregation.Builder protoBuilder = MissingAggregation.newBuilder()
            .setName(aggregationName)
            .setField(fieldName)
            .setMissing(missingFieldValue)
            .setMeta(metadata);

        MissingAggregationBuilder missingAgg = MissingAggregationBuilderProtoUtils.fromProto(protoBuilder.build());

        assertNotNull("Missing aggregation should not be null", missingAgg);
        assertEquals("Aggregation name should match", aggregationName, missingAgg.getName());
        assertEquals("Aggregation field should match", fieldName, missingAgg.field());

        Map<String, Object> actualMetadata = missingAgg.getMetadata();
        assertNotNull("Metadata should not be null", actualMetadata);
        assertEquals("Metadata should have 2 entries", 2, actualMetadata.size());
        assertEquals("Description metadata should match", "Test missing aggregation", actualMetadata.get("description"));
        assertEquals("Version metadata should match", 1, actualMetadata.get("version"));
    }

    public void testMissingAggregationBuilderMinimal() throws IOException {
        String aggregationName = "minimal_missing_agg";

        MissingAggregation.Builder protoBuilder = MissingAggregation.newBuilder()
            .setName(aggregationName);

        MissingAggregationBuilder missingAgg = MissingAggregationBuilderProtoUtils.fromProto(protoBuilder.build());

        assertNotNull("Missing aggregation should not be null", missingAgg);
        assertEquals("Aggregation name should match", aggregationName, missingAgg.getName());
    }
}
