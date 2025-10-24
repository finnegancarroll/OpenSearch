/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.aggs;

import org.opensearch.protobufs.MissingAggregation;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

public class MissingAggregationBuilderProtoUtilsTests extends OpenSearchTestCase {

    public void testMissingAggregationBuilderWithBasicFields() throws IOException {
        String aggName = "miss_agg_basic";
        String fieldName = "status";

        MissingAggregation.Builder protoBuilder = MissingAggregation.newBuilder()
            .setField(fieldName);

        MissingAggregationBuilder missingAgg = MissingAggregationBuilderProtoUtils.fromProto(protoBuilder.build(), aggName);

        assertNotNull("Missing aggregation should not be null", missingAgg);
        assertEquals("Aggregation field should match", fieldName, missingAgg.field());
    }

    public void testMissingAggregationBuilderWithMetadata() throws IOException {
        String aggName = "miss_agg_basic";
        String fieldName = "status";

        ObjectMap metadata = ObjectMap.newBuilder()
            .putFields("Request origin", ObjectMap.Value.newBuilder().setString("Unit tests").build())
            .putFields("Integer field", ObjectMap.Value.newBuilder().setInt32(1234).build())
            .build();

        MissingAggregation.Builder protoBuilder = MissingAggregation.newBuilder()
            .setField(fieldName)
            .setMeta(metadata);

        MissingAggregationBuilder missingAgg = MissingAggregationBuilderProtoUtils.fromProto(protoBuilder.build(), aggName);

        assertNotNull("Missing aggregation should not be null", missingAgg);
        assertEquals("Aggregation field should match", fieldName, missingAgg.field());

        Map<String, Object> actualMetadata = missingAgg.getMetadata();
        assertNotNull("Metadata should not be null", actualMetadata);
        assertEquals("Metadata should have 3 entries", 2, actualMetadata.size());
        assertEquals("Request origin metadata should match", "Unit tests", actualMetadata.get("Request origin"));
        assertEquals("Integer field metadata should match", 1234, actualMetadata.get("Integer field"));
    }
}
