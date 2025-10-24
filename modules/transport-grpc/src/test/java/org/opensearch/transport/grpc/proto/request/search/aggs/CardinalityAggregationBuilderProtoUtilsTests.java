/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.aggs;

import org.opensearch.protobufs.CardinalityAggregation;
import org.opensearch.protobufs.CardinalityExecutionMode;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.InlineScript;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.protobufs.Script;
import org.opensearch.protobufs.ScriptLanguage;
import org.opensearch.script.ScriptType;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.proto.response.common.FieldValueProtoUtils;

import java.io.IOException;
import java.util.Map;

public class CardinalityAggregationBuilderProtoUtilsTests extends OpenSearchTestCase {

    public void testCardinalityAggregationBuilderWithBasicFields() throws IOException {
        String aggName = "card_agg_basic";
        String fieldName = "user_id";
        Boolean missingField = true;
        long precisionThreshold = 3000L;

        FieldValue missingFieldValue = FieldValueProtoUtils.toProto(missingField);

        CardinalityAggregation.Builder protoBuilder = CardinalityAggregation.newBuilder()
            .setField(fieldName)
            .setMissing(missingFieldValue)
            .setPrecisionThreshold((int) precisionThreshold)
            .setExecutionHint(CardinalityExecutionMode.CARDINALITY_EXECUTION_MODE_DIRECT);

        CardinalityAggregationBuilder cardinalityAgg = CardinalityAggregationBuilderProtoUtils.fromProto(protoBuilder.build(), aggName);

        assertNotNull("Cardinality aggregation should not be null", cardinalityAgg);
        assertEquals("Aggregation name should match", "card_agg_basic", cardinalityAgg.getName());
        assertEquals("Aggregation field should match", fieldName, cardinalityAgg.field());
        assertEquals("Aggregation missing field should match", cardinalityAgg.missing(), true);
    }

    public void testCardinalityAggregationBuilderWithScript() throws IOException {
        String aggName = "card_agg_with_script";
        String scriptSource = "doc['field1'].value + doc['field2'].value";
        String missingField = "this string is arbitrary for this agg type";

        FieldValue missingFieldValue = FieldValueProtoUtils.toProto(missingField);

        Script script = Script.newBuilder()
            .setInline(
                InlineScript.newBuilder()
                    .setSource(scriptSource)
                    .setLang(
                        ScriptLanguage.newBuilder()
                            .setBuiltin(org.opensearch.protobufs.BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS)
                    )
                    .build()
            )
            .build();

        CardinalityAggregation.Builder protoBuilder = CardinalityAggregation.newBuilder()
            .setScript(script)
            .setMissing(missingFieldValue)
            .setExecutionHint(CardinalityExecutionMode.CARDINALITY_EXECUTION_MODE_GLOBAL_ORDINALS);

        CardinalityAggregationBuilder cardinalityAgg = CardinalityAggregationBuilderProtoUtils.fromProto(protoBuilder.build(), aggName);

        assertNotNull("Cardinality aggregation should not be null", cardinalityAgg);
        assertEquals("Aggregation name should match", "card_agg_with_script", cardinalityAgg.getName());
        assertEquals("Aggregation missing field should match", cardinalityAgg.missing(), "this string is arbitrary for this agg type");

        org.opensearch.script.Script actualScript = cardinalityAgg.script();
        assertNotNull("Script should not be null", actualScript);
        assertEquals("Script type should be INLINE", ScriptType.INLINE, actualScript.getType());
        assertEquals("Script source should match", scriptSource, actualScript.getIdOrCode());
        assertEquals("Script language should be painless", "painless", actualScript.getLang());
    }

    public void testCardinalityAggregationBuilderWithMetadata() throws IOException {
        String aggName = "card_agg_metadata";
        String fieldName = "missing_field_name";
        long precisionThreshold = 123;

        ObjectMap metadata = ObjectMap.newBuilder()
            .putFields("description", ObjectMap.Value.newBuilder().setString("Unit test metadata").build())
            .build();

        CardinalityAggregation.Builder protoBuilder = CardinalityAggregation.newBuilder()
            .setField(fieldName)
            .setMeta(metadata)
            .setPrecisionThreshold((int) precisionThreshold)
            .setExecutionHint(CardinalityExecutionMode.CARDINALITY_EXECUTION_MODE_GLOBAL_ORDINALS);

        CardinalityAggregationBuilder cardinalityAgg = CardinalityAggregationBuilderProtoUtils.fromProto(protoBuilder.build(), aggName);

        assertNotNull("Cardinality aggregation should not be null", cardinalityAgg);
        assertEquals("Aggregation name should match", "card_agg_metadata", cardinalityAgg.getName());
        assertEquals("Aggregation field should match", fieldName, cardinalityAgg.field());

        Map<String, Object> actualMetadata = cardinalityAgg.getMetadata();
        assertNotNull("Metadata should not be null", actualMetadata);
        assertEquals("Metadata should have 1 entry", 1, actualMetadata.size());
        assertEquals("Description metadata should match", "Unit test metadata", actualMetadata.get("description"));
    }

    // TODO: Update the proto such that this test fails - Cardinality agg "field" should be required.
    public void testCardinalityAggregationBuilderMinimal() throws IOException {
        String aggName = "card_agg_min";

        CardinalityAggregation.Builder protoBuilder = CardinalityAggregation.newBuilder()
            .setExecutionHint(CardinalityExecutionMode.CARDINALITY_EXECUTION_MODE_UNSPECIFIED);

        CardinalityAggregationBuilder cardinalityAgg = CardinalityAggregationBuilderProtoUtils.fromProto(protoBuilder.build(), aggName);

        assertNotNull("Cardinality aggregation should not be null", cardinalityAgg);
        assertEquals("Aggregation name should match", "card_agg_min", cardinalityAgg.getName());
    }
}
