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
        String aggregationName = "test_cardinality_agg";
        String fieldName = "user_id";
        Long missingValue = -1L;
        long precisionThreshold = 3000L;

        FieldValue missingFieldValue = FieldValueProtoUtils.toProto(missingValue);

        CardinalityAggregation.Builder protoBuilder = CardinalityAggregation.newBuilder()
            .setName(aggregationName)
            .setField(fieldName)
            .setMissing(missingFieldValue)
            .setPrecisionThreshold((int) precisionThreshold)
            .setExecutionHint(CardinalityExecutionMode.CARDINALITY_EXECUTION_MODE_DIRECT);

        CardinalityAggregationBuilder cardinalityAgg = CardinalityAggregationBuilderProtoUtils.fromProto(protoBuilder.build());

        assertNotNull("Cardinality aggregation should not be null", cardinalityAgg);
        assertEquals("Aggregation name should match", aggregationName, cardinalityAgg.getName());
        assertEquals("Aggregation field should match", fieldName, cardinalityAgg.field());
        assertEquals("Aggregation missing field should match", cardinalityAgg.missing(), missingValue);
    }

    public void testCardinalityAggregationBuilderWithScript() throws IOException {
        String aggregationName = "test_cardinality_agg_with_script";
        String scriptSource = "doc['field1'].value + doc['field2'].value";
        String missingValue = "0";

        FieldValue missingFieldValue = FieldValueProtoUtils.toProto(missingValue);

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

        ObjectMap metadata = ObjectMap.newBuilder()
            .putFields("description", ObjectMap.Value.newBuilder().setString("Cardinality with script").build())
            .build();

        CardinalityAggregation.Builder protoBuilder = CardinalityAggregation.newBuilder()
            .setName(aggregationName)
            .setScript(script)
            .setMissing(missingFieldValue)
            .setMeta(metadata)
            .setExecutionHint(CardinalityExecutionMode.CARDINALITY_EXECUTION_MODE_GLOBAL_ORDINALS);

        CardinalityAggregationBuilder cardinalityAgg = CardinalityAggregationBuilderProtoUtils.fromProto(protoBuilder.build());

        assertNotNull("Cardinality aggregation should not be null", cardinalityAgg);
        assertEquals("Aggregation name should match", aggregationName, cardinalityAgg.getName());

        org.opensearch.script.Script actualScript = cardinalityAgg.script();
        assertNotNull("Script should not be null", actualScript);
        assertEquals("Script type should be INLINE", ScriptType.INLINE, actualScript.getType());
        assertEquals("Script source should match", scriptSource, actualScript.getIdOrCode());
        assertEquals("Script language should be painless", "painless", actualScript.getLang());

        Map<String, Object> actualMetadata = cardinalityAgg.getMetadata();
        assertNotNull("Metadata should not be null", actualMetadata);
        assertEquals("Metadata should have 1 entry", 1, actualMetadata.size());
        assertEquals("Description metadata should match", "Cardinality with script", actualMetadata.get("description"));
    }

    public void testCardinalityAggregationBuilderMinimal() throws IOException {
        String aggregationName = "minimal_cardinality_agg";

        CardinalityAggregation.Builder protoBuilder = CardinalityAggregation.newBuilder()
            .setName(aggregationName);

        CardinalityAggregationBuilder cardinalityAgg = CardinalityAggregationBuilderProtoUtils.fromProto(protoBuilder.build());

        assertNotNull("Cardinality aggregation should not be null", cardinalityAgg);
        assertEquals("Aggregation name should match", aggregationName, cardinalityAgg.getName());
    }
}
