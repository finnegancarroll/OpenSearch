/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file to be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;

/**
 * Backend-local relation that row-expands a LIST input column into its scalar element, REPLACING
 * the column in-place (same ordinal, same name). This mirrors the semantics of an explicit
 * {@code mvexpand} (Correlate+Uncollect): one output row per element, duplicates preserved (no
 * distinct dedup). Used for implicit multi-value GROUP BY (e.g. {@code stats count() by tags}),
 * so the downstream aggregate references the same column position with a scalar element type and
 * no stale LIST-typed ancestor reference is left behind.
 */
final class MultiValueExpandRel extends SingleRel {

    private final int fieldIndex;

    MultiValueExpandRel(RelNode input, int fieldIndex) {
        super(input.getCluster(), input.getTraitSet(), input);
        this.fieldIndex = fieldIndex;
        if (input.getRowType().getFieldList().get(fieldIndex).getType().getComponentType() == null) {
            throw new IllegalArgumentException("field " + fieldIndex + " is not a collection");
        }
    }

    int fieldIndex() {
        return fieldIndex;
    }

    @Override
    protected RelDataType deriveRowType() {
        RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
        List<RelDataTypeField> fields = getInput().getRowType().getFieldList();
        for (int i = 0; i < fields.size(); i++) {
            RelDataTypeField field = fields.get(i);
            if (i == fieldIndex) {
                RelDataType elementType = field.getType().getComponentType();
                builder.add(field.getName(), getCluster().getTypeFactory().createTypeWithNullability(elementType, true));
            } else {
                builder.add(field.getName(), field.getType());
            }
        }
        return builder.build();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MultiValueExpandRel(sole(inputs), fieldIndex);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("field", getInput().getRowType().getFieldNames().get(fieldIndex))
            .item("mode", "replace")
            .item("distinct", false);
    }
}
