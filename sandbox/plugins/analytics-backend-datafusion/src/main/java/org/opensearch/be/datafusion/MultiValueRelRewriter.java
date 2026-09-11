/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file to be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

import java.util.List;

/**
 * Adds implicit per-element expansion for LIST-valued GROUP BY keys: an aggregate that groups by a
 * multi_value (LIST) column has its input wrapped in a {@link MultiValueExpandRel} so each element
 * becomes its own bucket (Lucene terms-agg parity). The expand is emitted to the shard as a Substrait
 * ExtensionSingleRel and reconstructed as a DataFusion {@code Unnest} (see substrait_consumer.rs).
 */
final class MultiValueRelRewriter {

    private MultiValueRelRewriter() {}

    static RelNode rewrite(RelNode root) {
        return root.accept(new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(LogicalProject project) {
                RelNode input = project.getInput().accept(this);
                if (input == project.getInput()) {
                    return project;
                }

                RexShuttle inputRefRetype = new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef inputRef) {
                        return new RexInputRef(
                            inputRef.getIndex(),
                            input.getRowType().getFieldList().get(inputRef.getIndex()).getType());
                    }
                };
                List<RexNode> projects =
                    project.getProjects().stream().map(expression -> expression.accept(inputRefRetype)).toList();
                return LogicalProject.create(
                    input,
                    project.getHints(),
                    projects,
                    project.getRowType().getFieldNames(),
                    project.getVariablesSet());
            }

            @Override
            public RelNode visit(RelNode other) {
                RelNode visited = super.visit(other);
                return visited instanceof Aggregate aggregate ? rewriteAggregate(aggregate) : visited;
            }
        });
    }

    private static RelNode rewriteAggregate(Aggregate aggregate) {
        RelNode input = aggregate.getInput();
        boolean groupKeysExpanded = false;
        for (int fieldIndex : aggregate.getGroupSet()) {
            if (input.getRowType().getFieldList().get(fieldIndex).getType().getComponentType() != null) {
                input = new MultiValueExpandRel(input, fieldIndex);
                groupKeysExpanded = true;
            }
        }
        if (!groupKeysExpanded) {
            return aggregate;
        }
        return aggregate.copy(
            aggregate.getTraitSet(),
            input,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            aggregate.getAggCallList());
    }
}
