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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

import java.util.ArrayList;
import java.util.List;

/**
 * Adds implicit element expansion for LIST-valued GROUP BY keys.
 *
 * <p>This rewriter appends a scalar unnested column per LIST GROUP BY key and remaps
 * the grouping set indices so that Substrait serialisation sees scalar types throughout.
 * It currently runs at Substrait-conversion time (inside
 * {@code DataFusionFragmentConvertor.preprocessForSubstrait}), which is <em>after</em>
 * {@code PlannerImpl.decomposeAggregates} has already split the aggregate into
 * PARTIAL/FINAL halves.  In a multi-shard query the FINAL fragment therefore receives
 * a {@code List(Utf8)} GROUP BY key where Calcite expects the element type
 * ({@code Utf8View}), causing a type mismatch 500.
 *
 * <p><b>Known issue</b>: the correct fix is to move this expansion into
 * {@code PlannerImpl.runAllOptimizations} <em>before</em> the
 * {@code decomposeAggregates} call so that Calcite propagates element types into both
 * the PARTIAL and FINAL fragments.  That requires relocating
 * {@link MultiValueExpandRel} into a module that {@code analytics-engine} can depend
 * on (the dependency currently flows the other way: {@code analytics-backend-datafusion}
 * extends {@code analytics-engine}).  This is tracked for a follow-up PR.
 */
final class MultiValueRelRewriter {

    private MultiValueRelRewriter() {}

    static RelNode rewrite(RelNode root) {
        return root.accept(new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                // Rewrite children first, then rebuild this node against the (possibly rewritten)
                // children. We can't rely on the default copy: the implicit GROUP BY expansion
                // changes a LIST group key to its scalar element type, so a parent Project/Filter/
                // Sort built against the pre-expansion LIST type would fail Calcite's RexChecker
                // ("type mismatch: ref VARCHAR ARRAY, input VARCHAR") the moment its copy is
                // validated. Retyping the input refs to the child's current field types keeps the
                // plan consistent all the way to the root.
                List<RelNode> newInputs = new ArrayList<>(other.getInputs().size());
                boolean inputChanged = false;
                for (RelNode input : other.getInputs()) {
                    RelNode rewritten = input.accept(this);
                    newInputs.add(rewritten);
                    inputChanged |= rewritten != input;
                }
                RelNode current = inputChanged ? retypeInputRefs(other, newInputs) : other;
                return current instanceof Aggregate aggregate ? rewriteAggregate(aggregate) : current;
            }
        });
    }

    /**
     * Rebuilds {@code node} on top of {@code newInputs}, rewriting every {@link RexInputRef} so its
     * declared type matches the corresponding field of the (possibly retyped) new input. This lets a
     * LIST-to-scalar group-key expansion propagate up through ancestor Project/Filter/Sort nodes
     * without tripping Calcite's type-consistency checker.
     *
     * <p>{@link Project} is handled explicitly: its {@code copy} validates the (stale-typed)
     * projection expressions against the new input during construction, so we retype the projection
     * list first and build the Project from the corrected expressions rather than calling the
     * validating copy on stale refs.
     */
    private static RelNode retypeInputRefs(RelNode node, List<RelNode> newInputs) {
        final List<RelDataTypeField> inputFields = new ArrayList<>();
        for (RelNode in : newInputs) {
            inputFields.addAll(in.getRowType().getFieldList());
        }
        RexShuttle refRetyper = new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                int index = ref.getIndex();
                if (index < inputFields.size()) {
                    RelDataType actual = inputFields.get(index).getType();
                    if (!actual.equals(ref.getType())) {
                        return new RexInputRef(index, actual);
                    }
                }
                return ref;
            }
        };
        if (node instanceof Project project) {
            List<RexNode> newProjects = refRetyper.visitList(project.getProjects());
            // Preserve the original output field names; the retyped element type flows into the
            // Project's derived row type automatically.
            return LogicalProject.create(
                newInputs.get(0),
                project.getHints(),
                newProjects,
                project.getRowType().getFieldNames()
            );
        }
        // For non-Project nodes (Filter/Sort/…) the default copy does not eagerly validate stale
        // input-ref types against the new input, so copy-then-retype is safe.
        return node.copy(node.getTraitSet(), newInputs).accept(refRetyper);
    }

    private static RelNode rewriteAggregate(Aggregate aggregate) {
        RelNode input = aggregate.getInput();
        boolean changed = false;
        // Replace each LIST GROUP BY key in-place with its row-expanded scalar element, mirroring
        // explicit mvexpand (one row per element, duplicates preserved). Because the expansion keeps
        // the column at the SAME ordinal with the element type, the aggregate groups by the same
        // index and no group-set remap or output-restoring Project is needed -- so no stale
        // LIST-typed ancestor reference is created.
        for (int fieldIndex : aggregate.getGroupSet()) {
            if (input.getRowType().getFieldList().get(fieldIndex).getType().getComponentType() != null) {
                input = new MultiValueExpandRel(input, fieldIndex);
                changed = true;
            }
        }
        if (!changed) {
            return aggregate;
        }
        return aggregate.copy(
            aggregate.getTraitSet(),
            input,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            aggregate.getAggCallList()
        );
    }

}
