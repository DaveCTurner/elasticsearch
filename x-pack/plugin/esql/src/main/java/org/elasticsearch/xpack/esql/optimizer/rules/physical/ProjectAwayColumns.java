/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.Collections.singletonList;

/**
 * Adds an explicit project to minimize the amount of attributes sent from the local plan to the coordinator.
 * This is done here to localize the project close to the data source and simplify the upcoming field
 * extraction.
 */
public class ProjectAwayColumns extends Rule<PhysicalPlan, PhysicalPlan> {
    public static String ALL_FIELDS_PROJECTED = "<all-fields-projected>";

    @Override
    public PhysicalPlan apply(PhysicalPlan plan) {
        Holder<Boolean> keepTraversing = new Holder<>(TRUE);
        // Invariant: if we add a projection with these attributes after the current plan node, the plan remains valid
        // and the overall output will not change.
        AttributeSet.Builder requiredAttrBuilder = plan.outputSet().asBuilder();

        return plan.transformDown(currentPlanNode -> {
            if (keepTraversing.get() == false) {
                return currentPlanNode;
            }

            // for non-unary execution plans, we apply the rule for each child
            if (currentPlanNode instanceof MergeExec mergeExec) {
                keepTraversing.set(FALSE);
                List<PhysicalPlan> newChildren = new ArrayList<>();
                boolean changed = false;

                for (var child : mergeExec.children()) {
                    var newChild = apply(child);

                    if (newChild != child) {
                        changed = true;
                    }

                    newChildren.add(newChild);
                }
                return changed ? new MergeExec(mergeExec.source(), newChildren, mergeExec.output()) : mergeExec;
            }

            if (currentPlanNode instanceof ExchangeExec exec) {
                keepTraversing.set(FALSE);
                var child = exec.child();
                // otherwise expect a Fragment
                if (child instanceof FragmentExec fragmentExec) {
                    var logicalFragment = fragmentExec.fragment();

                    // no need for projection when dealing with aggs
                    if (logicalFragment instanceof Aggregate == false) {
                        // we should respect the order of the attributes
                        List<Attribute> output = new ArrayList<>();
                        for (Attribute attribute : logicalFragment.output()) {
                            if (requiredAttrBuilder.contains(attribute)) {
                                output.add(attribute);
                                requiredAttrBuilder.remove(attribute);
                            }
                        }
                        // requiredAttrBuilder should be empty unless the plan is inconsistent due to a bug.
                        // This can happen in case of remote ENRICH, see https://github.com/elastic/elasticsearch/issues/118531
                        // TODO: stop adding the remaining required attributes once remote ENRICH is fixed.
                        output.addAll(requiredAttrBuilder.build());

                        // if all the fields are filtered out, it's only the count that matters
                        // however until a proper fix (see https://github.com/elastic/elasticsearch/issues/98703)
                        // add a synthetic field (so it doesn't clash with the user defined one) to return a constant
                        // to avoid the block from being trimmed
                        if (output.isEmpty()) {
                            var alias = new Alias(logicalFragment.source(), ALL_FIELDS_PROJECTED, Literal.NULL, null, true);
                            List<Alias> fields = singletonList(alias);
                            logicalFragment = new Eval(logicalFragment.source(), logicalFragment, fields);
                            output = Expressions.asAttributes(fields);
                        }
                        // add a logical projection (let the local replanning remove it if needed)
                        FragmentExec newChild = new FragmentExec(
                            Source.EMPTY,
                            new Project(logicalFragment.source(), logicalFragment, output),
                            fragmentExec.esFilter(),
                            fragmentExec.estimatedRowSize()
                        );
                        return new ExchangeExec(exec.source(), output, exec.inBetweenAggs(), newChild);
                    }
                }
            } else {
                AttributeSet.Builder addedAttrBuilder = currentPlanNode.outputSet().asBuilder();
                addedAttrBuilder.removeIf(currentPlanNode.inputSet()::contains);
                requiredAttrBuilder.removeIf(addedAttrBuilder::contains);
                requiredAttrBuilder.addAll(currentPlanNode.references());
            }
            return currentPlanNode;
        });
    }
}
