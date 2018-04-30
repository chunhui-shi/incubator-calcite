/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.BitSet;

public class ProjectCorrelateTransposeRule  extends RelOptRule {

  /**
   * preserveExprCondition to define the condition for a expression not to be pushed
   */
  private final PushProjector.ExprCondition preserveExprCondition;

  public static final ProjectCorrelateTransposeRule INSTANCE =
      new ProjectCorrelateTransposeRule(
          PushProjector.ExprCondition.TRUE,
          RelFactories.LOGICAL_BUILDER);

  public ProjectCorrelateTransposeRule(
      PushProjector.ExprCondition preserveExprCondition,
      RelBuilderFactory relFactory) {
    super(
        operand(Project.class,
            operand(Correlate.class, any())),
        relFactory, null);
    this.preserveExprCondition = preserveExprCondition;
  }

  public void onMatch(RelOptRuleCall call) {
    Project origProj = call.rel(0);
    final Correlate corr = call.rel(1);

    // locate all fields referenced in the projection and join condition;
    // determine which inputs are referenced in the projection and
    // join condition; if all fields are being referenced and there are no
    // special expressions, no point in proceeding any further
    PushProjector pushProject =
        new PushProjector(
            origProj,
            call.builder().literal(true),
            corr,
            preserveExprCondition,
            call.builder());
    if (pushProject.locateAllRefs()) {
      return;
    }

    // create left and right projections, projecting only those
    // fields referenced on each side
    RelNode leftProjRel =
        pushProject.createProjectRefsAndExprs(
            corr.getLeft(),
            true,
            false);
    RelNode rightProjRel =
        pushProject.createProjectRefsAndExprs(
            corr.getRight(),
            true,
            true);

    // convert the join condition to reference the projected columns
    int[] adjustments = pushProject.getAdjustments();

    // adjust RequiredColumns
    BitSet updatedBits = new BitSet();

    for (Integer col : corr.getRequiredColumns()) {
      int newCol = col + adjustments[col];
      updatedBits.set(newCol);
    }
    // create a new join with the projected children
    Correlate newCorrRel =
        corr.copy(
            corr.getTraitSet(),
            leftProjRel,
            rightProjRel,
            corr.getCorrelationId(),
            ImmutableBitSet.of(BitSets.toIter(updatedBits)),
            corr.getJoinType());

    // put the original project on top of the join, converting it to
    // reference the modified projection list
    RelNode topProject =
        pushProject.createNewProject(newCorrRel, adjustments);

    call.transformTo(topProject);

  }

}
