/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.extension

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.OmniCascadingHashJoinExecTransformer

import org.apache.spark.sql.catalyst.expressions.{And, Expression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec}

case class OmniCascadingJoinRule() extends Rule[SparkPlan] {

  private val minCascadeLevels: Int = 2
  private val maxRewritePasses: Int = 16

  override def apply(plan: SparkPlan): SparkPlan = {
    val columnarConf = GlutenConfig.get
    if (!columnarConf.enableOmniCascadingJoin) {
      return plan
    }

    var currentPlan = plan
    var pass = 0
    var keepGoing = true

    while (keepGoing && pass < maxRewritePasses) {
      val (nextPlan, rewriteCount) = rewriteOnce(currentPlan)
      currentPlan = nextPlan
      keepGoing = rewriteCount > 0
      pass += 1
    }

    currentPlan
  }

  private def rewriteOnce(plan: SparkPlan): (SparkPlan, Int) = {
    var rewriteCount = 0

    val rewrittenPlan =
      plan.transformDown {
        case top: BroadcastHashJoinExec =>
          val rewritten = tryRewrite(top)
          if (!(rewritten eq top)) {
            rewriteCount += 1
          }
          rewritten

        case top: ShuffledHashJoinExec =>
          val rewritten = tryRewrite(top)
          if (!(rewritten eq top)) {
            rewriteCount += 1
          }
          rewritten

        case other =>
          other
      }

    (rewrittenPlan, rewriteCount)
  }

  private def tryRewrite(top: SparkPlan): SparkPlan = {
    OmniCascadingHashJoinExecTransformer.extractJoinChain(top) match {
      case Some((probePlan, joinInfoChain))
          if joinInfoChain.length >= minCascadeLevels =>

        val cascading =
          OmniCascadingHashJoinExecTransformer(
            probePlan = probePlan,
            joinChain = joinInfoChain,
            originalOutput = top.output)

        // Native CascadingLookupJoin currently evaluates only equi/hash keys.
        // Keep every Spark HashJoin residual condition semantically intact by
        // hoisting it to one FilterExec above the fused cascading subtree.
        val residualConditions: Seq[Expression] =
          joinInfoChain.flatMap(_.condition.toSeq)

        val combinedResidual: Option[Expression] =
          residualConditions.reduceLeftOption { (left, right) => And(left, right) }

        val withResidualFilter: SparkPlan = combinedResidual match {
          case Some(condition) =>
            // The filter must run while the expanded
            // root ++ build0 ++ build1 ++ ... schema is still available.
            FilterExec(condition, cascading)
          case None =>
            cascading
        }

        // Restore Spark's original logical output only after the residual
        // filter has run. The native Cascading output is intentionally wider:
        //   root ++ build0 ++ build1 ++ ...
        if (OmniCascadingHashJoinExecTransformer.sameExprIds(
              withResidualFilter.output,
              top.output)) {
          withResidualFilter
        } else {
          ProjectExec(top.output, withResidualFilter)
        }

      case Some(_) =>
        top

      case None =>
        top
    }
  }
}
