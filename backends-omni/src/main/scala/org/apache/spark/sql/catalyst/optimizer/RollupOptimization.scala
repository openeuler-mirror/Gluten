package org.apache.spark.sql.catalyst.optimizer

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{ExpandExecTransformer, OmniHashAggregateExecTransformer, OmniRollUpOptimizeTransformer}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

import scala.math.pow

case class RollupOptimization(session: SparkSession) extends Rule[SparkPlan] {
  def matchRollupOptimization(expandExec: ExpandExecTransformer): Boolean = {
    // Expand operator contains "count(distinct)", "rollup", "cube", "grouping sets",
    // it checks whether match "rollup" operations and part "grouping sets" operation.
    // For example, grouping columns a and b, such as rollup(a, b), grouping sets((a, b), (a)).
    if (expandExec.projections.length == 1) {
      return false
    }
    var step = 0
    expandExec.projections.foreach(
      projection => {
        projection.last match {
          case literal: Literal =>
            if (literal.value != (pow(2, step) - 1)) {
              return false
            }
          case _ =>
            return false
        }
        step += 1
      })
    true
  }

  def replaceWithColumnarPlan(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case p @ OmniHashAggregateExecTransformer(_, _, _, _, _, _, ExpandExecTransformer(_, _, _)) =>
        val expand = p.child.asInstanceOf[ExpandExecTransformer]
        val isMatchRollupOptimization = matchRollupOptimization(expand)
        if (isMatchRollupOptimization) {
          // The sparkPlan: ColumnarExpandExec -> ColumnarHashAggExec => ColumnarExpandExec -> ColumnarHashAggExec -> ColumnarOptRollupExec.
          // ColumnarHashAggExec handles the first combination by Partial mode, i.e. projections[0].
          // ColumnarOptRollupExec handles the residual combinations by PartialMerge mode, i.e. projections[1]~projections[n].
          OmniRollUpOptimizeTransformer(
            p.requiredChildDistributionExpressions,
            p.groupingExpressions,
            p.aggregateExpressions,
            p.aggregateAttributes,
            p.initialInputBufferOffset,
            p.resultExpressions,
            expand,
            expand.child
          )
        } else {
          p
        }
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    logInfo("Using BoostKit Spark Native Sql Engine Extension ColumnarPreOverrides")
    if (GlutenConfig.get.enableRollupOptimization) {
      replaceWithColumnarPlan(plan)
    } else {
      plan
    }
  }
}
