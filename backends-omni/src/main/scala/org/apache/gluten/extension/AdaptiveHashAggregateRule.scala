package org.apache.gluten.extension

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.aggregate.{Partial, PartialMerge}
import org.apache.spark.sql.catalyst.plans.physical.ClusteredDistribution
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.EXCHANGE
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.gluten.execution.OmniHashAggregateExecTransformer

case class AdaptiveHashAggregateRule(session: SparkSession) extends Rule[SparkPlan] {
  import AdaptiveHashAggregateRule._
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!GlutenConfig.get.enableAdaptivePartialAggregation) {
      return plan
    }
    plan.transformUpWithPruning(_.containsPattern(EXCHANGE)) {
      case s: ShuffleExchangeLike =>
        // If an exchange follows a hash aggregate in which all functions are in partial mode,
        // then it's safe to convert the hash aggregate to adaptive hash aggregate.
        val out = s.withNewChildren(
          List(
            replaceEligibleAggregates(s.child) {
              agg =>
                OmniAdaptiveHashAggregateExecTransformer(
                  agg.requiredChildDistributionExpressions,
                  agg.groupingExpressions,
                  agg.aggregateExpressions,
                  agg.aggregateAttributes,
                  agg.initialInputBufferOffset,
                  agg.resultExpressions,
                  agg.child
                )
            }
          )
        )
        out
    }
  }

  private def replaceEligibleAggregates(plan: SparkPlan)(
    func: OmniHashAggregateExecTransformer => SparkPlan): SparkPlan = {
    def transformDown: SparkPlan => SparkPlan = {
      case agg: OmniHashAggregateExecTransformer
        if agg.isSortAggregation =>
        // SortAggregate requires its child ordering. Do not replace it with
        // adaptive hash aggregation, even when it is a partial aggregate.
        agg
      case agg: OmniHashAggregateExecTransformer
        if !agg.aggregateExpressions.forall(p => p.mode == Partial || p.mode == PartialMerge) =>
        // Not a intermediate agg. Skip.
        agg
      case agg: OmniHashAggregateExecTransformer
        if isAggInputAlreadyDistributedWithAggKeys(agg) =>
        // Data already grouped by aggregate keys, Skip.
        agg
      case agg: OmniHashAggregateExecTransformer =>
        func(agg)
      case p if !canPropagate(p) => p
      case other => other.withNewChildren(other.children.map(transformDown))
    }

    val out = transformDown(plan)
    out
  }

  private def canPropagate(plan: SparkPlan): Boolean = plan match {
    case _: ProjectExecTransformer => true
    case _: OmniResizeBatchesExec => true
    case _ => false
  }
}

object AdaptiveHashAggregateRule {

  /**
   * If child output already partitioned by aggregation keys (this function returns true), we
   * usually avoid the optimization converting to flushable aggregation.
   *
   * For example, if input is hash-partitioned by keys (a, b) and aggregate node requests "group by
   * a, b, c", then the aggregate should NOT flush as the grouping set (a, b, c) will be created
   * only on a single partition among the whole cluster. Spark's planner may use this information to
   * perform optimizations like doing "partial_count(a, b, c)" directly on the output data.
   */
  private def isAggInputAlreadyDistributedWithAggKeys(agg: OmniHashAggregateExecTransformer): Boolean = {
    if (agg.groupingExpressions.isEmpty) {
      // Empty grouping set () should not be satisfied by any partitioning patterns.
      //   E.g.,
      //   (a, b) satisfies (a, b, c)
      //   (a, b) satisfies (a, b)
      //   (a, b) doesn't satisfy (a)
      //   (a, b) doesn't satisfy ()
      return false
    }
    val distribution = ClusteredDistribution(agg.groupingExpressions)
    val res = agg.child.outputPartitioning.satisfies(distribution)
    res
  }
}
