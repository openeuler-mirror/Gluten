package org.apache.gluten.extension

import org.apache.gluten.execution.{HashAggregateExecBaseTransformer, OmniResizeBatchesExec, ProjectExecTransformer}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Final, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.plans.physical.ClusteredDistribution
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec

/**
 * Predicates shared by [[AdaptiveHashAggregateRule]] (skip-partial / flushable conversion) and
 * [[OmniMixedOutputRule]] (mixed storage). Keeping them here guarantees the two rules stay aligned
 * on what a convertible partial hash aggregate looks like.
 */
object HashAggEligibility {

  /** All aggregate functions are in partial/partialmerge mode (an intermediate agg). */
  def isIntermediateAgg(agg: HashAggregateExecBaseTransformer): Boolean =
    agg.aggregateExpressions.forall(p => p.mode == Partial || p.mode == PartialMerge)

  /** The agg merges partial states (Final or PartialMerge); it can consume mixed input.
   * Uses requiredChildDistributionExpressions (Some for Final/PartialMerge, None for
   * Partial/Complete) so that GROUP BY without aggregate functions (empty aggregateExpressions)
   * is still correctly identified as a merging agg. */
  def isMergingAgg(agg: HashAggregateExecBaseTransformer): Boolean =
    agg.requiredChildDistributionExpressions.isDefined ||
      agg.aggregateExpressions.exists(p => p.mode == Final || p.mode == PartialMerge)

  def isMergingAgg(agg: BaseAggregateExec): Boolean =
    agg.requiredChildDistributionExpressions.isDefined ||
      agg.aggregateExpressions.exists(p => p.mode == Final || p.mode == PartialMerge)

  def canPropagate(plan: SparkPlan): Boolean = plan match {
    case _: ProjectExecTransformer => true
    case _: OmniResizeBatchesExec => true
    case _ => false
  }

  def isAggInputAlreadyDistributedWithAggKeys(agg: HashAggregateExecBaseTransformer): Boolean = {
    if (agg.groupingExpressions.isEmpty) {
      return false
    }
    val distribution = ClusteredDistribution(agg.groupingExpressions)
    agg.child.outputPartitioning.satisfies(distribution)
  }
}
