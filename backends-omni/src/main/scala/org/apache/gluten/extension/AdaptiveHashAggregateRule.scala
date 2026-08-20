package org.apache.gluten.extension

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.EXCHANGE
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike

/**
 * Skip-partial / flushable aggregation: converts a regular partial HashAggregate (followed by a
 * shuffle exchange) into an adaptive hash aggregate that can flush/abandon partial states.
 *
 * This rule does NOT handle mixed storage. Mixed storage enablement is owned by
 * [[OmniMixedOutputRule]].
 */
case class AdaptiveHashAggregateRule(session: SparkSession) extends Rule[SparkPlan] {
  import HashAggEligibility._

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
                  agg.child,
                  agg.mixedInputExpected,
                  agg.mixedOutputEnabled
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
        if !isIntermediateAgg(agg) =>
        // Not an intermediate agg. Skip.
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

    transformDown(plan)
  }
}
