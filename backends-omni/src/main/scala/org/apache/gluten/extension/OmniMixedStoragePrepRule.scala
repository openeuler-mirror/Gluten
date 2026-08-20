/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.extension

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.aggregate.{Final, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ExpandExec, ProjectExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.execution.joins.BaseJoinExec
import org.apache.spark.sql.types.MapType
import org.apache.gluten.extension.HashAggEligibility._

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable

object OmniMixedStoragePrepRule {
  // 已打过"条件不满足"警告的中间聚合结构签名；跨 AQE 重计划（物理计划对象被重建、TreeNodeTag 不保留）
  // 仍能抑制重复 WARN。用线程安全 Set，driver 端可能并发跑多条查询。
  private val warnedSignatures: mutable.Set[String] = ConcurrentHashMap.newKeySet[String]().asScala
}

/**
 * Decides which partial HashAggregates should emit mixed (RowSegment) output when AQE is enabled.
 *
 * This rule runs at query-stage preparation time: the full vanilla physical plan is visible and
 * no stage has been created yet. It is the only phase that simultaneously satisfies:
 *   1. the complete topology (final agg / join consumers) is visible
 *   2. the producing partial agg has not executed
 *
 * The decision is stored as a short-lived tag on the vanilla partial aggregate and consumed by
 * `HashAggregateExecBaseTransformer.from`, which materializes it as the `mixedOutputEnabled`
 * case-class field before the Gluten transformer is executed.
 *
 * AQE-off path is intentionally NOT handled here: query-stage prep rules do not run with AQE
 * disabled; the post-transform `OmniMixedOutputRule` handles that case on the complete plan.
 */
case class OmniMixedStoragePrepRule(session: SparkSession) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!GlutenConfig.get.enableMixedStorage) {
      return plan
    }

    var hasJoin = false
    val mergingAggs = mutable.ArrayBuffer[BaseAggregateExec]()
    val taintedKeys = mutable.HashSet[Any]()

    def walk(p: SparkPlan, effConsumer: SparkPlan): Unit = p match {
      case _: BaseJoinExec =>
        hasJoin = true
        p.children.foreach(c => walk(c, p))
      case s: ShuffleExchangeLike =>
        if ((effConsumer ne p) && !isMergingHashAgg(effConsumer)) {
          taintedKeys += partitionKey(s.outputPartitioning)
        }
        walk(s.child, s)
      case w if isTransparentWrapper(w) && w.children.size == 1 =>
        walk(w.children.head, effConsumer)
      case agg: BaseAggregateExec if isMergingAgg(agg) =>
        mergingAggs += agg
        agg.children.foreach(c => walk(c, agg))
      case other =>
        other.children.foreach(c => walk(c, other))
    }

    walk(plan, plan)

    if (!hasJoin) {
      taintedKeys.clear()
    }

    val keys = taintedKeys.toSet
    mergingAggs.foreach(agg => tagProducerIfSafe(agg.child, keys))

    plan
  }

  /**
   * Walks down from a merging agg's child through transparent wrappers and the producing
   * shuffle, then tags the eligible partial agg below the shuffle.
   */
  private def tagProducerIfSafe(plan: SparkPlan, taintedKeys: Set[Any]): Unit = plan match {
    case s: ShuffleExchangeLike =>
      if (!taintedKeys.contains(partitionKey(s.outputPartitioning))) {
        tagEligiblePartial(s.child)
      }
    case w if isTransparentWrapper(w) && w.children.size == 1 =>
      tagProducerIfSafe(w.children.head, taintedKeys)
    case _ =>
  }

  /**
   * 对"被检查但条件不满足"的中间聚合打一次警告；跨 AQE 重计划按结构签名去重。
   * 签名用 .sql（不含 exprId），同一逻辑聚合在多次重计划中文本稳定；
   * 不同分支里的同构聚合会合并为一条（消息相同，无信息损失）。
   */
  private def logRejectedPartialAggOnce(agg: BaseAggregateExec): Unit = {
    val signature =
      s"${agg.nodeName}:${agg.groupingExpressions.map(_.sql).mkString(",")}:" +
        agg.aggregateExpressions.map(_.sql).mkString(",")
    if (OmniMixedStoragePrepRule.warnedSignatures.add(signature)) {
      logWarning(s"Mixed storage not enabled for partial agg: conditions not met")
    }
  }

  /**
   * Tags the first eligible partial HashAgg found below the shuffle.
   */
  private def tagEligiblePartial(plan: SparkPlan): Unit = plan match {
    case agg: BaseAggregateExec if MixedStorageTags.isDecided(agg) =>
      // Already decided, skip entirely
    case agg: BaseAggregateExec
        if isIntermediateAgg(agg) && !isRollupOptimizationCandidate(agg) &&
          !hasUnsupportedComplexTypeKeys(agg) &&
          agg.groupingExpressions.size >= GlutenConfig.get.mixedStorageMinKeys =>
      MixedStorageTags.setMixedOutput(agg, true)
    case agg: BaseAggregateExec if isIntermediateAgg(agg) =>
      logRejectedPartialAggOnce(agg)
      MixedStorageTags.markDecided(agg)
    case w if isTransparentWrapper(w) && w.children.size == 1 =>
      tagEligiblePartial(w.children.head)
    case _ =>
  }

  private def hasUnsupportedComplexTypeKeys(agg: BaseAggregateExec): Boolean = {
    agg.groupingExpressions.exists(_.dataType.isInstanceOf[MapType])
  }

  private def isTransparentWrapper(p: SparkPlan): Boolean = p match {
    case _: WholeStageCodegenExec => true
    case _: ProjectExec => true
    case _ => false
  }

  private def isMergingHashAgg(plan: SparkPlan): Boolean = plan match {
    case agg: BaseAggregateExec =>
      agg.requiredChildDistributionExpressions.isDefined ||
        agg.aggregateExpressions.exists(p => p.mode == Final || p.mode == PartialMerge)
    case _ => false
  }

  private def isIntermediateAgg(agg: BaseAggregateExec): Boolean = {
    agg.aggregateExpressions.forall(p => p.mode == Partial || p.mode == PartialMerge)
  }

  /** Cheap, conservative identity of a shuffle's partition key (ignores partition count). */
  private def partitionKey(p: Partitioning): Any = p match {
    case HashPartitioning(exprs, _) => exprs.map(_.canonicalized)
    case other => other
  }

  /** Vanilla-plan equivalent of RollupOptimization.matchRollupOptimization. */
  private def isRollupOptimizationCandidate(agg: BaseAggregateExec): Boolean = {
    GlutenConfig.get.enableRollupOptimization && (agg.child match {
      case expand: ExpandExec => matchRollupOptimization(expand)
      case _ => false
    })
  }

  private def matchRollupOptimization(expand: ExpandExec): Boolean = {
    if (expand.projections.length == 1) {
      return false
    }
    var step = 0
    expand.projections.foreach { projection =>
      projection.last match {
        case literal: Literal =>
          if (literal.value != (math.pow(2, step) - 1)) {
            return false
          }
        case _ =>
          return false
      }
      step += 1
    }
    true
  }
}
