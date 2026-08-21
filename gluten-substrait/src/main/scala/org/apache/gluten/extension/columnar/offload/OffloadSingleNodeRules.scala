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
package org.apache.gluten.extension.columnar.offload

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution._
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.logging.LogLevelUtil
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.api.python.EvalPythonExecTransformer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{BindReferences, SortOrder}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{BROADCAST, Join, JoinHint, JoinStrategyHint, SHUFFLE_HASH, SHUFFLE_MERGE, SHUFFLE_REPLICATE_NL}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.python.{ArrowEvalPythonExec, BatchEvalPythonExec}
import org.apache.spark.sql.execution.window.{WindowExec, WindowGroupLimitExecShim}
import org.apache.spark.sql.hive.HiveTableScanExecTransformer
import org.apache.spark.sql.internal.SQLConf

// Exchange transformation.
case class OffloadExchange() extends OffloadSingleNode with LogLevelUtil {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case p if FallbackTags.nonEmpty(p) =>
      p
    case s: ShuffleExchangeExec
        if (s.child.supportsColumnar || GlutenConfig.get.enablePreferColumnar) &&
          BackendsApiManager.getSettings.supportColumnarShuffleExec() =>
      logDebug(s"Columnar Processing for ${s.getClass} is currently supported.")
      BackendsApiManager.getSparkPlanExecApiInstance.genColumnarShuffleExchange(s)
    case b: BroadcastExchangeExec =>
      val child = b.child
      logDebug(s"Columnar Processing for ${b.getClass} is currently supported.")
      ColumnarBroadcastExchangeExec(b.mode, child)
    case other => other
  }
}

// Join transformation.
case class OffloadJoin() extends OffloadSingleNode with LogLevelUtil {
  override def offload(plan: SparkPlan): SparkPlan = {
    if (FallbackTags.nonEmpty(plan)) {
      logDebug(s"Columnar Processing for ${plan.getClass} is under row guard.")
      return plan
    }
    plan match {
      case plan: ShuffledHashJoinExec =>
        val left = plan.left
        val right = plan.right
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        BackendsApiManager.getSparkPlanExecApiInstance
          .genShuffledHashJoinExecTransformer(
            plan.leftKeys,
            plan.rightKeys,
            plan.joinType,
            OffloadJoin.getShjBuildSide(plan),
            plan.condition,
            left,
            right,
            plan.isSkewJoin)

      case plan: SortMergeJoinExec =>
        val left = plan.left
        val right = plan.right
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")

        // AQE runtime statistics decide the hash-join strategy.
        // Priority: BHJ -> SHJ -> keep SMJ.
        OffloadJoin.checkAndConvertSmjToBhj(plan, left, right)
          .orElse(OffloadJoin.checkAndConvertSmjToShj(plan, left, right)) match {
          case Some(hashJoinTransformer) =>
            hashJoinTransformer

          case None =>
            BackendsApiManager.getSparkPlanExecApiInstance
              .genSortMergeJoinExecTransformer(
                plan.leftKeys,
                plan.rightKeys,
                plan.joinType,
                plan.condition,
                left,
                right,
                plan.isSkewJoin)
        }

      case plan: BroadcastHashJoinExec =>
        val left = plan.left
        val right = plan.right
        BackendsApiManager.getSparkPlanExecApiInstance
          .genBroadcastHashJoinExecTransformer(
            plan.leftKeys,
            plan.rightKeys,
            plan.joinType,
            plan.buildSide,
            plan.condition,
            left,
            right,
            isNullAwareAntiJoin = plan.isNullAwareAntiJoin)

      case plan: CartesianProductExec =>
        val left = plan.left
        val right = plan.right
        BackendsApiManager.getSparkPlanExecApiInstance
          .genCartesianProductExecTransformer(left, right, plan.condition)

      case plan: BroadcastNestedLoopJoinExec =>
        val left = plan.left
        val right = plan.right
        BackendsApiManager.getSparkPlanExecApiInstance
          .genBroadcastNestedLoopJoinExecTransformer(
            left,
            right,
            plan.buildSide,
            plan.joinType,
            plan.condition)

      case other => other
    }
  }
}

object OffloadJoin extends Logging {
  /**
   * Pick the SHJ build side.
   *
   * Priority:
   *   1. Respect backend/join-type build-side restrictions.
   *   2. If re-optimization is disabled, keep Spark's original build side.
   *   3. Prefer materialized AQE ShuffleQueryStageExec runtime statistics.
   *   4. Fall back to logical-plan statistics.
   *   5. Finally fall back to Spark's original build side.
   *
   * Runtime total size is used to choose the smaller legal build side. The
   * max-partition check used to decide whether SMJ should become SHJ is kept
   * separately in checkAndConvertSmjToShj().
   */
  def getShjBuildSide(shj: ShuffledHashJoinExec): BuildSide = {
    val leftBuildable =
      BackendsApiManager.getSettings.supportHashBuildJoinTypeOnLeft(shj.joinType)
    val rightBuildable =
      BackendsApiManager.getSettings.supportHashBuildJoinTypeOnRight(shj.joinType)

    assert(leftBuildable || rightBuildable)

    // If only one side is legal for this join type/backend, we must use it.
    if (!leftBuildable) {
      logInfo(
        s"SHJ build side forced to BuildRight: joinType=${shj.joinType}, " +
          s"leftBuildable=$leftBuildable, rightBuildable=$rightBuildable")
      return BuildRight
    }

    if (!rightBuildable) {
      logInfo(
        s"SHJ build side forced to BuildLeft: joinType=${shj.joinType}, " +
          s"leftBuildable=$leftBuildable, rightBuildable=$rightBuildable")
      return BuildLeft
    }

    // Both sides are legal. Respect the existing configuration switch.
    if (!GlutenConfig.get.shuffledHashJoinOptimizeBuildSide) {
      logInfo(
        s"SHJ build-side re-optimization disabled; keep Spark buildSide=${shj.buildSide}")
      return shj.buildSide
    }

    // First choice: AQE runtime statistics from materialized shuffle stages.
    val leftStageOpt = findShuffleQueryStage(shj.left)
    val rightStageOpt = findShuffleQueryStage(shj.right)

    if (leftStageOpt.isDefined && rightStageOpt.isDefined) {
      val leftStage = leftStageOpt.get
      val rightStage = rightStageOpt.get

      if (leftStage.isMaterialized && rightStage.isMaterialized) {
        val leftStats = leftStage.getRuntimeStatistics
        val rightStats = rightStage.getRuntimeStatistics

        if (leftStats != null && rightStats != null) {
          val leftSize = leftStats.sizeInBytes
          val rightSize = rightStats.sizeInBytes
          val leftRowCount = leftStats.rowCount
          val rightRowCount = rightStats.rowCount

          val runtimeBuildSide =
            if (leftSize < rightSize) {
              BuildLeft
            } else if (rightSize < leftSize) {
              BuildRight
            } else if (leftRowCount.isDefined && rightRowCount.isDefined) {
              if (leftRowCount.get <= rightRowCount.get) BuildLeft else BuildRight
            } else {
              // Equal runtime size and no row-count tie breaker: preserve Spark's choice.
              shj.buildSide
            }

          logInfo(
            s"""
               |========== SHJ RUNTIME BUILD-SIDE SELECTION ==========
               |joinType              = ${shj.joinType}
               |Spark original side   = ${shj.buildSide}
               |left runtime size     = $leftSize
               |right runtime size    = $rightSize
               |left runtime rows     = $leftRowCount
               |right runtime rows    = $rightRowCount
               |selected build side   = $runtimeBuildSide
               |======================================================
               |""".stripMargin)

          return runtimeBuildSide
        }
      }

      logDebug(
        s"AQE runtime statistics unavailable for SHJ build-side selection: " +
          s"leftMaterialized=${leftStage.isMaterialized}, " +
          s"rightMaterialized=${rightStage.isMaterialized}")
    } else {
      logDebug(
        s"Cannot find both ShuffleQueryStageExec nodes for SHJ build-side selection: " +
          s"leftStage=${leftStageOpt.isDefined}, rightStage=${rightStageOpt.isDefined}")
    }

    // Second choice: logical statistics, preserving the original Gluten behavior.
    shj.logicalLink
      .flatMap {
        case join: Join =>
          val buildSide = getOptimalBuildSide(join)
          logInfo(
            s"SHJ runtime stats unavailable; use logical statistics: " +
              s"joinType=${shj.joinType}, selectedBuildSide=$buildSide")
          Some(buildSide)
        case _ =>
          None
      }
      .getOrElse {
        // Some SHJ operators (for example in Spark tests) may not have a logical link.
        logInfo(
          s"SHJ runtime/logical stats unavailable; keep Spark buildSide=${shj.buildSide}")
        shj.buildSide
      }
  }

  def getOptimalBuildSide(join: Join): BuildSide = {
    val leftSize = join.left.stats.sizeInBytes
    val rightSize = join.right.stats.sizeInBytes
    val leftRowCount = join.left.stats.rowCount
    val rightRowCount = join.right.stats.rowCount
    if (leftSize == rightSize && rightRowCount.isDefined && leftRowCount.isDefined) {
      if (rightRowCount.get <= leftRowCount.get) {
        return BuildRight
      }
      return BuildLeft
    }
    if (rightSize <= leftSize) {
      return BuildRight
    }
    BuildLeft
  }

  // Read the original logical JoinHint attached to this SMJ.
  private def getJoinHint(smj: SortMergeJoinExec): Option[JoinHint] = {
    smj.logicalLink.collect {
      case join: Join => join.hint
    }
  }

  private def hasHint(hint: JoinHint, strategy: JoinStrategyHint): Boolean = {
    hint.leftHint.exists(_.strategy.contains(strategy)) ||
    hint.rightHint.exists(_.strategy.contains(strategy))
  }

  /**
   * Find the ShuffleQueryStageExec that provides the input of one SMJ side.
   *
   * In AQE the stage is often wrapped by unary operators, for example:
   *
   * SortExec
   *   +- AQEShuffleReadExec
   *        +- ShuffleQueryStageExec
   *
   * Only follow a unary chain. This avoids accidentally picking an unrelated
   * shuffle stage from a more complex multi-child subtree.
   */
  private def findShuffleQueryStage(plan: SparkPlan): Option[ShuffleQueryStageExec] = {
    plan match {
      case stage: ShuffleQueryStageExec =>
        Some(stage)

      case p if p.children.size == 1 =>
        findShuffleQueryStage(p.children.head)

      case _ =>
        None
    }
  }

  /** Return the largest materialized shuffle partition size, in bytes. */
  private def maxPartitionSize(stage: ShuffleQueryStageExec): Option[Long] = {
    stage.mapStats.flatMap { stats =>
      val partitionSizes = stats.bytesByPartitionId
      if (partitionSizes == null || partitionSizes.isEmpty) {
        None
      } else {
        Some(partitionSizes.max)
      }
    }
  }

  /**
   * Remove the local SortExec that was inserted only to satisfy SMJ's join-key ordering.
   *
   * We intentionally keep global sorts and any sort whose ordering does not satisfy the
   * ordering required by this SMJ input. This makes the removal conservative: only the
   * obvious SMJ-local sort is stripped when switching to a hash join, which itself has no
   * child-ordering requirement.
   */
  private def stripSmjLocalSort(
      plan: SparkPlan,
      requiredOrdering: Seq[SortOrder]): SparkPlan = {
    plan match {
      case sort: SortExec
          if !sort.global && SortOrder.orderingSatisfies(sort.sortOrder, requiredOrdering) =>
        logInfo(
          s"Remove SMJ-local SortExec before hash join: sortOrder=${sort.sortOrder}, " +
            s"requiredOrdering=$requiredOrdering")
        sort.child

      case other =>
        other
    }
  }

  private def canBuildBroadcastLeft(joinType: JoinType): Boolean = {
    joinType match {
      case _: InnerLike | RightOuter => true
      case _ => false
    }
  }

  private def canBuildBroadcastRight(joinType: JoinType): Boolean = {
    joinType match {
      case _: InnerLike | LeftOuter | LeftSemi | LeftAnti | _: ExistenceJoin => true
      case _ => false
    }
  }

  /**
   * Select a legal broadcast build side from AQE runtime sizes.
   * A side is eligible only when Spark's join semantics allow it and the materialized
   * runtime size is no larger than spark.sql.autoBroadcastJoinThreshold.
   */
  private def chooseBroadcastBuildSide(
      smj: SortMergeJoinExec,
      leftSize: BigInt,
      rightSize: BigInt,
      threshold: Long): Option[BuildSide] = {
    if (threshold < 0) {
      return None
    }

    val leftEligible = canBuildBroadcastLeft(smj.joinType) && leftSize >= 0 && leftSize <= threshold
    val rightEligible =
      canBuildBroadcastRight(smj.joinType) && rightSize >= 0 && rightSize <= threshold

    (leftEligible, rightEligible) match {
      case (false, false) => None
      case (true, false) => Some(BuildLeft)
      case (false, true) => Some(BuildRight)
      case (true, true) =>
        if (leftSize <= rightSize) Some(BuildLeft) else Some(BuildRight)
    }
  }

  /**
   * Try to offload a materialized AQE SortMergeJoin as BroadcastHashJoin.
   *
   * The smaller legal side is broadcast when its real runtime table size is no larger
   * than spark.sql.autoBroadcastJoinThreshold. We construct ColumnarBroadcastExchangeExec
   * directly so no Spark row BroadcastHashJoinExec is inserted into an already-columnar
   * Gluten plan.
   */
  def checkAndConvertSmjToBhj(
      smj: SortMergeJoinExec,
      left: SparkPlan,
      right: SparkPlan): Option[SparkPlan] = {
    val joinHint = getJoinHint(smj)

    // Hint first. BHJ only consumes BROADCAST.
    // MERGE keeps SMJ; SHUFFLE_HASH is handled by checkAndConvertSmjToShj.
    if (joinHint.exists(h =>
        hasHint(h, SHUFFLE_MERGE) ||
        hasHint(h, SHUFFLE_HASH) ||
        hasHint(h, SHUFFLE_REPLICATE_NL))) {
      return None
    }

    val hasBroadcastHint = joinHint.exists(hasHint(_, BROADCAST))
    val buildSideOpt =
    if (hasBroadcastHint) {
      val hint = joinHint.get
      val hintLeft = hint.leftHint.exists(_.strategy.contains(BROADCAST))
      val hintRight = hint.rightHint.exists(_.strategy.contains(BROADCAST))

      // Explicit BROADCAST hint has priority over the automatic size threshold.
      // Prefer the hinted side; if that side is illegal for this join type,
      // fall back to the other legal side.
      val leftBuildable = canBuildBroadcastLeft(smj.joinType)
      val rightBuildable = canBuildBroadcastRight(smj.joinType)
      if (hintLeft && hintRight) {
        // Both sides have BROADCAST: use the smaller legal side.
        (leftBuildable, rightBuildable) match {
          case (true, true) => Some(BuildLeft)
          case (true, false) => Some(BuildLeft)
          case (false, true) => Some(BuildRight)
          case _ => None
        }
      } else if (hintLeft && leftBuildable) {
        Some(BuildLeft)
      } else if (hintRight && rightBuildable) {
        Some(BuildRight)
      } else if (leftBuildable) {
        Some(BuildLeft)
      } else if (rightBuildable) {
        Some(BuildRight)
      } else {
        None
      }
    } else {
      val leftStageOpt = findShuffleQueryStage(left)
      val rightStageOpt = findShuffleQueryStage(right)

      if (leftStageOpt.isEmpty || rightStageOpt.isEmpty) {
        logDebug(
          s"Skip SMJ -> BHJ: cannot find both ShuffleQueryStageExec nodes, " +
            s"leftStage=${leftStageOpt.isDefined}, rightStage=${rightStageOpt.isDefined}")
        return None
      }

      val leftStage = leftStageOpt.get
      val rightStage = rightStageOpt.get

      if (!leftStage.isMaterialized || !rightStage.isMaterialized) {
        logDebug(
          s"Skip SMJ -> BHJ: shuffle stages are not both materialized, " +
            s"leftMaterialized=${leftStage.isMaterialized}, " +
            s"rightMaterialized=${rightStage.isMaterialized}")
        return None
      }

      val leftStats = leftStage.getRuntimeStatistics
      val rightStats = rightStage.getRuntimeStatistics
      if (leftStats == null || rightStats == null) {
        logDebug("Skip SMJ -> BHJ: runtime statistics are unavailable.")
        return None
      }

      val leftSize = leftStats.sizeInBytes
      val rightSize = rightStats.sizeInBytes
      val autoBroadcastJoinThreshold = SQLConf.get.autoBroadcastJoinThreshold

      chooseBroadcastBuildSide(smj, leftSize, rightSize, autoBroadcastJoinThreshold)
    }

    if (buildSideOpt.isEmpty) {
      return None
    }

    val buildSide = buildSideOpt.get

    // Hash joins do not require the SMJ child ordering. Strip only local SortExec nodes
    // that can be identified as satisfying this SMJ's own join-key ordering requirement.
    val hashLeft = stripSmjLocalSort(left, smj.requiredChildOrdering.head)
    val hashRight = stripSmjLocalSort(right, smj.requiredChildOrdering(1))

    val (buildKeys, buildChild) = buildSide match {
      case BuildLeft => (smj.leftKeys, hashLeft)
      case BuildRight => (smj.rightKeys, hashRight)
    }

    // BroadcastHashJoinExec uses bound build keys in HashedRelationBroadcastMode.
    val boundBuildKeys = BindReferences.bindReferences(buildKeys, buildChild.output)
    val broadcastMode = HashedRelationBroadcastMode(boundBuildKeys, isNullAware = false)
    val broadcastExchange = ColumnarBroadcastExchangeExec(broadcastMode, buildChild)

    val bhjLeft = if (buildSide == BuildLeft) broadcastExchange else hashLeft
    val bhjRight = if (buildSide == BuildRight) broadcastExchange else hashRight

    val bhjTransformer = BackendsApiManager.getSparkPlanExecApiInstance
      .genBroadcastHashJoinExecTransformer(
        smj.leftKeys,
        smj.rightKeys,
        smj.joinType,
        buildSide,
        smj.condition,
        bhjLeft,
        bhjRight,
        isNullAwareAntiJoin = false)

    val validateResult = bhjTransformer.doValidate()
    if (validateResult.ok()) {
      Some(bhjTransformer)
    } else {
      logDebug(
        s"Keep non-BHJ plan because BHJ transformer validation failed: " +
          s"${validateResult.reason()}")
      None
    }
  }

  /**
   * Select a legal SHJ build side.
   *
   * The smaller side is preferred only when both sides are supported by the
   * backend for this join type. Outer/semi/anti joins may restrict the legal
   * build side.
   */
  private def chooseBuildSide(
      smj: SortMergeJoinExec,
      leftSize: BigInt,
      rightSize: BigInt): Option[BuildSide] = {
    val leftBuildable =
      BackendsApiManager.getSettings.supportHashBuildJoinTypeOnLeft(smj.joinType)
    val rightBuildable =
      BackendsApiManager.getSettings.supportHashBuildJoinTypeOnRight(smj.joinType)

    (leftBuildable, rightBuildable) match {
      case (false, false) =>
        None

      case (true, false) =>
        Some(BuildLeft)

      case (false, true) =>
        Some(BuildRight)

      case (true, true) =>
        if (leftSize <= rightSize) {
          Some(BuildLeft)
        } else {
          Some(BuildRight)
        }
    }
  }

  /**
   * Try to offload a SortMergeJoin as ShuffledHashJoin using AQE runtime
   * shuffle statistics.
   *
   * Conditions:
   *   1. Both SMJ inputs must contain ShuffleQueryStageExec.
   *   2. Both query stages must already be materialized.
   *   3. Runtime MapOutputStatistics must be available.
   *   4. Omni must support a legal hash build side for this join type.
   *   5. The maximum partition size on the selected build side must not
   *      exceed Gluten's shuffleHashJoinThreshold.
   *   6. The generated SHJ transformer must pass backend validation.
   *
   * This method deliberately creates the Omni SHJ transformer directly. It
   * does NOT insert a new Spark ShuffledHashJoinExec after Gluten columnar
   * transformation has started, avoiding Row/Columnar execution mismatches.
   */
  def checkAndConvertSmjToShj(
      smj: SortMergeJoinExec,
      left: SparkPlan,
      right: SparkPlan): Option[SparkPlan] = {
    val joinHint = getJoinHint(smj)

    // Hint first. SHJ only consumes SHUFFLE_HASH.
    // BROADCAST is handled by BHJ; MERGE keeps SMJ.
    if (joinHint.exists(h =>
        hasHint(h, BROADCAST) ||
        hasHint(h, SHUFFLE_MERGE) ||
        hasHint(h, SHUFFLE_REPLICATE_NL))) {
      return None
    }

    val hasShuffleHashHint = joinHint.exists(hasHint(_, SHUFFLE_HASH))

    val buildSideOpt =
      if (hasShuffleHashHint) {
        val hint = joinHint.get
        val hintLeft = hint.leftHint.exists(_.strategy.contains(SHUFFLE_HASH))
        val hintRight = hint.rightHint.exists(_.strategy.contains(SHUFFLE_HASH))
        val leftBuildable =
          BackendsApiManager.getSettings.supportHashBuildJoinTypeOnLeft(smj.joinType)
        val rightBuildable =
          BackendsApiManager.getSettings.supportHashBuildJoinTypeOnRight(smj.joinType)

        // Explicit SHUFFLE_HASH hint: prefer its build side. If that side is
        // illegal (e.g. BuildLeft for LeftSemi), reuse the original legal-side logic.
        if (hintLeft && hintRight) {
          // Both sides have SHUFFLE_HASH: keep the original smaller/legal-side selection.
          Some(BuildLeft)
        } else if (hintLeft && leftBuildable) {
          Some(BuildLeft)
        } else if (hintRight && rightBuildable) {
          Some(BuildRight)
        }  else if (leftBuildable) {
          Some(BuildLeft)
        } else if (rightBuildable) {
          Some(BuildRight)
        } else {
          None
        }
      } else {
        val leftStageOpt = findShuffleQueryStage(left)
        val rightStageOpt = findShuffleQueryStage(right)

        if (leftStageOpt.isEmpty || rightStageOpt.isEmpty) {
          logDebug(
            s"Skip SMJ -> SHJ: cannot find ShuffleQueryStageExec, " +
              s"leftStage=${leftStageOpt.isDefined}, rightStage=${rightStageOpt.isDefined}")
          return None
        }

        val leftStage = leftStageOpt.get
        val rightStage = rightStageOpt.get

        // getRuntimeStatistics is meaningful only after stage materialization.
        if (!leftStage.isMaterialized || !rightStage.isMaterialized) {
          logDebug(
            s"Skip SMJ -> SHJ: shuffle stages are not both materialized, " +
              s"leftMaterialized=${leftStage.isMaterialized}, " +
              s"rightMaterialized=${rightStage.isMaterialized}")
          return None
        }

        val leftStats = leftStage.getRuntimeStatistics
        val rightStats = rightStage.getRuntimeStatistics

        if (leftStats == null || rightStats == null) {
          logDebug("Skip SMJ -> SHJ: runtime statistics are unavailable.")
          return None
        }

        val leftSize = leftStats.sizeInBytes
        val rightSize = rightStats.sizeInBytes

        // For SHJ, each task builds a hash table for one shuffle partition. The
        // per-partition size therefore matters more than only the total stage size.
        val leftMaxPartitionOpt = maxPartitionSize(leftStage)
        val rightMaxPartitionOpt = maxPartitionSize(rightStage)

        if (leftMaxPartitionOpt.isEmpty || rightMaxPartitionOpt.isEmpty) {
          logDebug(
            s"Skip SMJ -> SHJ: MapOutputStatistics unavailable, " +
              s"leftMapStats=${leftStage.mapStats.isDefined}, " +
              s"rightMapStats=${rightStage.mapStats.isDefined}")
          return None
        }

        val leftMaxPartition = leftMaxPartitionOpt.get
        val rightMaxPartition = rightMaxPartitionOpt.get

        // No hint: original logic unchanged.
        val buildSideOpt = chooseBuildSide(smj, leftSize, rightSize)
        if (buildSideOpt.isEmpty) {
          logDebug(
            s"Skip SMJ -> SHJ: joinType=${smj.joinType} has no supported hash build side.")
          return None
        }

        val buildMaxPartitionSize = buildSideOpt.get match {
          case BuildLeft => leftMaxPartition
          case BuildRight => rightMaxPartition
        }

        val shuffleHashJoinThreshold = GlutenConfig.get.shuffleHashJoinThreshold

        if (buildMaxPartitionSize > shuffleHashJoinThreshold) {
          logDebug(
            s"Keep SMJ: buildMaxPartitionSize=$buildMaxPartitionSize > " +
              s"shuffleHashJoinThreshold=$shuffleHashJoinThreshold")
          return None
        }

        buildSideOpt
    }

    if (buildSideOpt.isEmpty) {
      logDebug(
        s"Skip SMJ -> SHJ: joinType=${smj.joinType} has no supported hash build side.")
      return None
    }

    val buildSide = buildSideOpt.get

    // SHJ has no child-ordering requirement. Remove only the local SortExec nodes that
    // were inserted to satisfy this SMJ's join-key ordering.
    val shjLeft = stripSmjLocalSort(left, smj.requiredChildOrdering.head)
    val shjRight = stripSmjLocalSort(right, smj.requiredChildOrdering(1))

    val shjTransformer = BackendsApiManager.getSparkPlanExecApiInstance
      .genShuffledHashJoinExecTransformer(
        smj.leftKeys,
        smj.rightKeys,
        smj.joinType,
        buildSide,
        smj.condition,
        shjLeft,
        shjRight,
        smj.isSkewJoin)

    val validateResult = shjTransformer.doValidate()
    if (validateResult.ok()) {
      Some(shjTransformer)
    } else {
      logDebug(
        s"Keep SMJ because SHJ transformer validation failed: ${validateResult.reason()}")
      None
    }
  }
}

// Other transformations.
case class OffloadOthers() extends OffloadSingleNode with LogLevelUtil {
  import OffloadOthers._
  private val replace = new ReplaceSingleNode

  override def offload(plan: SparkPlan): SparkPlan = replace.doReplace(plan)
}

object OffloadOthers {
  // Utility to replace single node within transformed Gluten node.
  // Children will be preserved as they are as children of the output node.
  //
  // Do not look up on children on the input node in this rule. Otherwise
  // it may break RAS which would group all the possible input nodes to
  // search for validate candidates.
  private class ReplaceSingleNode extends LogLevelUtil with Logging {

    def doReplace(p: SparkPlan): SparkPlan = {
      val plan = p
      if (FallbackTags.nonEmpty(plan)) {
        return plan
      }
      plan match {
        case plan: BatchScanExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          ScanTransformerFactory.createBatchScanTransformer(plan)
        case plan: FileSourceScanExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          ScanTransformerFactory.createFileSourceScanTransformer(plan)
        case plan if HiveTableScanExecTransformer.isHiveTableScan(plan) =>
          // TODO: Add DynamicPartitionPruningHiveScanSuite.scala
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          ScanTransformerFactory.createHiveTableScanTransformer(plan)
        case plan: CoalesceExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          ColumnarCoalesceExec(plan.numPartitions, plan.child)
        case plan: FilterExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          BackendsApiManager.getSparkPlanExecApiInstance
            .genFilterExecTransformer(plan.condition, plan.child)
        case plan: ProjectExec =>
          val columnarChild = plan.child
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          ProjectExecTransformer(plan.projectList, columnarChild)
        case plan: HashAggregateExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          HashAggregateExecBaseTransformer.from(plan)
        case plan: SortAggregateExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          HashAggregateExecBaseTransformer.from(plan)
        case plan: ObjectHashAggregateExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          HashAggregateExecBaseTransformer.from(plan)
        case plan: UnionExec =>
          val children = plan.children
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          ColumnarUnionExec(children)
        case plan: ExpandExec =>
          val child = plan.child
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          ExpandExecTransformer(plan.projections, plan.output, child)
        case plan: WriteFilesExec =>
          val child = plan.child
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val writeTransformer = WriteFilesExecTransformer(
            child,
            plan.fileFormat,
            plan.partitionColumns,
            plan.bucketSpec,
            plan.options,
            plan.staticPartitions)
          ColumnarWriteFilesExec(
            writeTransformer,
            plan.fileFormat,
            plan.partitionColumns,
            plan.bucketSpec,
            plan.options,
            plan.staticPartitions)
        case plan: SortExec =>
          val child = plan.child
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          SortExecTransformer(plan.sortOrder, plan.global, child, plan.testSpillFrequency)
        case plan: TakeOrderedAndProjectExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          val (limit, offset) = SparkShimLoader.getSparkShims.getLimitAndOffsetFromTopK(plan)
          TakeOrderedAndProjectExecTransformer(
            limit,
            plan.sortOrder,
            plan.projectList,
            child,
            offset)
        case plan: WindowExec =>
          WindowExecTransformer(
            plan.windowExpression,
            plan.partitionSpec,
            plan.orderSpec,
            plan.child)
        case plan if SparkShimLoader.getSparkShims.isWindowGroupLimitExec(plan) =>
          val windowGroupLimitPlan = SparkShimLoader.getSparkShims
            .getWindowGroupLimitExecShim(plan)
            .asInstanceOf[WindowGroupLimitExecShim]
          BackendsApiManager.getSparkPlanExecApiInstance.genWindowGroupLimitTransformer(
            windowGroupLimitPlan.partitionSpec,
            windowGroupLimitPlan.orderSpec,
            windowGroupLimitPlan.rankLikeFunction,
            windowGroupLimitPlan.limit,
            windowGroupLimitPlan.mode,
            windowGroupLimitPlan.child
          )
        case plan: GlobalLimitExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          val (limit, offset) =
            SparkShimLoader.getSparkShims.getLimitAndOffsetFromGlobalLimit(plan)
          LimitExecTransformer(child, offset, limit)
        case plan: LocalLimitExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          LimitExecTransformer(child, 0L, plan.limit)
        case plan: GenerateExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          BackendsApiManager.getSparkPlanExecApiInstance.genGenerateTransformer(
            plan.generator,
            plan.requiredChildOutput,
            plan.outer,
            plan.generatorOutput,
            child)
        case plan: BatchEvalPythonExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          EvalPythonExecTransformer(plan.udfs, plan.resultAttrs, child)
        case plan: ArrowEvalPythonExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          // For ArrowEvalPythonExec, CH supports it through EvalPythonExecTransformer while
          // Velox backend uses ColumnarArrowEvalPythonExec.
          if (
            !BackendsApiManager.getSettings.supportColumnarArrowUdf() ||
            !GlutenConfig.get.enableColumnarArrowUDF
          ) {
            EvalPythonExecTransformer(plan.udfs, plan.resultAttrs, child)
          } else {
            BackendsApiManager.getSparkPlanExecApiInstance.createColumnarArrowEvalPythonExec(
              plan.udfs,
              plan.resultAttrs,
              child,
              plan.evalType)
          }
        case plan: SampleExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          BackendsApiManager.getSparkPlanExecApiInstance.genSampleExecTransformer(
            plan.lowerBound,
            plan.upperBound,
            plan.withReplacement,
            plan.seed,
            child)
        case p if !p.isInstanceOf[GlutenPlan] =>
          logDebug(s"Transformation for ${p.getClass} is currently not supported.")
          p
        case other => other
      }
    }
  }
}
