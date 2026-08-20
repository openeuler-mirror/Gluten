/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.extension

import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.SparkPlan

/**
 * Tree-node tags used to carry mixed-storage enablement decisions from the
 * query-stage preparation phase (where the full physical topology is visible)
 * to the Gluten offload conversion (`HashAggregateExecBaseTransformer.from`).
 *
 * The tag is intentionally short-lived: it is set on vanilla `BaseAggregateExec`
 * nodes by `OmniMixedStoragePrepRule` and consumed by `from` when the vanilla
 * partial aggregate is converted into a Gluten transformer. After conversion the
 * decision lives as the `mixedOutputEnabled` case-class field, which survives
 * all future rule transforms without further tag propagation.
 */
object MixedStorageTags {

  private val MIXED_OUTPUT = TreeNodeTag[Boolean]("gluten.omni.mixedOutput")
  private val MIXED_DECIDED = TreeNodeTag[Boolean]("gluten.omni.mixedDecided")

  def setMixedOutput(plan: SparkPlan, value: Boolean): Unit = {
    plan.setTagValue(MIXED_OUTPUT, value)
    plan.setTagValue(MIXED_DECIDED, true)
  }

  def getMixedOutput(plan: SparkPlan): Boolean = {
    plan.getTagValue(MIXED_OUTPUT).getOrElse(false)
  }

  def isDecided(plan: SparkPlan): Boolean = {
    plan.getTagValue(MIXED_DECIDED).getOrElse(false)
  }

  def markDecided(plan: SparkPlan): Unit = {
    plan.setTagValue(MIXED_DECIDED, true)
  }
}
