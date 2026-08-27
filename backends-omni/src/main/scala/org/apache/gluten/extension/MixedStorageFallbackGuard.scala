/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.extension

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{OmniAdaptiveHashAggregateExecTransformer, OmniHashAggregateExecTransformer}
import org.apache.gluten.extension.columnar.FallbackTags

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

/**
 * Rule that runs after fallback policy. If a ShuffleExchangeExec fell back to vanilla Spark
 * while mixed storage is enabled, the upstream partial HashAgg would still produce MixedVec
 * which the vanilla shuffle cannot handle. This rule disables mixedOutputEnabled on any
 * OmniAdaptiveHashAggregateExecTransformer that is a descendant of the fallen-back shuffle.
 */
case class MixedStorageFallbackGuard(session: SparkSession) extends Rule[SparkPlan] with Logging {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!GlutenConfig.get.enableMixedStorage) {
      return plan
    }

    plan.transformDown {
      case p: ShuffleExchangeExec if FallbackTags.nonEmpty(p) =>
        val reason = FallbackTags.get(p).reason()
        logWarning(
          s"Mixed storage will not be enabled: ShuffleExchange fell back to vanilla Spark. " +
            s"Reason: $reason")
        p.transformDown {
          case agg: OmniAdaptiveHashAggregateExecTransformer if agg.mixedOutputEnabled =>
            logWarning(s"Disabling mixed output for hash aggregate due to downstream shuffle fallback")
            agg.copy(mixedOutputEnabled = false)
          case agg: OmniHashAggregateExecTransformer if agg.mixedOutputEnabled =>
            logWarning(s"Disabling mixed output for hash aggregate due to downstream shuffle fallback")
            agg.copy(mixedOutputEnabled = false)
          case other => other
        }
      case other => other
    }
  }
}
