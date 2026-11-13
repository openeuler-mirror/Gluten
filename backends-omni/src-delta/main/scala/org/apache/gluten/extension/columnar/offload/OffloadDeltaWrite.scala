/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.extension.columnar.offload

import org.apache.gluten.backendsapi.omni.DeltaWriteUtil
import org.apache.gluten.execution.{
  OmniDeltaAppendDataExecV1,
  OmniDeltaOverwriteByExpressionExecV1
}
import org.apache.gluten.extension.columnar.FallbackTags

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlan

/**
 * Offloads Spark 3.5 + Delta 3.2 INSERT/APPEND write plans.
 *
 * Delta still owns the transaction and commit protocol, while the wrapper enables Omni native
 * Parquet writing for the files produced by Delta's FileFormatWriter path.
 */
case class OffloadDeltaAppendDataExecV1() extends OffloadSingleNode with Logging {
  override def offload(plan: SparkPlan): SparkPlan =
    if (!OffloadWrite.nativeWriterEnabled) plan else plan match {
    case p if FallbackTags.nonEmpty(p) =>
      p
    case p if DeltaWriteUtil.isDeltaAppendDataExecV1(p) =>
      logWarning(
        s"[Gluten][Delta+Omni] Offload AppendDataExecV1 -> OmniDeltaAppendDataExecV1 " +
          s"(plan=${p.nodeName}).")
      OmniDeltaAppendDataExecV1(p)
    case other =>
      other
  }
}

/** Offloads Spark 3.5 + Delta 3.2 INSERT OVERWRITE write plans. */
case class OffloadDeltaOverwriteByExpressionExecV1() extends OffloadSingleNode with Logging {
  override def offload(plan: SparkPlan): SparkPlan =
    if (!OffloadWrite.nativeWriterEnabled) plan else plan match {
    case p if FallbackTags.nonEmpty(p) =>
      p
    case p if DeltaWriteUtil.isDeltaOverwriteByExpressionExecV1(p) =>
      logWarning(
        s"[Gluten][Delta+Omni] Offload OverwriteByExpressionExecV1 -> " +
          s"OmniDeltaOverwriteByExpressionExecV1 (plan=${p.nodeName}).")
      OmniDeltaOverwriteByExpressionExecV1(p)
    case other =>
      other
  }
}

/**
 * Provides Delta write offload rules for reflective registration by
 * [[org.apache.gluten.backendsapi.omni.DeltaOffloadRegistry]].
 */
object OffloadDeltaWrite {
  def offloads: Seq[OffloadSingleNode] = Seq(
    OffloadDeltaAppendDataExecV1(),
    OffloadDeltaOverwriteByExpressionExecV1()
  )
}
