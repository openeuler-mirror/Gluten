/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.extension.columnar.offload

import org.apache.gluten.backendsapi.omni.HudiWriteUtil
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.execution.{OmniHudiAppendDataExec, OmniHudiInsertIntoCommandExec}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.v2.AppendDataExec
import org.apache.spark.sql.hudi.command.InsertIntoHoodieTableCommand

/**
 * Hudi Append write offload: replaces [[AppendDataExec]] with [[OmniHudiAppendDataExec]]
 * when the underlying [[org.apache.spark.sql.connector.write.Write]] is Hudi.
 *
 * @since 2026
 */

case class OffloadHudiAppend() extends OffloadSingleNode with Logging {
  override def offload(plan: SparkPlan): SparkPlan =
    if (!OffloadWrite.nativeWriterEnabled) plan else plan match {
    case p if FallbackTags.nonEmpty(p) =>
      p
    case a: AppendDataExec if HudiWriteUtil.supportsWrite(a.write) =>
      logWarning(
        s"[Gluten][Hudi+Omni] Offload Hudi append write: AppendDataExec -> OmniHudiAppendDataExec " +
          s"(write=${a.write.getClass.getName}).")
      OmniHudiAppendDataExec(a)
    case other =>
      other
  }
}

/** Hudi Spark SQL INSERT command offload: Hudi extension plans SQL INSERT as a RunnableCommand. */
case class OffloadHudiInsertIntoCommand() extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan =
    if (!OffloadWrite.nativeWriterEnabled) plan else plan match {
    case p if FallbackTags.nonEmpty(p) =>
      p
    case ExecutedCommandExec(command: InsertIntoHoodieTableCommand) =>
      OmniHudiInsertIntoCommandExec(command)
    case other =>
      other
  }
}

/** Collection of Hudi write offload rules (loaded via `HudiOffloadRegistry` when `-Phudi` is used). */
object OffloadHudiWrite {
  /** Rules registered for Omni columnar Hudi write. */
  def offloads: Seq[OffloadSingleNode] = Seq(
    OffloadHudiInsertIntoCommand(),
    OffloadHudiAppend()
  )
}
