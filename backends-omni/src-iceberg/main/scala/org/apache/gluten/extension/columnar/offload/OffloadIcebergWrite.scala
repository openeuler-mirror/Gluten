/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.extension.columnar.offload

import org.apache.gluten.execution.{
  OmniIcebergAppendDataExec,
  OmniIcebergOverwriteByExpressionExec,
  OmniIcebergOverwritePartitionsDynamicExec,
  OmniIcebergReplaceDataExec,
  OmniIcebergWriteToDataSourceV2Exec
}
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.logging.LogLevelUtil

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{
  AppendDataExec,
  OverwriteByExpressionExec,
  OverwritePartitionsDynamicExec,
  ReplaceDataExec,
  WriteToDataSourceV2Exec
}

import org.apache.iceberg.spark.source.IcebergWriteUtil.supportsWrite

/** Iceberg Append write offload: replaces AppendDataExec with Omni columnar write Exec. */
case class OffloadIcebergAppend() extends OffloadSingleNode with LogLevelUtil {
  override def offload(plan: SparkPlan): SparkPlan =
    if (!OffloadWrite.nativeWriterEnabled) plan else plan match {
    case p if FallbackTags.nonEmpty(p) =>
      p
    case a: AppendDataExec if supportsWrite(a.write) =>
      OmniIcebergAppendDataExec(a)
    case other =>
      other
  }
}

/** ReplaceData write: replaces ReplaceDataExec with OmniIcebergReplaceDataExec. */
case class OffloadIcebergReplaceData() extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan =
    if (!OffloadWrite.nativeWriterEnabled) plan else plan match {
    case p if FallbackTags.nonEmpty(p) => p
    case r: ReplaceDataExec if supportsWrite(r.write) =>
      OmniIcebergReplaceDataExec(r)
    case other => other
  }
}

/** Overwrite write: replaces OverwriteByExpressionExec with OmniIcebergOverwriteByExpressionExec. */
case class OffloadIcebergOverwrite() extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan =
    if (!OffloadWrite.nativeWriterEnabled) plan else plan match {
    case p if FallbackTags.nonEmpty(p) => p
    case r: OverwriteByExpressionExec if supportsWrite(r.write) =>
      OmniIcebergOverwriteByExpressionExec(r)
    case other => other
  }
}

/** Dynamic overwrite partitions: replaces OverwritePartitionsDynamicExec with OmniIcebergOverwritePartitionsDynamicExec. */
case class OffloadIcebergOverwritePartitionsDynamic() extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan =
    if (!OffloadWrite.nativeWriterEnabled) plan else plan match {
    case p if FallbackTags.nonEmpty(p) => p
    case r: OverwritePartitionsDynamicExec if supportsWrite(r.write) =>
      OmniIcebergOverwritePartitionsDynamicExec(r)
    case other => other
  }
}

/** WriteToDataSourceV2: replaces with OmniIcebergWriteToDataSourceV2Exec only when underlying is Iceberg streaming/micro-batch write. */
case class OffloadIcebergWriteToDataSourceV2() extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan =
    if (!OffloadWrite.nativeWriterEnabled) plan else plan match {
    case p if FallbackTags.nonEmpty(p) => p
    case r: WriteToDataSourceV2Exec =>
      OmniIcebergWriteToDataSourceV2Exec(r).getOrElse(r)
    case other => other
  }
}

/** Collection of Iceberg write offload rules for Omni rule injection (loaded via IcebergOffloadRegistry reflection). */
object OffloadIcebergWrite {
  def offloads: Seq[OffloadSingleNode] = Seq(
    OffloadIcebergAppend(),
    OffloadIcebergReplaceData(),
    OffloadIcebergOverwrite(),
    OffloadIcebergOverwritePartitionsDynamic(),
    OffloadIcebergWriteToDataSourceV2()
  )
}
