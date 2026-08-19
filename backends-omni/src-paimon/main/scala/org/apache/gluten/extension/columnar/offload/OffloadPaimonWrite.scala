/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.extension.columnar.offload

import org.apache.gluten.execution.{
  OmniPaimonAppendDataExec,
  OmniPaimonAppendDataExecV1,
  OmniPaimonDynamicPartitionOverwriteExec,
  OmniPaimonOverwriteByExpressionExec,
  OmniPaimonOverwriteByExpressionExecV1,
  OmniPaimonOverwritePartitionsDynamicExec,
  OmniPaimonWriteToDataSourceV2Exec,
  PaimonWriteUtil
}
import org.apache.gluten.extension.columnar.FallbackTags

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{
  AppendDataExec,
  OverwriteByExpressionExec,
  OverwriteByExpressionExecV1,
  OverwritePartitionsDynamicExec,
  WriteToDataSourceV2Exec
}

case class OffloadPaimonAppend() extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case p if FallbackTags.nonEmpty(p) => p
    case p if PaimonWriteUtil.isPaimonAppendDataExecV1(p) =>
      OmniPaimonAppendDataExecV1(p)
    case a: AppendDataExec if PaimonWriteUtil.supportsWrite(a.write) =>
      OmniPaimonAppendDataExec(a)
    case other => other
  }
}

case class OffloadPaimonOverwrite() extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case p if FallbackTags.nonEmpty(p) => p
    case p if PaimonWriteUtil.isPaimonDynamicPartitionOverwriteCommand(p) =>
      OmniPaimonDynamicPartitionOverwriteExec(p)
    case r: OverwriteByExpressionExecV1 if PaimonWriteUtil.supportsWrite(r.write) =>
      OmniPaimonOverwriteByExpressionExecV1(r)
    case r: OverwriteByExpressionExec if PaimonWriteUtil.supportsWrite(r.write) =>
      OmniPaimonOverwriteByExpressionExec(r)
    case other => other
  }
}

case class OffloadPaimonOverwritePartitionsDynamic() extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case p if FallbackTags.nonEmpty(p) => p
    case r: OverwritePartitionsDynamicExec if PaimonWriteUtil.supportsWrite(r.write) =>
      OmniPaimonOverwritePartitionsDynamicExec(r)
    case other => other
  }
}

case class OffloadPaimonWriteToDataSourceV2() extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case p if FallbackTags.nonEmpty(p) => p
    case r: WriteToDataSourceV2Exec =>
      OmniPaimonWriteToDataSourceV2Exec(r).getOrElse(r)
    case other => other
  }
}

object OffloadPaimonWrite {
  def offloads: Seq[OffloadSingleNode] = Seq(
    OffloadPaimonAppend(),
    OffloadPaimonOverwrite(),
    OffloadPaimonOverwritePartitionsDynamic(),
    OffloadPaimonWriteToDataSourceV2())
}
