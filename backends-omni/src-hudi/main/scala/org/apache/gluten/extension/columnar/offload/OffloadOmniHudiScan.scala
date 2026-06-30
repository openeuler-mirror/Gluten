/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.extension.columnar.offload

import org.apache.gluten.execution.OmniHudiScanExecTransformer
import org.apache.gluten.extension.columnar.FallbackTags

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}

/** Pre-offload Hudi scan before generic [[org.apache.gluten.extension.columnar.offload.OffloadOthers]]. */
case class OffloadOmniHudiScan() extends OffloadSingleNode with Logging {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case p if FallbackTags.nonEmpty(p) =>
      p
    case scan: FileSourceScanExec if OmniHudiScanExecTransformer.isHudiTableScan(scan) =>
      logWarning(
        s"[Gluten][Hudi] OffloadOmniHudiScan: FileSourceScanExec -> OmniHudiScanExecTransformer, " +
          s"table=${scan.tableIdentifier.map(_.unquotedString).getOrElse("<unknown>")}, " +
          s"formatClass=${scan.relation.fileFormat.getClass.getName}, " +
          s"locationClass=${scan.relation.location.getClass.getName}")
      OmniHudiScanExecTransformer(scan)
    case other =>
      other
  }
}

object OffloadOmniHudiScanPreRule {
  private val offload = OffloadOmniHudiScan()

  def apply(): Rule[SparkPlan] = (plan: SparkPlan) =>
    plan.transformUp {
      case node =>
        offload.offload(node)
    }
}
