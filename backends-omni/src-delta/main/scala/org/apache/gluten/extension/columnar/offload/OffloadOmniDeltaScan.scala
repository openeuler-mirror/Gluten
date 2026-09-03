/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.extension.columnar.offload

import org.apache.gluten.execution.OmniDeltaScanExecTransformer
import org.apache.gluten.extension.columnar.FallbackTags

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}

/**
 * Converts Spark Delta data scans to [[OmniDeltaScanExecTransformer]] before generic scan offload.
 *
 * Running this rule first lets Delta Parquet scans use Delta-specific validation and copy logic
 * instead of being handled as ordinary file source scans.
 */
case class OffloadOmniDeltaScan() extends OffloadSingleNode with Logging {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case p if FallbackTags.nonEmpty(p) =>
      p
    case scan: FileSourceScanExec if OmniDeltaScanExecTransformer.isDeltaTableScan(scan) =>
      logWarning(
        s"[Gluten][Delta] OffloadOmniDeltaScan: FileSourceScanExec -> " +
          s"OmniDeltaScanExecTransformer, " +
          s"table=${scan.tableIdentifier.map(_.unquotedString).getOrElse("<unknown>")}, " +
          s"formatClass=${scan.relation.fileFormat.getClass.getName}, " +
          s"locationClass=${scan.relation.location.getClass.getName}")
      OmniDeltaScanExecTransformer(scan)
    case other =>
      other
  }
}

/** Exposes [[OffloadOmniDeltaScan]] as a Spark plan rule for Delta pre-offload registration. */
object OffloadOmniDeltaScanPreRule extends Logging {
  private val offload = OffloadOmniDeltaScan()

  def apply(): Rule[SparkPlan] = (plan: SparkPlan) =>
    plan.transformUp {
      case node =>
        offload.offload(node)
    }
}
