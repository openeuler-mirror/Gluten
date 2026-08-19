/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.extension.columnar.offload

import org.apache.gluten.execution.OmniPaimonScanExecTransformer
import org.apache.gluten.extension.columnar.FallbackTags

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

/** Pre-offloads Paimon V2 scans before generic BatchScan offload handles them. */
case class OffloadOmniPaimonScan() extends OffloadSingleNode with Logging {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case scan: BatchScanExec if OmniPaimonScanExecTransformer.supportsBatchScan(scan) =>
      logWarning(
        s"[Gluten][Paimon] OffloadOmniPaimonScan: BatchScanExec -> " +
          s"OmniPaimonScanExecTransformer, scanClass=${scan.scan.getClass.getName}")
      OmniPaimonScanExecTransformer.offload(scan)
    case other =>
      other
  }
}

object OffloadOmniPaimonScanPreRule {
  private val offload = OffloadOmniPaimonScan()

  def apply(): Rule[SparkPlan] = (plan: SparkPlan) =>
    plan.transformUp {
      case node if hasUnsupportedPaimonMetadata(node) =>
        FallbackTags.add(node, "Unsupported Paimon metadata columns use Spark native scan")
        node
      case node =>
        offload.offload(node)
  }

  private def hasUnsupportedPaimonMetadata(plan: SparkPlan): Boolean = {
    OmniPaimonScanExecTransformer.hasUnsupportedPaimonMetadataColumns(plan.output) ||
      plan.expressions.exists { expression =>
        OmniPaimonScanExecTransformer.hasUnsupportedPaimonMetadataColumns(expression.references.toSeq)
      }
  }
}
