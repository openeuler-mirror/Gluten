/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.execution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{CommandExecutionMode, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec

case class OmniPaimonDynamicPartitionOverwriteExec(original: SparkPlan)
  extends LeafV2CommandExec {

  override def nodeName: String = "OmniPaimonDynamicPartitionOverwriteExec"

  override def output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    val spark = SparkSession.active
    val command = PaimonWriteUtil.dynamicPartitionOverwriteCommand(original).getOrElse(original)
    val table = PaimonWriteUtil.tableFromPlan(original).getOrElse {
      throw new IllegalStateException("Cannot extract Paimon table from " + original.nodeName)
    }
    val query = PaimonWriteUtil.queryFromPlan(original).orElse {
      PaimonWriteUtil.logicalQueryFromAny(command).map { logicalPlan =>
        spark.sessionState.executePlan(logicalPlan, CommandExecutionMode.SKIP).executedPlan
      }
    }.getOrElse {
      throw new IllegalStateException(
        "Cannot extract query from " + original.nodeName + "; " +
          PaimonWriteUtil.describePlanMembers(original))
    }

    OmniPaimonAppendDataExecV1.writeAndCommitDynamicOverwrite(
      spark,
      query,
      table,
      "PaimonDynamicPartitionOverwriteCommand -> OmniPaimonDynamicPartitionOverwriteExec")
    PaimonWriteUtil.refreshCache(original)
    Nil
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan = {
    if (newChildren.nonEmpty) {
      throw new IllegalArgumentException("OmniPaimonDynamicPartitionOverwriteExec is a leaf node")
    }
    this
  }
}
