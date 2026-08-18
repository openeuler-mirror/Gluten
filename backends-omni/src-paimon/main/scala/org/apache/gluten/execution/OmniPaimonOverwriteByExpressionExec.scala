/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.execution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.execution.CommandExecutionMode
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{
  LeafV2CommandExec,
  OverwriteByExpressionExec,
  OverwriteByExpressionExecV1
}

case class OmniPaimonOverwriteByExpressionExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: Write)
  extends AbstractPaimonWriteExec {

  override def nodeName: String = "OmniPaimonOverwriteByExpressionExec"

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(query = newChild)
}

object OmniPaimonOverwriteByExpressionExec {
  def apply(original: OverwriteByExpressionExec): OmniPaimonOverwriteByExpressionExec = {
    OmniPaimonOverwriteByExpressionExec(
      original.query,
      original.refreshCache,
      original.write)
  }
}

case class OmniPaimonOverwriteByExpressionExecV1(original: OverwriteByExpressionExecV1)
  extends LeafV2CommandExec {

  override def nodeName: String = "OmniPaimonOverwriteByExpressionExecV1"

  override def output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    val table = PaimonWriteUtil.tableFromPlan(original).getOrElse {
      throw new IllegalStateException("Cannot extract Paimon table from " + original.nodeName)
    }
    val query =
      SparkSession.active.sessionState
        .executePlan(original.plan, CommandExecutionMode.SKIP)
        .executedPlan
    OmniPaimonAppendDataExecV1.writeAndCommit(
      SparkSession.active,
      query,
      table,
      overwrite = true,
      commandName = "OverwriteByExpressionExecV1 -> OmniPaimonOverwriteByExpressionExecV1")
    original.refreshCache()
    Nil
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan = {
    if (newChildren.nonEmpty) {
      throw new IllegalArgumentException("OmniPaimonOverwriteByExpressionExecV1 is a leaf node")
    }
    this
  }
}
