/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.execution

import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.OverwritePartitionsDynamicExec

case class OmniPaimonOverwritePartitionsDynamicExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: Write)
  extends AbstractPaimonWriteExec {

  override def nodeName: String = "OmniPaimonOverwritePartitionsDynamicExec"

  override protected def run(): Seq[org.apache.spark.sql.catalyst.InternalRow] = {
    logWarning("[Gluten][Paimon] OverwritePartitionsDynamicExec -> " + nodeName)
    super.run()
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(query = newChild)
}

object OmniPaimonOverwritePartitionsDynamicExec {
  def apply(original: OverwritePartitionsDynamicExec): OmniPaimonOverwritePartitionsDynamicExec = {
    OmniPaimonOverwritePartitionsDynamicExec(
      original.query,
      original.refreshCache,
      original.write)
  }
}
