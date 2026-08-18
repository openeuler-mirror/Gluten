/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.execution

import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.AppendDataExec

case class OmniPaimonAppendDataExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: Write)
  extends AbstractPaimonWriteExec {

  override def nodeName: String = "OmniPaimonAppendDataExec"

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(query = newChild)
}

object OmniPaimonAppendDataExec {
  def apply(original: AppendDataExec): OmniPaimonAppendDataExec = {
    OmniPaimonAppendDataExec(
      original.query,
      original.refreshCache,
      original.write)
  }
}
