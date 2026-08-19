/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager

import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.write.{BatchWrite, Write}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2Exec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.streaming.sources.MicroBatchWrite

case class OmniPaimonWriteToDataSourceV2Exec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: Write,
    override val batchWrite: BatchWrite,
    writeMetrics: Seq[CustomMetric])
  extends AbstractPaimonWriteExec {

  override def nodeName: String = "OmniPaimonWriteToDataSourceV2Exec"

  override val customMetrics: Map[String, SQLMetric] = {
    writeMetrics.map { m => m.name() -> SQLMetrics.createV2CustomMetric(sparkContext, m) }.toMap ++
      BackendsApiManager.getMetricsApiInstance.genBatchWriteMetrics(sparkContext)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(query = newChild)
}

object OmniPaimonWriteToDataSourceV2Exec {
  private def extractOuterWrite(batchWrite: BatchWrite): Option[Write] = {
    batchWrite match {
      case microBatchWrite: MicroBatchWrite =>
        try {
          val streamWrite = microBatchWrite.writeSupport
          val outerClassField = streamWrite.getClass.getDeclaredField("this$0")
          outerClassField.setAccessible(true)
          outerClassField.get(streamWrite) match {
            case w: Write => Some(w)
            case _ => None
          }
        } catch {
          case _: Throwable => None
        }
      case _ => None
    }
  }

  def apply(original: WriteToDataSourceV2Exec): Option[OmniPaimonWriteToDataSourceV2Exec] = {
    extractOuterWrite(original.batchWrite)
      .filter(PaimonWriteUtil.supportsWrite)
      .orElse {
        if (PaimonWriteUtil.supportsBatchWrite(original.batchWrite)) {
          Some(new Write {
            override def toBatch: BatchWrite = original.batchWrite
            override def description(): String = original.batchWrite.toString
          })
        } else {
          None
        }
      }
      .map { w =>
        OmniPaimonWriteToDataSourceV2Exec(
          original.query,
          original.refreshCache,
          w,
          original.batchWrite,
          original.writeMetrics)
      }
  }
}
