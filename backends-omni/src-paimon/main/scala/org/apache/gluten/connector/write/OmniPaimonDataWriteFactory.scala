/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.connector.write

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.backendsapi.omni.PaimonCommitMessageBuilder
import org.apache.gluten.execution.{PaimonWriteJniWrapper, PaimonWriteUtil}
import org.apache.gluten.expression.OmniExpressionAdaptor.perBatchColumnOmniTypeIds
import org.apache.gluten.runtime.OmniRuntimes

import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters._

case class OmniPaimonDataWriteFactory(
    schema: StructType,
    table: FileStoreTable,
    format: String,
    queryId: String)
  extends ColumnarBatchDataWriterFactory
  with ColumnarStreamingDataWriterFactory {

  // Resolved on driver when the factory is created; executors only read the string field.
  private val normalizedTableLocation: String =
    normalizeHdfsPath(table.location().toString)

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[ColumnarBatch] =
    createWriter(partitionId, taskId, 0L)

  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[ColumnarBatch] =
    OmniPaimonColumnarBatchDataWriter(
      createJniWrapper(partitionId, taskId, epochId),
      table,
      format)

  private def createJniWrapper(
      partitionId: Int,
      taskId: Long,
      epochId: Long): PaimonWriteJniWrapper = {
    val operationId = if (epochId == 0L) queryId else queryId + "-" + epochId
    val omniTypes = perBatchColumnOmniTypeIds(schema)
    val runtime = OmniRuntimes.contextInstance(
      BackendsApiManager.getBackendName,
      "PaimonWrite#write")
    val wrapper = new PaimonWriteJniWrapper(runtime)
    val params = new PaimonWriteJniWrapper.PaimonWriterInitParams(
      fileFormatId(format),
      stagingDirectory(operationId),
      partitionId,
      taskId,
      operationId,
      SQLConf.get.getConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE) == "LEGACY",
      table.partitionKeys().asScala.map(_.toString).toArray,
      PaimonWriteUtil.bucketKeys(table).toArray,
      PaimonWriteUtil.numBuckets(table),
      PaimonWriteUtil.hiddenBucketColumnIndex(table, schema),
      PaimonWriteUtil.tableFieldNames(table).size,
      Option(table.options().get("bucket-function")).getOrElse("default"))
    wrapper.init(schema, omniTypes, params)
    wrapper
  }

  private def stagingDirectory(operationId: String): String = {
    normalizedTableLocation + "/.gluten-omni-staging/" + operationId
  }

  private def normalizeHdfsPath(path: String): String = {
    if (path == null || !path.startsWith("hdfs:/") || path.startsWith("hdfs://")) {
      path
    } else {
      val hadoopConf = SparkSession.active.sparkContext.hadoopConfiguration
      Option(hadoopConf.get("fs.defaultFS"))
        .filter(_.startsWith("hdfs://"))
        .map { defaultFs =>
          val normalizedDefaultFs =
            if (defaultFs.endsWith("/")) defaultFs.substring(0, defaultFs.length - 1) else defaultFs
          normalizedDefaultFs + path.substring("hdfs:".length)
        }
        .getOrElse(path)
    }
  }

  private def fileFormatId(format: String): Int = {
    format.toLowerCase(java.util.Locale.ROOT) match {
      case "orc" => 0
      case "parquet" => 1
      case other => throw new UnsupportedOperationException("Unsupported Paimon write format: " + other)
    }
  }
}

case class OmniPaimonColumnarBatchDataWriter(
    jniWrapper: PaimonWriteJniWrapper,
    table: FileStoreTable,
    format: String)
  extends DataWriter[ColumnarBatch] {

  override def write(batch: ColumnarBatch): Unit = {
    jniWrapper.write(batch)
  }

  override def commit(): WriterCommitMessage = {
    PaimonCommitMessageBuilder.packageTaskCommitMessage(jniWrapper.commit())
  }

  override def abort(): Unit = {}

  override def close(): Unit = {}

  override def currentMetricsValues(): Array[CustomTaskMetric] = {
    val metrics = jniWrapper.metrics()
    if (metrics == null) Array.empty[CustomTaskMetric] else metrics.toCustomTaskMetrics
  }
}
