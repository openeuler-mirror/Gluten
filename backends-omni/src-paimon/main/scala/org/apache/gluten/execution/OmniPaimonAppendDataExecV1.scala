/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.execution

import org.apache.gluten.connector.write.OmniPaimonDataWriteFactory
import org.apache.gluten.backendsapi.omni.PaimonCommitMessageBuilder

import org.apache.paimon.table.FileStoreTable

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.datasources.v2.{
  DataWritingColumnarBatchSparkTask,
  DataWritingColumnarBatchSparkTaskResult
}
import org.apache.spark.sql.execution.CommandExecutionMode
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters._
import scala.language.reflectiveCalls

/**
 * Executes Paimon Spark V1 append commands through the Omni columnar file writer.
 *
 * Paimon plans Spark SQL INSERT as AppendDataExecV1 for the V1 command path. This wrapper keeps
 * Paimon's commit protocol and replaces executor-side data file writing with Omni columnar writes.
 */
case class OmniPaimonAppendDataExecV1(original: SparkPlan) extends LeafV2CommandExec {
  override def nodeName: String = "OmniPaimonAppendDataExecV1"

  override def output: Seq[org.apache.spark.sql.catalyst.expressions.Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    val spark = SparkSession.active
    val table = PaimonWriteUtil.tableFromPlan(original).getOrElse {
      throw new IllegalStateException("Cannot extract Paimon table from " + original.nodeName)
    }
    val query = PaimonWriteUtil.queryFromPlan(original).orElse {
      PaimonWriteUtil.logicalQueryFromPlan(original).map { logicalPlan =>
        spark.sessionState.executePlan(logicalPlan, CommandExecutionMode.SKIP).executedPlan
      }
    }.getOrElse {
      throw new IllegalStateException(
        "Cannot extract query from " + original.nodeName + "; " +
          PaimonWriteUtil.describePlanMembers(original))
    }
    OmniPaimonAppendDataExecV1.writeAndCommit(spark, query, table)
    PaimonWriteUtil.refreshCache(original)
    Nil
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan = {
    if (newChildren.nonEmpty) {
      throw new IllegalArgumentException("OmniPaimonAppendDataExecV1 is a leaf node")
    }
    this
  }
}

object OmniPaimonAppendDataExecV1 {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)
  private type PaimonCommit = {
    def commit(messages: java.util.List[org.apache.paimon.table.sink.CommitMessage]): Unit
    def abort(messages: java.util.List[org.apache.paimon.table.sink.CommitMessage]): Unit
    def close(): Unit
  }

  private[execution] def writeAndCommit(
      sparkSession: SparkSession,
      query: SparkPlan,
      table: FileStoreTable,
      overwrite: Boolean = false,
      commandName: String = "AppendDataExecV1 -> OmniPaimonAppendDataExecV1"): Unit = {
    if (!PaimonWriteUtil.supportsNativeColumnarTable(table)) {
      throw new UnsupportedOperationException(
        "Omni Paimon V1 write does not support this Paimon table mode")
    }
    val messages = runColumnarWriteJob(sparkSession, query, table)
    val commitMessages =
      PaimonCommitMessageBuilder.mergeCommitMessages(
        messages.toSeq,
        table,
        PaimonWriteUtil.fileFormat(table))
    // For static overwrite (Spark default partitionOverwriteMode=static, no PARTITION spec),
    // Paimon's dynamic-partition-overwrite option defaults to true, which would make the
    // no-arg withOverwrite() do per-partition dynamic overwrite instead of whole-table.
    // Mirror native WriteIntoPaimonTable by forcing the option to false for the overwrite
    // commit so withOverwrite() performs a whole-table static overwrite.
    val commitTable =
      if (overwrite) table.copy(Map("dynamic-partition-overwrite" -> "false").asJava)
      else table
    val writeBuilder = commitTable.newBatchWriteBuilder()
    val commit =
      if (overwrite) {
        writeBuilder.withOverwrite().newCommit()
      } else {
        writeBuilder.newCommit()
      }
    try {
      log.warn(
        s"[Gluten][Paimon] $commandName; " +
          s"table=${table.location()} files=${commitMessages.size}")
      commit.commit(commitMessages)
    } catch {
      case t: Throwable =>
        try {
          commit.abort(commitMessages)
        } catch {
          case abortError: Throwable => t.addSuppressed(abortError)
        }
        throw t
    } finally {
      commit.close()
    }
  }

  private[execution] def writeAndCommitDynamicOverwrite(
      sparkSession: SparkSession,
      query: SparkPlan,
      table: FileStoreTable,
      commandName: String): Unit = {
    if (!PaimonWriteUtil.supportsNativeColumnarTable(table)) {
      throw new UnsupportedOperationException(
        "Omni Paimon dynamic overwrite does not support this Paimon table mode")
    }
    val messages = runColumnarWriteJob(sparkSession, query, table)
    val groups = PaimonCommitMessageBuilder.groupCommitMessagesByPartition(
      messages.toSeq,
      table,
      PaimonWriteUtil.fileFormat(table))
    if (groups.isEmpty) {
      throw new IllegalStateException(
        "Omni Paimon dynamic overwrite received no partition commit messages.")
    }

    log.warn(
      s"[Gluten][Paimon] $commandName; " +
        s"table=${table.location()} partitions=${groups.size}")
    if (table.partitionKeys().isEmpty) {
      commitPaimonMessages(table.newBatchWriteBuilder().withOverwrite().newCommit(), groups.head._2)
    } else {
      groups.foreach {
        case (partitionValues, messagesToCommit) =>
          val staticPartition =
            table.partitionKeys().asScala.zip(partitionValues).toMap.asJava
          commitPaimonMessages(
            table.newBatchWriteBuilder().withOverwrite(staticPartition).newCommit(),
            messagesToCommit)
      }
    }
  }

  private def commitPaimonMessages(
      commit: PaimonCommit,
      commitMessages: java.util.List[org.apache.paimon.table.sink.CommitMessage]): Unit = {
    try {
      commit.commit(commitMessages)
    } catch {
      case t: Throwable =>
        try {
          commit.abort(commitMessages)
        } catch {
          case abortError: Throwable => t.addSuppressed(abortError)
        }
        throw t
    } finally {
      commit.close()
    }
  }

  private def runColumnarWriteJob(
      sparkSession: SparkSession,
      query: SparkPlan,
      table: FileStoreTable): Array[WriterCommitMessage] = {
    val writeInputPlan =
      if (query.supportsColumnar) {
        query
      } else {
        RowToOmniColumnarExec(query)
      }
    val rdd: RDD[ColumnarBatch] = {
      val out = writeInputPlan.executeColumnar()
      if (out.partitions.isEmpty) {
        sparkSession.sparkContext.parallelize(Array.empty[ColumnarBatch], 1)
      } else if (table.schema().numBuckets() == 1 && out.partitions.length > 1) {
        out.coalesce(1)
      } else {
        out
      }
    }
    val messages = new Array[WriterCommitMessage](rdd.partitions.length)
    var writtenRows = 0L
    val factory = OmniPaimonDataWriteFactory(
      writeInputPlan.schema,
      table,
      PaimonWriteUtil.fileFormat(table),
      java.util.UUID.randomUUID().toString)

    try {
      sparkSession.sparkContext.runJob(
        rdd,
        (context: TaskContext, iter: Iterator[ColumnarBatch]) => {
          var taskRows = 0L
          val countingIter = iter.map { batch =>
            taskRows += batch.numRows().toLong
            batch
          }
          val result = DataWritingColumnarBatchSparkTask.run(factory, context, countingIter, Map.empty)
          (result, taskRows)
        },
        rdd.partitions.indices,
        (index, taskResult: (DataWritingColumnarBatchSparkTaskResult, Long)) => {
          val (result, taskRows) = taskResult
          writtenRows += taskRows
          messages(index) = result.writerCommitMessage
        })
      if (writtenRows == 0L) {
        throw new IllegalStateException(
          "Omni Paimon V1 append received zero input rows; refuse to commit empty messages.")
      }
      messages.filter(_ != null)
    } catch {
      case t: Throwable =>
        throw new SparkException("Omni Paimon V1 append columnar write job failed", t)
    }
  }
}
