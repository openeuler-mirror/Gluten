/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.backendsapi.omni

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.gluten.connector.write.PaimonFileInfoJson

import org.apache.paimon.data.BinaryRow
import org.apache.paimon.data.serializer.InternalRowSerializer
import org.apache.paimon.fs.Path
import org.apache.paimon.io.{CompactIncrement, DataIncrement}
import org.apache.paimon.migrate.FileMetaUtils
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.{CommitMessage, CommitMessageImpl}
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.InternalRowPartitionComputer

import org.apache.spark.sql.connector.write.WriterCommitMessage

import java.util

import scala.collection.mutable
import scala.collection.JavaConverters._

/** Builds Paimon Spark writer commit messages from Omni-written file metadata. */
object PaimonCommitMessageBuilder {
  private val mapper = new ObjectMapper()
  // Must match Paimon CoreOptions partition.default-name (default: __DEFAULT_PARTITION__).
  private val DefaultPartitionName = "__DEFAULT_PARTITION__"

  /** Task-side message: only JSON metadata crosses executor -> driver boundary. */
  def packageTaskCommitMessage(fileInfoJsonArray: Array[String]): WriterCommitMessage = {
    OmniPaimonWriterCommitMessage(
      if (fileInfoJsonArray == null) Array.empty[String] else fileInfoJsonArray)
  }

  def mergeCommitMessages(
      messages: Seq[WriterCommitMessage],
      table: FileStoreTable,
      format: String): util.List[CommitMessage] = {
    messages.flatMap(taskCommitMessages(_, table, format)).asJava
  }

  def groupCommitMessagesByPartition(
      messages: Seq[WriterCommitMessage],
      table: FileStoreTable,
      format: String): Seq[(Seq[String], util.List[CommitMessage])] = {
    val groups = new util.LinkedHashMap[Seq[String], util.ArrayList[CommitMessage]]()
    messages.foreach {
      case null =>
      case message: OmniPaimonWriterCommitMessage =>
        val built = buildCommitMessage(message.fileInfoJson, table, format)
        built.commitMessages.zip(built.partitionValues).foreach {
          case (commitMessage, partitionValues) =>
            groups
              .computeIfAbsent(partitionValues, _ => new util.ArrayList[CommitMessage]())
              .add(commitMessage)
        }
      case other =>
        throw new IllegalStateException(
          "Unsupported Paimon writer commit message: " + other.getClass.getName)
    }
    groups.asScala.toSeq.map { case (partitionValues, commitMessages) =>
      partitionValues -> commitMessages.asInstanceOf[util.List[CommitMessage]]
    }
  }

  private def taskCommitMessages(
      message: WriterCommitMessage,
      table: FileStoreTable,
      format: String): Seq[CommitMessage] = {
    message match {
      case null => Nil
      case taskMessage: OmniPaimonWriterCommitMessage =>
        buildCommitMessage(taskMessage.fileInfoJson, table, format).commitMessages
      case other =>
        throw new IllegalStateException(
          "Unsupported Paimon writer commit message: " + other.getClass.getName)
    }
  }

  private def buildCommitMessage(
      fileInfoJsonArray: Array[String],
      table: FileStoreTable,
      format: String): BuiltCommitMessage = {
    if (fileInfoJsonArray == null || fileInfoJsonArray.isEmpty) {
      return BuiltCommitMessage(Seq.empty, Seq.empty)
    }

    val messagesByPartitionBucket =
      new util.LinkedHashMap[(String, Int), util.ArrayList[org.apache.paimon.io.DataFileMeta]]()
    val partitionValuesByKey = new util.HashMap[String, util.List[String]]()

    fileInfoJsonArray.foreach { json =>
      val info = mapper.readValue(json, classOf[PaimonFileInfoJson])
      val partitionKey = Option(info.getPartitionValues).map(_.asScala.mkString("\u0001")).getOrElse("")
      partitionValuesByKey.put(partitionKey, safePartitionValues(info))
      val key = (partitionKey, info.getBucket)
      val metas = messagesByPartitionBucket.computeIfAbsent(
        key,
        _ => new util.ArrayList[org.apache.paimon.io.DataFileMeta]())
      metas.add(constructDataFileMeta(info, table, format))
    }

    val partitionValues = mutable.ArrayBuffer[Seq[String]]()
    val commitMessages = messagesByPartitionBucket.asScala.toSeq.map {
      case ((partitionKey, bucket), metas) =>
        val values = partitionValuesByKey.get(partitionKey)
        partitionValues += values.asScala.toSeq
        val partition = partitionRow(table, values)
        new CommitMessageImpl(
          partition,
          bucket,
          totalBuckets(table),
          new DataIncrement(metas, util.Collections.emptyList(), util.Collections.emptyList()),
          CompactIncrement.emptyIncrement())
    }
    BuiltCommitMessage(
      commitMessages.asInstanceOf[Seq[CommitMessage]],
      partitionValues.toSeq)
  }

  private def totalBuckets(table: FileStoreTable): java.lang.Integer = {
    val numBuckets = table.schema().numBuckets()
    if (numBuckets <= 0) null else java.lang.Integer.valueOf(numBuckets)
  }

  private def constructDataFileMeta(
      info: PaimonFileInfoJson,
      table: FileStoreTable,
      format: String): org.apache.paimon.io.DataFileMeta = {
    val fileIO = table.fileIO()
    val origin = new Path(info.getPath)
    val finalDir = bucketDirectory(table, info)
    fileIO.mkdirs(finalDir)
    val rollback = new util.HashMap[Path, Path]()
    FileMetaUtils.constructFileMeta(
      format,
      fileIO.getFileStatus(origin),
      fileIO,
      table,
      finalDir,
      rollback,
      table.schema().id())
  }

  private def bucketDirectory(table: FileStoreTable, info: PaimonFileInfoJson): Path = {
    val base = table.location().toString
    val partitionSegment = table
      .partitionKeys()
      .asScala
      .zip(safePartitionValues(info).asScala)
      .map { case (name, value) => s"$name=$value" }
      .mkString("/")
    val dir =
      if (partitionSegment.isEmpty) s"$base/bucket-${info.getBucket}"
      else s"$base/$partitionSegment/bucket-${info.getBucket}"
    new Path(dir)
  }

  private def partitionRow(table: FileStoreTable, values: util.List[String]): BinaryRow = {
    if (table.partitionKeys().isEmpty) {
      BinaryRow.EMPTY_ROW
    } else {
      val partitionType = partitionRowType(table)
      val spec = new util.LinkedHashMap[String, String]()
      table.partitionKeys().asScala.zip(values.asScala).foreach {
        case (name, value) => spec.put(name, value)
      }
      val genericRow =
        InternalRowPartitionComputer.convertSpecToInternalRow(
          spec,
          partitionType,
          DefaultPartitionName)
      new InternalRowSerializer(partitionType).toBinaryRow(genericRow)
    }
  }

  private def partitionRowType(table: FileStoreTable): RowType = {
    val fieldNames = table.partitionKeys().asScala.toSet
    new RowType(table.rowType().getFields.asScala.filter(f => fieldNames.contains(f.name())).asJava)
  }

  private def safePartitionValues(info: PaimonFileInfoJson): util.List[String] = {
    Option(info.getPartitionValues).getOrElse(util.Collections.emptyList[String]())
  }

  private case class BuiltCommitMessage(
      commitMessages: Seq[CommitMessage],
      partitionValues: Seq[Seq[String]])
}

/** Serializable task commit payload; CommitMessageImpl is built on the driver. */
case class OmniPaimonWriterCommitMessage(fileInfoJson: Array[String]) extends WriterCommitMessage
