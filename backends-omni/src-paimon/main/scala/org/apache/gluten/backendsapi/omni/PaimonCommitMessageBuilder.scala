/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.backendsapi.omni

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.gluten.connector.write.PaimonFileInfoJson

import org.apache.paimon.data.{BinaryRow, BinaryRowWriter, BinaryString, Decimal, Timestamp}
import org.apache.paimon.fs.Path
import org.apache.paimon.io.{CompactIncrement, DataIncrement}
import org.apache.paimon.migrate.FileMetaUtils
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.{CommitMessage, CommitMessageImpl}
import org.apache.paimon.types.{
  BigIntType,
  BinaryType,
  BooleanType,
  CharType,
  DateType,
  DecimalType,
  DoubleType,
  FloatType,
  IntType,
  LocalZonedTimestampType,
  RowKind,
  RowType,
  SmallIntType,
  TimestampType,
  TinyIntType,
  VarBinaryType,
  VarCharType
}
import org.apache.paimon.utils.{InternalRowPartitionComputer, TypeUtils}

import org.apache.spark.sql.connector.write.WriterCommitMessage

import java.nio.charset.StandardCharsets
import java.time.LocalDate
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
    val fileInfoJson = messages.flatMap {
      case null => Nil
      case taskMessage: OmniPaimonWriterCommitMessage =>
        Option(taskMessage.fileInfoJson).map(_.toSeq).getOrElse(Nil)
      case other =>
        throw new IllegalStateException(
          "Unsupported Paimon writer commit message: " + other.getClass.getName)
    }.toArray
    buildCommitMessage(fileInfoJson, table, format).commitMessages.asJava
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

  private def buildCommitMessage(
      fileInfoJsonArray: Array[String],
      table: FileStoreTable,
      format: String): BuiltCommitMessage = {
    if (fileInfoJsonArray == null || fileInfoJsonArray.isEmpty) {
      return BuiltCommitMessage(Seq.empty, Seq.empty)
    }

    val messagesByPartitionBucket =
      new util.LinkedHashMap[(String, Int), util.ArrayList[org.apache.paimon.io.DataFileMeta]]()
    val partitionByKey = new util.HashMap[String, BinaryRow]()

    fileInfoJsonArray.foreach { json =>
      val info = mapper.readValue(json, classOf[PaimonFileInfoJson])
      val values = safePartitionValues(info)
      val partition = partitionRow(table, values)
      val partitionKey = values.asScala.mkString("\u0001")
      partitionByKey.put(partitionKey, partition)
      val key = (partitionKey, info.getBucket)
      val metas = messagesByPartitionBucket.computeIfAbsent(
        key,
        _ => new util.ArrayList[org.apache.paimon.io.DataFileMeta]())
      metas.add(constructDataFileMeta(info, table, format, partition))
    }

    val partitionValues = mutable.ArrayBuffer[Seq[String]]()
    val commitMessages = messagesByPartitionBucket.asScala.toSeq.map {
      case ((partitionKey, bucket), metas) =>
        val partition = partitionByKey.get(partitionKey)
        partitionValues += partitionSpecValues(table, partition)
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
      format: String,
      partition: BinaryRow): org.apache.paimon.io.DataFileMeta = {
    val fileIO = table.fileIO()
    val origin = new Path(info.getPath)
    // Same as native Spark-Paimon: FileStorePathFactory formats + escapes partition segments.
    val finalDir = table.store().pathFactory().bucketPath(partition, info.getBucket)
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

  /**
   * Build a typed partition BinaryRow the same way native Spark-Paimon does (Spark InternalRow
   * micros/days -> Paimon Timestamp/Date). Do not use convertSpecToInternalRow: Omni writer
   * serializes TIMESTAMP/DATE as epoch numbers, which Paimon string-cast cannot parse.
   */
  private def partitionRow(table: FileStoreTable, values: util.List[String]): BinaryRow = {
    if (table.partitionKeys().isEmpty) {
      BinaryRow.EMPTY_ROW
    } else {
      val partitionType = partitionRowType(table)
      val row = new BinaryRow(partitionType.getFieldCount)
      val writer = new BinaryRowWriter(row)
      writer.reset()
      writer.writeRowKind(RowKind.INSERT)
      partitionType.getFields.asScala.zipWithIndex.foreach {
        case (field, i) =>
          val value = if (i < values.size()) values.get(i) else null
          writePartitionField(writer, i, field.`type`(), value)
      }
      writer.complete()
      row
    }
  }

  private def writePartitionField(
      writer: BinaryRowWriter,
      pos: Int,
      dataType: org.apache.paimon.types.DataType,
      value: String): Unit = {
    if (value == null || value == DefaultPartitionName) {
      writer.setNullAt(pos)
      return
    }
    dataType match {
      case _: BooleanType =>
        writer.writeBoolean(pos, java.lang.Boolean.parseBoolean(value))
      case _: TinyIntType =>
        writer.writeByte(pos, java.lang.Byte.parseByte(value))
      case _: SmallIntType =>
        writer.writeShort(pos, java.lang.Short.parseShort(value))
      case _: IntType =>
        writer.writeInt(pos, java.lang.Integer.parseInt(value))
      case _: DateType =>
        writer.writeInt(pos, toDateDays(value))
      case _: BigIntType =>
        writer.writeLong(pos, java.lang.Long.parseLong(value))
      case t: TimestampType =>
        writer.writeTimestamp(pos, toPaimonTimestamp(value), t.getPrecision)
      case t: LocalZonedTimestampType =>
        writer.writeTimestamp(pos, toPaimonTimestamp(value), t.getPrecision)
      case _: FloatType =>
        writer.writeFloat(pos, java.lang.Float.parseFloat(value))
      case _: DoubleType =>
        writer.writeDouble(pos, java.lang.Double.parseDouble(value))
      case _: VarCharType | _: CharType =>
        writer.writeString(pos, BinaryString.fromString(value))
      case _: BinaryType | _: VarBinaryType =>
        PaimonBinaryRowCompat.writeBinary(writer, pos, value.getBytes(StandardCharsets.ISO_8859_1))
      case d: DecimalType =>
        writer.writeDecimal(
          pos,
          Decimal.fromBigDecimal(new java.math.BigDecimal(value), d.getPrecision, d.getScale),
          d.getPrecision)
      case other =>
        throw new UnsupportedOperationException("Unsupported Paimon partition column type: " + other)
    }
  }

  private def toPaimonTimestamp(value: String): Timestamp = {
    try {
      Timestamp.fromMicros(java.lang.Long.parseLong(value))
    } catch {
      case _: NumberFormatException =>
        TypeUtils.castFromString(value, org.apache.paimon.types.DataTypes.TIMESTAMP(6))
          .asInstanceOf[Timestamp]
    }
  }

  private def toDateDays(value: String): Int = {
    try {
      java.lang.Integer.parseInt(value)
    } catch {
      case _: NumberFormatException =>
        LocalDate.parse(value).toEpochDay.toInt
    }
  }

  /** Paimon-canonical unescaped spec strings (for withOverwrite), from typed BinaryRow. */
  private def partitionSpecValues(table: FileStoreTable, partition: BinaryRow): Seq[String] = {
    if (table.partitionKeys().isEmpty) {
      Seq.empty
    } else {
      val partitionType = partitionRowType(table)
      val computer = new InternalRowPartitionComputer(
        DefaultPartitionName,
        partitionType,
        partitionType.getFieldNames.toArray(Array.empty[String]),
        table.coreOptions().legacyPartitionName())
      val spec = computer.generatePartValues(partition)
      table.partitionKeys().asScala.map(name => spec.get(name)).toSeq
    }
  }

  private def partitionRowType(table: FileStoreTable): RowType = {
    val byName = table.rowType().getFields.asScala.map(field => field.name() -> field).toMap
    new RowType(table.partitionKeys().asScala.map(byName).asJava)
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
