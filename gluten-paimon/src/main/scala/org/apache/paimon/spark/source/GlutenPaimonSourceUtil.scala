/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.paimon.spark.source

import org.apache.gluten.substrait.rel.{LocalFilesBuilder, LocalFilesNode, SplitInfo}
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.paimon.data.InternalRow

import org.apache.spark.sql.catalyst.util.DateFormatter
import org.apache.spark.sql.connector.read.{InputPartition, Scan}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import java.util.{Collections, Optional}

import scala.collection.JavaConverters._
import scala.util.Try

object GlutenPaimonSourceUtil extends Logging {
  private val PaimonScanClass = "org.apache.paimon.spark.PaimonScan"
  private val PaimonSplitScanClass = "org.apache.paimon.spark.read.PaimonSplitScan"
  // Native IsNullPartitionMarker maps this to SQL NULL. Empty / whitespace must not use it.
  private val PaimonNullPartition = "__DEFAULT_PARTITION__"

  def supportsScan(scan: Scan): Boolean = {
    val name = scan.getClass.getName
    name == PaimonScanClass || name == PaimonSplitScanClass || name.contains(".paimon.")
  }

  def getFileFormat(scan: Scan): ReadFileFormat = {
    val configured = tableOptions(scan)
      .get("file.format")
      .orElse(tableOptions(scan).get("format"))
      .map(_.toLowerCase)

    configured match {
      case Some("orc") => ReadFileFormat.OrcReadFormat
      case Some("parquet") | None => ReadFileFormat.ParquetReadFormat
      case Some(format) =>
        throw new UnsupportedOperationException(s"Unsupported Paimon file format: $format")
    }
  }

  def getReadPartitionSchema(scan: Scan): StructType = {
    val partitionKeys = tablePartitionKeys(scan).toSet
    if (partitionKeys.isEmpty) {
      new StructType()
    } else {
      StructType(readSchema(scan).fields.filter(field => partitionKeys.contains(field.name)))
    }
  }

  def getReadDataSchema(scan: Scan): StructType = {
    val partitionKeys = tablePartitionKeys(scan).toSet
    StructType(readSchema(scan).fields.filterNot(field => partitionKeys.contains(field.name)))
  }

  def genSplitInfo(
      partitions: Seq[InputPartition],
      index: Int,
      partitionSchema: StructType,
      scanFileFormat: ReadFileFormat): SplitInfo = {
    val files = partitions.flatMap(toPaimonFiles(_, partitionSchema, scanFileFormat))
    if (files.isEmpty) {
      val partitionClasses = partitions.map(_.getClass.getName).mkString(",")
      throw new UnsupportedOperationException(
        s"Cannot extract data files from Paimon input partitions: $partitionClasses")
    } else {
      LocalFilesBuilder.makeLocalFiles(
        index,
        files.map(_.path).asJava,
        files.map(_.start).map(Long.box).asJava,
        files.map(_.length).map(Long.box).asJava,
        files.map(_.fileSize).map(Long.box).asJava,
        files.map(_.modificationTime).map(Long.box).asJava,
        files.map(_.partitionValues.asJava).asJava,
        files.map(_ => Collections.emptyMap[String, String]()).asJava,
        scanFileFormat,
        files.flatMap(_.preferredLocations).distinct.asJava,
        Collections.emptyMap[String, String]())
    }
  }

  private def toPaimonFiles(
      inputPartition: InputPartition,
      partitionSchema: StructType,
      scanFileFormat: ReadFileFormat): Seq[PaimonFile] = {
    val splits = invokeAny(inputPartition, Seq("splits", "inputSplits"))
      .flatMap(asSeq)
      .getOrElse(
        Seq(
          invokeAny(inputPartition, Seq("split", "getSplit"))
            .getOrElse(inputPartition)))

    splits.flatMap(splitToPaimonFiles(_, inputPartition, partitionSchema, scanFileFormat))
  }

  private def splitToPaimonFiles(
      split: Any,
      inputPartition: InputPartition,
      partitionSchema: StructType,
      scanFileFormat: ReadFileFormat): Seq[PaimonFile] = {
    val splitPartition = partitionValuesFromRow(invokeAny(split, Seq("partition")), partitionSchema)
    val rawFiles = invokeAny(split, Seq("convertToRawFiles"))
      .flatMap(optionalValue)
      .flatMap(asSeq)
      .getOrElse(Seq.empty)

    if (rawFiles.nonEmpty) {
      rawFiles.map(rawFileToPaimonFile(_, partitionSchema, scanFileFormat, splitPartition))
    } else {
      val bucketPath = invokeAny(split, Seq("bucketPath")).map(_.toString)
      val beforeFiles = invokeAny(split, Seq("beforeFiles"))
        .flatMap(asSeq)
        .getOrElse(Seq.empty)
      val dataFiles = invokeAny(split, Seq("dataFiles"))
        .flatMap(asSeq)
        .getOrElse(Seq.empty)
      val allFiles = beforeFiles ++ dataFiles
      if (allFiles.isEmpty) {
        logWarning(
          s"[Gluten][PaimonRead] No raw/data files extracted from splitClass=" +
            s"${split.getClass.getName}, partitionClass=${inputPartition.getClass.getName}")
      }
      allFiles.map(
        dataFileToPaimonFile(_, bucketPath, partitionSchema, scanFileFormat, splitPartition))
    }
  }

  private def rawFileToPaimonFile(
      rawFile: Any,
      partitionSchema: StructType,
      scanFileFormat: ReadFileFormat,
      splitPartition: Map[String, String]): PaimonFile = {
    val path = invokeAny(rawFile, Seq("path", "filePath", "getPath"))
      .map(_.toString)
      .getOrElse(throw new UnsupportedOperationException("Cannot extract Paimon raw file path"))
    val nativePath = normalizeNativePath(path)
    val length = invokeLong(rawFile, Seq("length", "fileSize", "getLength")).getOrElse(0L)
    val format = invokeAny(rawFile, Seq("format", "fileFormat"))
      .map(formatFromString)
      .getOrElse(scanFileFormat)
    PaimonFile(
      path = nativePath,
      start = invokeLong(rawFile, Seq("start", "offset", "getStart")).getOrElse(0L),
      length = length,
      fileSize = length,
      modificationTime = invokeLong(rawFile, Seq("modificationTime", "mtime")).getOrElse(0L),
      partitionValues = resolvePartitionValues(splitPartition, nativePath, partitionSchema) ++
        paimonMetadataValues(path, bucketFromPath(path)),
      fileFormat = format)
  }

  private def dataFileToPaimonFile(
      dataFile: Any,
      bucketPath: Option[String],
      partitionSchema: StructType,
      scanFileFormat: ReadFileFormat,
      splitPartition: Map[String, String]): PaimonFile = {
    val fileName = invokeAny(dataFile, Seq("fileName", "name", "path"))
      .map(_.toString)
      .getOrElse(throw new UnsupportedOperationException("Cannot extract Paimon data file name"))
    val externalPath = invokeAny(dataFile, Seq("externalPath")).flatMap(optionalValue).map(_.toString)
    val path = externalPath.getOrElse(bucketPath.map(appendPath(_, fileName)).getOrElse(fileName))
    val nativePath = normalizeNativePath(path)
    val fileSize = invokeLong(dataFile, Seq("fileSize", "length")).getOrElse(0L)
    PaimonFile(
      path = nativePath,
      start = 0L,
      length = fileSize,
      fileSize = fileSize,
      modificationTime = invokeLong(dataFile, Seq("creationTimeEpochMillis")).getOrElse(0L),
      partitionValues = resolvePartitionValues(splitPartition, nativePath, partitionSchema) ++
        paimonMetadataValues(
          path,
          bucketFromPath(path).orElse(invokeLong(dataFile, Seq("bucket")).map(_.toInt))),
      fileFormat = invokeAny(dataFile, Seq("fileFormat"))
        .map(formatFromString)
        .getOrElse(scanFileFormat))
  }

  private def tableOptions(scan: Scan): Map[String, String] = {
    invokeAny(scan, Seq("table"))
      .flatMap(
        table =>
          invokeAny(table, Seq("options"))
            .orElse(invokeAny(table, Seq("coreOptions")).flatMap(invokeAny(_, Seq("toMap")))))
      .flatMap(asStringMap)
      .getOrElse(Map.empty)
  }

  private def tablePartitionKeys(scan: Scan): Seq[String] = {
    invokeAny(scan, Seq("table"))
      .flatMap(table => invokeAny(table, Seq("partitionKeys")).flatMap(asSeq))
      .map(_.map(_.toString))
      .getOrElse(Seq.empty)
  }

  private def readSchema(scan: Scan): StructType = {
    firstStructType(scan, Seq("readSchema", "requiredSchema")).getOrElse(new StructType())
  }

  private def firstStructType(target: Any, methodNames: Seq[String]): Option[StructType] = {
    methodNames.view.flatMap(name => invokeAny(target, Seq(name)).collect { case s: StructType => s }).headOption
  }

  private def invokeAny(target: Any, methodNames: Seq[String]): Option[Any] = {
    methodNames.view.flatMap(name => invokeMethod(target, name).orElse(readField(target, name))).headOption
  }

  private def invokeMethod(target: Any, name: String): Option[Any] = {
    Try {
      val method = target.getClass.getMethod(name)
      method.setAccessible(true)
      method.invoke(target)
    }.toOption
  }

  private def readField(target: Any, name: String): Option[Any] = {
    var cls = target.getClass
    while (cls != null) {
      try {
        val field = cls.getDeclaredField(name)
        field.setAccessible(true)
        return Option(field.get(target))
      } catch {
        case _: NoSuchFieldException =>
          cls = cls.getSuperclass
      }
    }
    None
  }

  private def invokeLong(target: Any, methodNames: Seq[String]): Option[Long] = {
    invokeAny(target, methodNames).flatMap {
      case n: java.lang.Number => Some(n.longValue())
      case s: String => Try(s.toLong).toOption
      case _ => None
    }
  }

  private def optionalValue(value: Any): Option[Any] = value match {
    case null => None
    case optional: Optional[_] => if (optional.isPresent) Some(optional.get()) else None
    case other => Some(other)
  }

  private def asSeq(value: Any): Option[Seq[Any]] = value match {
    case null => None
    case array: Array[_] => Some(array.toSeq)
    case iterable: java.lang.Iterable[_] => Some(iterable.asScala.toSeq)
    case seq: Seq[_] => Some(seq)
    case _ => None
  }

  private def asStringMap(value: Any): Option[Map[String, String]] = value match {
    case map: java.util.Map[_, _] =>
      Some(map.asScala.map { case (k, v) => k.toString -> v.toString }.toMap)
    case map: Map[_, _] =>
      Some(map.map { case (k, v) => k.toString -> v.toString })
    case _ => None
  }

  /**
   * Prefer DataSplit.partition BinaryRow over path segments. Paimon stores null / empty / space
   * as distinct BinaryRow values but generatePartValues collapses them all to
   * `__DEFAULT_PARTITION__` on disk. Parsing the path would make Omni SELECT return NULL for
   * empty-string and whitespace partitions (Spark still distinguishes them).
   */
  private def resolvePartitionValues(
      fromRow: Map[String, String],
      path: String,
      partitionSchema: StructType): Map[String, String] = {
    if (partitionSchema.isEmpty) {
      Map.empty
    } else if (partitionSchema.fieldNames.forall(fromRow.contains)) {
      fromRow
    } else {
      partitionValuesFromPath(path, partitionSchema)
    }
  }

  private def partitionValuesFromRow(
      partitionRow: Option[Any],
      partitionSchema: StructType): Map[String, String] = {
    if (partitionSchema.isEmpty) {
      Map.empty
    } else {
      partitionRow
        .collect { case row: InternalRow if row.getFieldCount == partitionSchema.size =>
          partitionSchema.fields.zipWithIndex.map {
            case (field, i) => field.name -> paimonFieldToPartitionString(row, i, field.dataType)
          }.toMap
        }
        .getOrElse(Map.empty)
    }
  }

  private def paimonFieldToPartitionString(
      row: InternalRow,
      index: Int,
      dataType: DataType): String = {
    if (row.isNullAt(index)) {
      PaimonNullPartition
    } else {
      dataType match {
        case _: StringType | _: CharType | _: VarcharType =>
          val s = row.getString(index)
          if (s == null) PaimonNullPartition else s.toString
        case _: BooleanType => String.valueOf(row.getBoolean(index))
        case _: ByteType => String.valueOf(row.getByte(index))
        case _: ShortType => String.valueOf(row.getShort(index))
        case _: IntegerType => String.valueOf(row.getInt(index))
        case _: LongType => String.valueOf(row.getLong(index))
        case _: FloatType => String.valueOf(row.getFloat(index))
        case _: DoubleType => String.valueOf(row.getDouble(index))
        case _: DateType => DateFormatter.apply().format(row.getInt(index))
        case TimestampType | TimestampNTZType =>
          val ts = row.getTimestamp(index, 6)
          if (ts == null) PaimonNullPartition
          else toOmniTimestampPartitionValue(ts.toString)
        case d: DecimalType =>
          val dec = row.getDecimal(index, d.precision, d.scale)
          if (dec == null) PaimonNullPartition else dec.toBigDecimal.toPlainString
        case _ =>
          Try(Option(row.getString(index)).map(_.toString)).toOption.flatten
            .getOrElse(PaimonNullPartition)
      }
    }
  }

  private def partitionValuesFromPath(path: String, partitionSchema: StructType): Map[String, String] = {
    if (partitionSchema.isEmpty) {
      Map.empty
    } else {
      val pairs = path
        .split("[/\\\\]")
        .flatMap {
          segment =>
            val idx = segment.indexOf('=')
            if (idx > 0) {
              Some(segment.substring(0, idx) -> unescapePathValue(segment.substring(idx + 1)))
            } else {
              None
            }
        }
        .toMap
      partitionSchema.fields.flatMap { field =>
        pairs.get(field.name).map(value => field.name -> toNativePartitionValue(value, field.dataType))
      }.toMap
    }
  }

  /**
   * Paimon partition paths use Timestamp.toString() / LocalDateTime ISO-8601
   * (e.g. 2024-10-10T00:00). Omni Hive SplitReader.StringToTimestamp only
   * accepts yyyy-MM-dd HH:mm:ss (space separator, seconds required, timegm).
   */
  private def toNativePartitionValue(value: String, dataType: DataType): String = {
    dataType match {
      case TimestampType | TimestampNTZType => toOmniTimestampPartitionValue(value)
      case _ => value
    }
  }

  private def toOmniTimestampPartitionValue(value: String): String = {
    if (value == null || value.isEmpty || value == "__DEFAULT_PARTITION__") {
      value
    } else {
      val wall = value.replace('T', ' ')
      val space = wall.indexOf(' ')
      if (space < 0) {
        if (wall.length >= 10) wall.substring(0, 10) + " 00:00:00" else wall
      } else {
        val timePart = {
          val raw = wall.substring(space + 1)
          val dot = raw.indexOf('.')
          if (dot >= 0) raw.substring(0, dot) else raw
        }
        val fields = timePart.split(':')
        val hhmmss =
          if (fields.length >= 3) s"${fields(0)}:${fields(1)}:${fields(2)}"
          else if (fields.length == 2) s"${fields(0)}:${fields(1)}:00"
          else if (fields.length == 1 && fields(0).nonEmpty) s"${fields(0)}:00:00"
          else "00:00:00"
        wall.substring(0, space) + " " + hhmmss
      }
    }
  }

  private def appendPath(parent: String, child: String): String = {
    if (parent.endsWith("/") || parent.endsWith("\\")) parent + child else parent + "/" + child
  }

  private def bucketFromPath(path: String): Option[Int] = {
    """(?:^|[/\\])bucket-(\d+)(?:[/\\]|$)""".r.findFirstMatchIn(path).map(_.group(1).toInt)
  }

  private def paimonMetadataValues(path: String, bucket: Option[Int]): Map[String, String] = {
    Map("__paimon_file_path" -> path) ++
      bucket.map(value => "__paimon_bucket" -> value.toString)
  }

  /**
   * Native Omni Hive reader requires scheme://authority/path (stringToUriInfo
   * looks for "://"). Paimon/Hadoop often emit hdfs:/path (no authority).
   * Do not use java.net.URI: CHAR(n) partition dirs contain trailing spaces and
   * URI parse fails, which previously skipped authority injection and caused
   * native "invalid scheme".
   */
  private def normalizeNativePath(path: String): String = {
    escapeHdfsPathColons(ensureHdfsAuthority(path))
  }

  private def ensureHdfsAuthority(path: String): String = {
    if (path.contains("://")) {
      path
    } else {
      val schemeEnd = path.indexOf(":/")
      if (schemeEnd > 0 && !path.substring(0, schemeEnd).contains("/")) {
        val scheme = path.substring(0, schemeEnd)
        val rest = path.substring(schemeEnd + 1)
        prependDefaultFs(scheme, rest).getOrElse(path)
      } else if (path.startsWith("/")) {
        defaultFsFromSpark()
          .map { fs =>
            val base = if (fs.endsWith("/")) fs.substring(0, fs.length - 1) else fs
            base + path
          }
          .getOrElse(path)
      } else {
        path
      }
    }
  }

  private def prependDefaultFs(scheme: String, absolutePath: String): Option[String] = {
    defaultFsFromSpark()
      .filter(_.toLowerCase.startsWith(scheme.toLowerCase + "://"))
      .map { fs =>
        val base = if (fs.endsWith("/")) fs.substring(0, fs.length - 1) else fs
        val suffix = if (absolutePath.startsWith("/")) absolutePath else "/" + absolutePath
        base + suffix
      }
  }

  private def dfsPathStart(path: String): Int = {
    val schemeAuth = path.indexOf("://")
    if (schemeAuth >= 0) {
      path.indexOf('/', schemeAuth + 3)
    } else {
      val schemeOnly = path.indexOf(":/")
      if (schemeOnly >= 0 && !path.substring(0, schemeOnly).contains("/")) {
        schemeOnly + 1
      } else if (path.startsWith("/")) {
        0
      } else {
        -1
      }
    }
  }

  private def escapeHdfsPathColons(path: String): String = {
    val pathStart = dfsPathStart(path)
    if (pathStart < 0) {
      path
    } else {
      val dfsPath = path.substring(pathStart)
      if (!dfsPath.contains(":")) {
        path
      } else {
        path.substring(0, pathStart) +
          dfsPath.split("/", -1).map(escapeColonInSegment).mkString("/")
      }
    }
  }

  private def escapeColonInSegment(segment: String): String = {
    if (segment.indexOf(':') < 0) {
      segment
    } else {
      val eq = segment.indexOf('=')
      if (eq > 0) {
        segment.substring(0, eq + 1) + segment.substring(eq + 1).replace(":", "%3A")
      } else {
        segment.replace(":", "%3A")
      }
    }
  }

  private def unescapePathValue(value: String): String = {
    if (value.indexOf('%') < 0) {
      value
    } else {
      value.replace("%3A", ":").replace("%3a", ":")
    }
  }

  private def defaultFsFromSpark(): Option[String] = {
    val spark = SparkSession.active
    spark.sparkContext.getConf.getOption("spark.hadoop.fs.defaultFS")
      .orElse(spark.conf.getOption("spark.hadoop.fs.defaultFS"))
      .orElse {
        Try {
          val method = spark.sparkContext.getClass.getMethod("hadoopConfiguration")
          val conf = method.invoke(spark.sparkContext)
          val get = conf.getClass.getMethod("get", classOf[String])
          Option(get.invoke(conf, "fs.defaultFS").asInstanceOf[String])
        }.toOption.flatten
      }
      .filter(_.nonEmpty)
  }

  private def formatFromString(value: Any): LocalFilesNode.ReadFileFormat = {
    Option(value).map(_.toString.toLowerCase).getOrElse("") match {
      case format if format.contains("orc") => ReadFileFormat.OrcReadFormat
      case format if format.contains("parquet") => ReadFileFormat.ParquetReadFormat
      case format => throw new UnsupportedOperationException(s"Unsupported Paimon file format: $format")
    }
  }

  private case class PaimonFile(
      path: String,
      start: Long,
      length: Long,
      fileSize: Long,
      modificationTime: Long,
      partitionValues: Map[String, String],
      fileFormat: ReadFileFormat,
      preferredLocations: Seq[String] = Seq.empty)
}
