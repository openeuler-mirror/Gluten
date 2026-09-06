/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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
package org.apache.gluten.datasources.text

import java.nio.charset.{Charset, StandardCharsets}
import java.util.Properties

import org.apache.gluten.extension.ValidationResult

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.serde2.`lazy`.LazySerDeParameters
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

import scala.util.{Failure, Success, Try}

object OmniTextOptionsAdapter {
  val SourceKindKey = "text_source_kind"
  val CodecKindKey = "text_codec_kind"
  val CharsetKey = "text_charset"
  val WholeTextKey = "text_whole_text"
  val LineSeparatorKey = "text_line_separator"
  val CompressionCodecKey = "text_compression_codec"
  val FileSchemaSizeKey = "text_file_schema_size"
  val FileTypeKey = "text_file_type"
  val ReadSchemaSizeKey = "text_read_schema_size"
  val ReadTypeKey = "text_read_type"
  val FileSchemaKey = "text_file_schema"
  val ReadSchemaKey = "text_read_schema"
  val ValidationErrorKey = "text_validation_error"
  val SessionTimezoneKey = "text_session_timezone"

  val SparkTextSource = "SPARK_TEXT"
  val HiveTextSource = "HIVE_TEXT"
  val RawLineCodec = "RAW_LINE"
  val LazySimpleCodec = "LAZY_SIMPLE"
  val NoCompression = "NONE"
  val LazySimpleSerdeClass = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"

  sealed trait TextDialectOptions {
    def toProperties: Map[String, String]
  }

  final case class CommonTextOptions(
      charset: String,
      lineSeparator: Option[String],
      compressionCodec: String,
      splitable: Boolean) {
    def toProperties: Map[String, String] = Map(
      CharsetKey -> charset,
      LineSeparatorKey -> lineSeparator.getOrElse(""),
      CompressionCodecKey -> compressionCodec,
      "text_splitable" -> splitable.toString)
  }

  final case class RawLineOptions(wholeText: Boolean) extends TextDialectOptions {
    override def toProperties: Map[String, String] = Map(WholeTextKey -> wholeText.toString)
  }

  final case class DelimitedOptions(
      fieldDelimiter: String,
      nullLiteral: String,
      escapeEnabled: Boolean,
      escapeChar: Option[String],
      skipInputLines: Int,
      emitHeader: Boolean) {
    def toProperties: Map[String, String] = Map(
      "field_delimiter" -> fieldDelimiter,
      "nullValue" -> nullLiteral,
      "escape" -> escapeChar.filter(_ => escapeEnabled).getOrElse(""),
      "header" -> skipInputLines.toString,
      "text_escape_enabled" -> escapeEnabled.toString,
      "text_emit_header" -> emitHeader.toString)
  }

  final case class LazySimpleOptions(
      delimited: DelimitedOptions,
      collectionDelimiter: String,
      mapKeyDelimiter: String,
      lastColumnTakesRest: Boolean)
    extends TextDialectOptions {
    override def toProperties: Map[String, String] = delimited.toProperties ++ Map(
      "text_collection_delimiter" -> collectionDelimiter,
      "text_map_key_delimiter" -> mapKeyDelimiter,
      "text_last_column_takes_rest" -> lastColumnTakesRest.toString)
  }

  // Reserved for phase three. CSV remains a distinct dialect and cannot select a codec yet.
  final case class CsvOptions(
      delimited: DelimitedOptions,
      quote: String,
      parseMode: String)
    extends TextDialectOptions {
    override def toProperties: Map[String, String] = delimited.toProperties ++ Map(
      "quote" -> quote,
      "text_parse_mode" -> parseMode)
  }

  final case class TextFormatOptions(
      sourceKind: String,
      codecKind: String,
      common: CommonTextOptions,
      dialect: TextDialectOptions) {
    def toProperties: Map[String, String] = Map(
      SourceKindKey -> sourceKind,
      CodecKindKey -> codecKind) ++ common.toProperties ++ dialect.toProperties
  }

  final case class TextSourceDescriptor(
      formatOptions: TextFormatOptions,
      fileSchema: StructType,
      readSchema: StructType) {
    def toProperties: Map[String, String] = formatOptions.toProperties ++ Map(
      FileSchemaSizeKey -> fileSchema.size.toString,
      FileTypeKey -> fileSchema.fields.headOption.map(_.dataType.catalogString).getOrElse(""),
      ReadSchemaSizeKey -> readSchema.size.toString,
      ReadTypeKey -> readSchema.fields.headOption.map(_.dataType.catalogString).getOrElse(""),
      FileSchemaKey -> fileSchema.json,
      ReadSchemaKey -> readSchema.json)
  }

  private def normalize(options: Map[String, String]): Map[String, String] =
    options.map { case (key, value) => key.toLowerCase(java.util.Locale.ROOT) -> value }

  private def byteToSingleByteUtf8(value: Byte, optionName: String): Either[String, String] = {
    val unsigned = value & 0xff
    if (unsigned > 0x7f) {
      Left(s"$optionName must be a single-byte UTF-8 character")
    } else {
      Right(unsigned.toChar.toString)
    }
  }

  private def parseNonNegativeInt(
      options: Map[String, String],
      name: String,
      defaultValue: Int): Either[String, Int] = {
    options.get(name) match {
      case None => Right(defaultValue)
      case Some(value) =>
        Try(value.toInt).toOption.filter(_ >= 0) match {
          case Some(parsed) => Right(parsed)
          case None => Left(s"$name must be a non-negative integer")
        }
    }
  }

  def fromSparkText(
      options: Map[String, String],
      fileSchema: StructType,
      readSchema: StructType): TextSourceDescriptor = {
    val normalized = normalize(options)
    val rawCharset = normalized.getOrElse("encoding", StandardCharsets.UTF_8.name())
    val charset = Try(Charset.forName(rawCharset).name()).getOrElse(rawCharset)
    val wholeText = normalized
      .get("wholetext")
      .flatMap(value => Try(value.toBoolean).toOption)
      .getOrElse(false)
    TextSourceDescriptor(
      TextFormatOptions(
        SparkTextSource,
        RawLineCodec,
        CommonTextOptions(charset, normalized.get("linesep"), NoCompression, !wholeText),
        RawLineOptions(wholeText)),
      fileSchema,
      readSchema)
  }

  def fromHiveLazySimple(
      conf: Configuration,
      tableProperties: Properties,
      fileSchema: StructType,
      readSchema: StructType): Either[String, TextSourceDescriptor] = {
    val normalized = tableProperties.stringPropertyNames().toArray(new Array[String](0)).map {
      key => key.toLowerCase(java.util.Locale.ROOT) -> tableProperties.getProperty(key)
    }.toMap

    Try(new LazySerDeParameters(conf, tableProperties, LazySimpleSerdeClass)) match {
      case Failure(error) => Left(s"failed to parse LazySimpleSerDe properties: ${error.getMessage}")
      case Success(parameters) =>
        val charsetName = normalized.getOrElse("serialization.encoding", StandardCharsets.UTF_8.name())
        val charset = Try(Charset.forName(charsetName)).toOption
        if (!charset.contains(StandardCharsets.UTF_8)) {
          return Left("only UTF-8 serialization.encoding is supported")
        }
        if (parameters.isLastColumnTakesRest) {
          return Left("serialization.last.column.takes.rest=true is not supported")
        }
        if (parameters.isExtendedBooleanLiteral) {
          return Left("hive.lazysimple.extended_boolean_literal=true is not supported")
        }
        if (Option(parameters.getTimestampFormats).exists(formats => !formats.isEmpty)) {
          return Left("custom timestamp.formats is not supported")
        }
        if (normalized.get("serialization.escape.crlf").exists(_.equalsIgnoreCase("true"))) {
          return Left("serialization.escape.crlf=true is not supported")
        }

        val header = parseNonNegativeInt(normalized, "skip.header.line.count", 0)
        val footer = parseNonNegativeInt(normalized, "skip.footer.line.count", 0)
        if (header.exists(_ != 0)) {
          return Left("skip.header.line.count is not supported by Spark Hive scan")
        }
        if (footer.exists(_ != 0)) {
          return Left("skip.footer.line.count is not supported")
        }

        val separators = parameters.getSeparators
        if (separators == null || separators.length < 3) {
          return Left("LazySimpleSerDe separators are incomplete")
        }
        val delimiter = byteToSingleByteUtf8(separators(0), "field.delim")
        val collectionDelimiter = byteToSingleByteUtf8(separators(1), "collection.delim")
        val mapKeyDelimiter = byteToSingleByteUtf8(separators(2), "mapkey.delim")
        val escape = if (parameters.isEscaped) {
          byteToSingleByteUtf8(parameters.getEscapeChar, "escape.delim").map(Some(_))
        } else {
          Right(None)
        }

        for {
          parsedHeader <- header
          fieldDelimiter <- delimiter
          collection <- collectionDelimiter
          mapKey <- mapKeyDelimiter
          escapeChar <- escape
        } yield TextSourceDescriptor(
          TextFormatOptions(
            HiveTextSource,
            LazySimpleCodec,
            CommonTextOptions(StandardCharsets.UTF_8.name(), None, NoCompression, splitable = true),
            LazySimpleOptions(
              DelimitedOptions(
                fieldDelimiter,
                parameters.getNullSequence.toString,
                parameters.isEscaped,
                escapeChar,
                parsedHeader,
                emitHeader = false),
              collection,
              mapKey,
              lastColumnTakesRest = false)),
          fileSchema,
          readSchema)
    }
  }

  def validationFailureProperties(reason: String): Map[String, String] = Map(
    SourceKindKey -> HiveTextSource,
    CodecKindKey -> LazySimpleCodec,
    ValidationErrorKey -> reason)

  private def parseSchema(properties: Map[String, String], key: String): Option[StructType] =
    properties
      .get(key)
      .flatMap(value => Try(DataType.fromJson(value)).toOption)
      .collect { case schema: StructType => schema }

  private def isLazySimpleType(dataType: DataType): Boolean = dataType match {
    case StringType | BooleanType | ByteType | ShortType | IntegerType | LongType |
        FloatType | DoubleType | DateType | TimestampType => true
    case decimal: DecimalType => decimal.scale >= 0
    case _ => false
  }

  private def validateRawLineRead(properties: Map[String, String]): ValidationResult = {
    def failed(reason: String): ValidationResult =
      ValidationResult.failed(s"Unsupported Spark Text scan: $reason")

    if (!properties.get(CharsetKey).exists(_.equalsIgnoreCase(StandardCharsets.UTF_8.name()))) {
      return failed("only UTF-8 encoding is supported")
    }
    if (!properties.get(WholeTextKey).contains("false")) {
      return failed("wholetext must be false")
    }
    if (properties.get(LineSeparatorKey).exists(_.nonEmpty)) {
      return failed("custom lineSep is not supported")
    }
    if (!properties.get(CompressionCodecKey).contains(NoCompression)) {
      return failed("compression is not supported")
    }
    if (properties.get(FileSchemaSizeKey) != Some("1") ||
        !properties.get(FileTypeKey).exists(_.equalsIgnoreCase("string"))) {
      return failed("file schema must contain exactly one String column")
    }
    val readSize = properties.get(ReadSchemaSizeKey).flatMap(value => Try(value.toInt).toOption)
    if (!readSize.exists(size => size >= 0 && size <= 1)) {
      return failed("read schema must contain zero or one column")
    }
    if (readSize.contains(1) &&
        !properties.get(ReadTypeKey).exists(_.equalsIgnoreCase("string"))) {
      return failed("read column must be String")
    }
    ValidationResult.succeeded
  }

  private def validateLazySimpleRead(properties: Map[String, String]): ValidationResult = {
    def failed(reason: String): ValidationResult =
      ValidationResult.failed(s"Unsupported Hive LazySimple Text scan: $reason")

    properties.get(ValidationErrorKey).foreach(reason => return failed(reason))
    if (!properties.get(CharsetKey).exists(_.equalsIgnoreCase(StandardCharsets.UTF_8.name()))) {
      return failed("only UTF-8 encoding is supported")
    }
    if (properties.get(LineSeparatorKey).exists(_.nonEmpty)) {
      return failed("custom line separators are not supported")
    }
    if (!properties.get(CompressionCodecKey).contains(NoCompression)) {
      return failed("compression is not supported")
    }
    if (!properties.get("field_delimiter").exists(_.getBytes(StandardCharsets.UTF_8).length == 1)) {
      return failed("field delimiter must be one byte")
    }
    if (properties.get("escape").exists(value => value.nonEmpty &&
        value.getBytes(StandardCharsets.UTF_8).length != 1)) {
      return failed("escape delimiter must be empty or one byte")
    }
    val header = properties.get("header").flatMap(value => Try(value.toInt).toOption)
    if (!header.contains(0)) {
      return failed("skip.header.line.count is not supported by Spark Hive scan")
    }
    val fileSchema = parseSchema(properties, FileSchemaKey).getOrElse {
      return failed("full file schema is missing or invalid")
    }
    if (fileSchema.isEmpty) {
      return failed("full file schema must contain at least one data column")
    }
    val readSchema = parseSchema(properties, ReadSchemaKey).getOrElse {
      return failed("read schema is missing or invalid")
    }
    fileSchema.fields.find(field => !isLazySimpleType(field.dataType)).foreach {
      field => return failed(s"unsupported file column type ${field.name}:${field.dataType.catalogString}")
    }
    readSchema.fields.find(field => !isLazySimpleType(field.dataType)).foreach {
      field => return failed(s"unsupported read column type ${field.name}:${field.dataType.catalogString}")
    }
    readSchema.fields.find { field =>
      !fileSchema.fields.exists(full => full.name.equalsIgnoreCase(field.name) &&
        full.dataType == field.dataType)
    }.foreach(field => return failed(s"read column is not present in full file schema: ${field.name}"))
    ValidationResult.succeeded
  }

  def validateRead(properties: Map[String, String]): ValidationResult = {
    (properties.get(SourceKindKey), properties.get(CodecKindKey)) match {
      case (Some(SparkTextSource), Some(RawLineCodec)) => validateRawLineRead(properties)
      case (Some(HiveTextSource), Some(LazySimpleCodec)) => validateLazySimpleRead(properties)
      case (source, codec) =>
        ValidationResult.failed(
          s"Unsupported Text source/codec combination: ${source.getOrElse("UNSPECIFIED")}/" +
            codec.getOrElse("UNSPECIFIED"))
    }
  }

  def validateInputPartitions(
      partitions: Seq[InputPartition],
      serializableHadoopConf: Option[SerializableConfiguration]): ValidationResult = {
    if (!partitions.forall(_.isInstanceOf[FilePartition])) {
      return ValidationResult.failed(
        "Unsupported Text scan: input partitions cannot be inspected for compression")
    }
    val conf = serializableHadoopConf
      .map(_.value)
      .getOrElse(new Configuration())
    val codecFactory = new CompressionCodecFactory(conf)
    Try {
      partitions.iterator
        .map(_.asInstanceOf[FilePartition])
        .flatMap(_.files.iterator)
        .map(file => new Path(file.filePath.toString))
        .find(path => codecFactory.getCodec(path) != null)
    } match {
      case Success(Some(path)) =>
        ValidationResult.failed(s"Unsupported Text scan: compressed input is not supported: $path")
      case Success(None) => ValidationResult.succeeded
      case Failure(error) =>
        ValidationResult.failed(
          s"Unsupported Text scan: unable to inspect input compression: ${error.getMessage}")
    }
  }

  def validateWrite(
      fields: Array[StructField],
      options: Map[String, String]): ValidationResult = {
    if (fields.length != 1 || fields.head.dataType != StringType) {
      return ValidationResult.failed(
        "Unsupported Spark Text write: data schema must contain exactly one String column")
    }
    validateWriteOptions(options)
  }

  def validateWriteOptions(options: Map[String, String]): ValidationResult = {
    def failed(reason: String): ValidationResult =
      ValidationResult.failed(s"Unsupported Spark Text write: $reason")

    val normalized = normalize(options)
    val rawCharset = normalized.getOrElse("encoding", StandardCharsets.UTF_8.name())
    val charset = Try(Charset.forName(rawCharset)).toOption
    if (!charset.contains(StandardCharsets.UTF_8)) {
      return failed("only UTF-8 encoding is supported")
    }
    if (normalized.contains("linesep")) {
      return failed("custom lineSep is not supported")
    }
    if (normalized.contains("compression")) {
      return failed("compression is not supported")
    }
    ValidationResult.succeeded
  }

  def validateHiveWrite(properties: Map[String, String]): ValidationResult =
    validateLazySimpleRead(properties)
}
