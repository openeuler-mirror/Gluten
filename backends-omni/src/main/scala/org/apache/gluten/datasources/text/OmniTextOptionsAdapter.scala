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
import java.util.{Properties, TimeZone}

import org.apache.gluten.extension.ValidationResult

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.serde2.`lazy`.LazySerDeParameters
import org.apache.hadoop.io.compress.{CompressionCodec, CompressionCodecFactory}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.util.{CompressionCodecs, PermissiveMode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters._
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
  val DateFormatKey = "text_date_format"
  val TimestampFormatCountKey = "text_timestamp_format_count"
  val TimestampFormatPrefix = "text_timestamp_format_"
  val EmptyValueKey = "text_empty_value"
  val IgnoreLeadingWhitespaceKey = "text_ignore_leading_whitespace"
  val IgnoreTrailingWhitespaceKey = "text_ignore_trailing_whitespace"
  val CommentKey = "text_comment"

  val SparkTextSource = "SPARK_TEXT"
  val SparkCsvSource = "SPARK_CSV"
  val HiveTextSource = "HIVE_TEXT"
  val RawLineCodec = "RAW_LINE"
  val LazySimpleCodec = "LAZY_SIMPLE"
  val CsvCodec = "CSV"
  val OpenCsvSerdeClass = "org.apache.hadoop.hive.serde2.OpenCSVSerde"

  def isSupportedHiveSerde(serde: String): Boolean =
    serde == LazySimpleSerdeClass || serde == OpenCsvSerdeClass
  val NoCompression = "NONE"
  val GzipCompression = "GZIP"
  val DeflateCompression = "DEFLATE"
  val SnappyCompression = "SNAPPY"
  val Lz4Compression = "LZ4"
  private val supportedTextTimestampTimeZones = Set("GMT+08:00", "Asia/Shanghai")
  val CompressionBlockSizeKey = "text_compression_block_size"
  val DefaultCompressionBlockSize = 256 * 1024
  val LazySimpleSerdeClass = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"

  private val supportedCodecClasses = Map(
    "org.apache.hadoop.io.compress.GzipCodec" -> GzipCompression,
    "org.apache.hadoop.io.compress.DefaultCodec" -> DeflateCompression,
    "org.apache.hadoop.io.compress.DeflateCodec" -> DeflateCompression,
    "org.apache.hadoop.io.compress.SnappyCodec" -> SnappyCompression,
    "org.apache.hadoop.io.compress.Lz4Codec" -> Lz4Compression)

  def resolveCompressionCodec(codecClassName: String): Either[String, String] =
    Option(codecClassName).filter(_.nonEmpty) match {
      case None => Right(NoCompression)
      case Some(codec) if codec.equalsIgnoreCase(NoCompression) ||
          codec.equalsIgnoreCase("uncompressed") => Right(NoCompression)
      case Some(codec) if supportedCodecClasses.values.exists(_.equalsIgnoreCase(codec)) =>
        Right(codec.toUpperCase(java.util.Locale.ROOT))
      case Some(codec) =>
        supportedCodecClasses.get(codec).toRight(s"unsupported Hadoop codec $codec")
    }

  def resolveInputCompression(
      paths: Seq[String],
      conf: Configuration): Either[String, String] = Try {
    val factory = new CompressionCodecFactory(conf)
    paths.map { path =>
      val codec = factory.getCodec(new Path(path))
      resolveCompressionCodec(Option(codec).map(_.getClass.getName).orNull)
        .fold(reason => throw new IllegalArgumentException(s"$reason: $path"), identity)
    }.distinct
  } match {
    case Success(Seq()) => Right(NoCompression)
    case Success(Seq(codec)) => Right(codec)
    case Success(codecs) =>
      Left(s"mixed Text compression codecs are not supported: ${codecs.mkString(",")}")
    case Failure(error) => Left(error.getMessage)
  }

  private def isSupportedCompression(properties: Map[String, String]): Boolean =
    properties.get(CompressionCodecKey).forall(codec =>
      codec == NoCompression || supportedCodecClasses.values.toSet.contains(codec))

  def configureSparkWriteCompression(
      job: Job,
      options: Map[String, String]): Either[String, (String, String, Int)] = {
    normalize(options).get("compression") match {
      case None => Right((NoCompression, "", DefaultCompressionBlockSize))
      case Some(name) if name.equalsIgnoreCase("none") || name.equalsIgnoreCase("uncompressed") =>
        CompressionCodecs.setCodecConfiguration(job.getConfiguration, null)
        Right((NoCompression, "", DefaultCompressionBlockSize))
      case Some(name) => Try {
        val codecClassName = CompressionCodecs.getCodecClassName(name)
        val nativeCodec = resolveCompressionCodec(codecClassName)
          .fold(reason => throw new IllegalArgumentException(reason), identity)
        val codecClass = Class.forName(codecClassName).asSubclass(classOf[CompressionCodec])
        CompressionCodecs.setCodecConfiguration(job.getConfiguration, codecClassName)
        val codec = org.apache.hadoop.util.ReflectionUtils
          .newInstance(codecClass, job.getConfiguration)
        val blockSizeKey = if (nativeCodec == SnappyCompression) {
          "io.compression.codec.snappy.buffersize"
        } else if (nativeCodec == Lz4Compression) {
          "io.compression.codec.lz4.buffersize"
        } else {
          ""
        }
        val blockSize = if (blockSizeKey.isEmpty) DefaultCompressionBlockSize
        else job.getConfiguration.getInt(blockSizeKey, DefaultCompressionBlockSize)
        require(blockSize > 0, s"$blockSizeKey must be positive")
        (nativeCodec, codec.getDefaultExtension, blockSize)
      } match {
        case Success(value) => Right(value)
        case Failure(error) => Left(error.getMessage)
      }
    }
  }

  sealed trait TextDialectOptions {
    def toProperties: Map[String, String]
  }

  final case class CommonTextOptions(
      charset: String,
      lineSeparator: Option[String],
      compressionCodec: String,
      splitable: Boolean,
      compressionBlockSize: Int = DefaultCompressionBlockSize) {
    def toProperties: Map[String, String] = Map(
      CharsetKey -> charset,
      LineSeparatorKey -> lineSeparator.getOrElse(""),
      CompressionCodecKey -> compressionCodec,
      "text_splitable" -> splitable.toString,
      CompressionBlockSizeKey -> compressionBlockSize.toString)
  }

  final case class TemporalTextOptions(
      dateFormat: Option[String] = None,
      timestampFormats: Seq[String] = Seq.empty) {
    def toProperties: Map[String, String] =
      dateFormat.map(value => DateFormatKey -> value).toMap ++
        Map(TimestampFormatCountKey -> timestampFormats.size.toString) ++
        timestampFormats.zipWithIndex.map { case (format, index) =>
          s"$TimestampFormatPrefix$index" -> format
        }
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

  final case class CsvOptions(
      delimited: DelimitedOptions,
      quote: String,
      parseMode: String,
      emptyValue: String = "",
      ignoreLeadingWhitespace: Boolean = false,
      ignoreTrailingWhitespace: Boolean = false,
      quoteAll: Boolean = false,
      escapeQuotes: Boolean = true,
      comment: Option[String] = None)
    extends TextDialectOptions {
    override def toProperties: Map[String, String] = delimited.toProperties ++ Map(
      "quote" -> quote,
      "text_parse_mode" -> parseMode,
      EmptyValueKey -> emptyValue,
      IgnoreLeadingWhitespaceKey -> ignoreLeadingWhitespace.toString,
      IgnoreTrailingWhitespaceKey -> ignoreTrailingWhitespace.toString,
      "text_quote_all" -> quoteAll.toString,
      "text_escape_quotes" -> escapeQuotes.toString,
      CommentKey -> comment.getOrElse(""))
  }

  final case class TextFormatOptions(
      sourceKind: String,
      codecKind: String,
      common: CommonTextOptions,
      dialect: TextDialectOptions,
      temporal: TemporalTextOptions = TemporalTextOptions()) {
    def toProperties: Map[String, String] = Map(
      SourceKindKey -> sourceKind,
      CodecKindKey -> codecKind) ++ common.toProperties ++ dialect.toProperties ++
      temporal.toProperties
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

  private def validateTimestampTimeZone(schema: StructType, timeZone: String): Option[String] = {
    if (schema.fields.exists(_.dataType == TimestampType) &&
        !supportedTextTimestampTimeZones.contains(timeZone)) {
      Some(s"unsupported timestamp time zone $timeZone")
    } else {
      None
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
        val timestampFormats = Option(parameters.getTimestampFormats)
          .map(_.asScala.toSeq)
          .getOrElse(Seq.empty)
        if (timestampFormats.exists(_.isEmpty)) {
          return Left("timestamp.formats must not contain an empty format")
        }
        val timeZone = TimeZone.getDefault.getID
        validateTimestampTimeZone(readSchema, timeZone) match {
          case Some(reason) => return Left(reason)
          case None => ()
        }
        val charsetName = normalized.getOrElse("serialization.encoding", StandardCharsets.UTF_8.name())
        val charset = Try(Charset.forName(charsetName)).toOption
        if (!charset.contains(StandardCharsets.UTF_8)) {
          return Left("only UTF-8 serialization.encoding is supported")
        }
        if (parameters.isExtendedBooleanLiteral) {
          return Left("hive.lazysimple.extended_boolean_literal=true is not supported")
        }
        if (normalized.get("serialization.escape.crlf").exists(_.equalsIgnoreCase("true"))) {
          return Left("serialization.escape.crlf=true is not supported")
        }

        val header = parseNonNegativeInt(normalized, "skip.header.line.count", 0)
        val footer = parseNonNegativeInt(normalized, "skip.footer.line.count", 0)
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
              lastColumnTakesRest = parameters.isLastColumnTakesRest),
            TemporalTextOptions(timestampFormats = timestampFormats)),
          fileSchema,
          readSchema)
    }
  }

  def fromHiveText(
      conf: Configuration,
      tableProperties: Properties,
      fileSchema: StructType,
      readSchema: StructType): Either[String, TextSourceDescriptor] = {
    if (tableProperties.getProperty("serialization.lib") == OpenCsvSerdeClass) {
      fromHiveOpenCsv(tableProperties, fileSchema, readSchema)
    } else {
      fromHiveLazySimple(conf, tableProperties, fileSchema, readSchema)
    }
  }

  private def fromHiveOpenCsv(
      properties: Properties,
      fileSchema: StructType,
      readSchema: StructType): Either[String, TextSourceDescriptor] = {
    val values = properties.stringPropertyNames().toArray(new Array[String](0))
      .map(key => key -> properties.getProperty(key)).toMap
    val header = parseNonNegativeInt(values, "skip.header.line.count", 0)
    val footer = parseNonNegativeInt(values, "skip.footer.line.count", 0)
    if (footer.exists(_ != 0)) {
      return Left("skip.footer.line.count is not supported")
    }
    def character(key: String, default: String): Either[String, String] = {
      val value = values.getOrElse(key, default)
      if (value.getBytes(StandardCharsets.UTF_8).length != 1) {
        Left(s"$key must contain exactly one single-byte UTF-8 character")
      } else {
        Right(value)
      }
    }
    for {
      parsedHeader <- header
      separator <- character("separatorChar", ",")
      escape <- character("escapeChar", "\"")
      quote <- character("quoteChar", "\"")
    } yield TextSourceDescriptor(
      TextFormatOptions(
        HiveTextSource,
        CsvCodec,
        CommonTextOptions("UTF-8", None, NoCompression, splitable = true),
        CsvOptions(
          DelimitedOptions(separator, "", escapeEnabled = true,
            Some(escape), parsedHeader, emitHeader = false),
          quote, "PERMISSIVE")),
      fileSchema, readSchema)
  }

  def fromSparkCsv(
      options: Map[String, String],
      fileSchema: StructType,
      readSchema: StructType,
      writing: Boolean = false): Map[String, String] = {
    val normalized = normalize(options)
    val conf = SQLConf.get
    def failed(reason: String): Map[String, String] = Map(
      SourceKindKey -> SparkCsvSource, CodecKindKey -> CsvCodec, ValidationErrorKey -> reason)
    Try(new CSVOptions(options, conf.csvColumnPruning, conf.sessionLocalTimeZone)) match {
      case Failure(error) => failed(error.getMessage)
      case Success(csv) =>
        val defaultCsv = new CSVOptions(
          Map.empty, conf.csvColumnPruning, conf.sessionLocalTimeZone)
        val compression = if (writing) {
          csv.compressionCodec match {
            case Some(codec) => resolveCompressionCodec(codec) match {
              case Right(value) => value
              case Left(reason) => return failed(reason)
            }
            case None => NoCompression
          }
        } else {
          NoCompression
        }
        val dateFormat = if (writing) Some(csv.dateFormatInWrite) else csv.dateFormatInRead
        val timestampFormat =
          if (writing) Some(csv.timestampFormatInWrite) else csv.timestampFormatInRead
        if (dateFormat.exists(_.isEmpty) || timestampFormat.exists(_.isEmpty)) {
          return failed("dateFormat and timestampFormat must not be empty")
        }
        val effectiveSchema = if (writing) fileSchema else readSchema
        validateTimestampTimeZone(effectiveSchema, csv.zoneId.getId) match {
          case Some(reason) => return failed(reason)
          case None => ()
        }
        val booleanDefaults = Map(
          "multiline" -> false, "enforceschema" -> true, "columnpruning" -> true)
        val invalidBoolean = booleanDefaults.find { case (key, expected) =>
          normalized.get(key).exists(value => Try(value.toBoolean).toOption != Some(expected))
        }
        if (invalidBoolean.nonEmpty) {
          return failed(s"non-default ${invalidBoolean.get._1} is not supported")
        }
        if (csv.lineSeparator != defaultCsv.lineSeparator) {
          return failed("non-default lineSep is not supported")
        }
        val emptyValue = if (writing) csv.emptyValueInWrite else csv.emptyValueInRead
        if (csv.enableDateTimeParsingFallback.getOrElse(false) !=
            defaultCsv.enableDateTimeParsingFallback.getOrElse(false)) {
          return failed("non-default enableDateTimeParsingFallback is not supported")
        }
        if (csv.multiLine || !csv.enforceSchema ||
            csv.parseMode != PermissiveMode) {
          return failed("only single-line PERMISSIVE CSV is supported")
        }
        val comment = if (!writing && csv.isCommentSet) Some(csv.comment.toString) else None
        if (comment.exists(_.getBytes(StandardCharsets.UTF_8).length != 1)) {
          return failed("CSV comment must contain one single-byte UTF-8 character")
        }
        if (csv.charset != "UTF-8" ||
            csv.maxColumns != 20480 || csv.maxCharsPerColumn != -1 ||
            !normalized.getOrElse("unescapedquotehandling", "STOP_AT_DELIMITER")
              .equalsIgnoreCase("STOP_AT_DELIMITER") ||
            csv.charToEscapeQuoteEscaping.exists(_ != (if (csv.quote == csv.escape) '\u0000' else csv.escape)) ||
            csv.nanValue != "NaN" || csv.positiveInf != "Inf" || csv.negativeInf != "-Inf" ||
            normalized.get("locale").exists(!_.equalsIgnoreCase("en-US")) ||
            normalized.get("extension").exists(_ != "csv")) {
          return failed("unsupported CSV encoding, compression or parser/writer option")
        }
        if ((!writing && !conf.csvColumnPruning) ||
            fileSchema.fields.exists(field => field.name == csv.columnNameOfCorruptRecord ||
              field.metadata.contains("EXISTS_DEFAULT"))) {
          return failed("CSV without column pruning, corrupt-record/default columns is not supported")
        }
        TextSourceDescriptor(
          TextFormatOptions(SparkCsvSource, CsvCodec,
            CommonTextOptions(
              csv.charset,
              None,
              compression,
              splitable = compression == NoCompression),
            CsvOptions(DelimitedOptions(csv.delimiter, csv.nullValue, escapeEnabled = true,
              Some(csv.escape.toString), if (csv.headerFlag && !writing) 1 else 0,
              emitHeader = csv.headerFlag && writing), csv.quote.toString, "PERMISSIVE",
              emptyValue,
              if (writing) csv.ignoreLeadingWhiteSpaceFlagInWrite
              else csv.ignoreLeadingWhiteSpaceInRead,
              if (writing) csv.ignoreTrailingWhiteSpaceFlagInWrite
              else csv.ignoreTrailingWhiteSpaceInRead,
              csv.quoteAll,
              csv.escapeQuotes,
              comment),
            TemporalTextOptions(dateFormat, timestampFormat.toSeq)),
          fileSchema, readSchema).toProperties +
          (SessionTimezoneKey -> csv.zoneId.getId)
    }
  }

  def validateCsv(properties: Map[String, String], writing: Boolean = false): ValidationResult = {
    def failed(reason: String): ValidationResult =
      ValidationResult.failed(s"Unsupported CSV: $reason")
    properties.get(ValidationErrorKey).foreach(reason => return failed(reason))
    if (!isSupportedCompression(properties)) {
      return failed("compression codec is not supported")
    }
    val characters = Seq("field_delimiter", "quote", "escape").map(properties.getOrElse(_, ""))
    if (characters.exists(value => value.getBytes(StandardCharsets.UTF_8).length != 1 ||
        value == "\u0000" || value == "\r" || value == "\n") ||
        characters.head == characters(1) || characters.head == characters(2)) {
      return failed("delimiter, quote and escape must be supported single-byte characters")
    }
    val fileSchema = parseSchema(properties, FileSchemaKey).getOrElse {
      return failed("full file schema is missing")
    }
    val readSchema = parseSchema(properties, ReadSchemaKey).getOrElse {
      return failed("read schema is missing")
    }
    val hive = properties.get(SourceKindKey).contains(HiveTextSource)
    val effectiveHiveEscape = if (characters(2) == "\"") "\\" else characters(2)
    if (hive && (characters.head == effectiveHiveEscape || characters(1) == effectiveHiveEscape)) {
      return failed("OpenCSV reader delimiter, quote and effective escape must be different")
    }
    def supported(dataType: DataType): Boolean = dataType match {
      case StringType => true
      case BooleanType | ByteType | ShortType | IntegerType | LongType |
          FloatType | DoubleType => !hive
      case decimal: DecimalType => !hive && decimal.scale >= 0
      case DateType | TimestampType => !hive
      case _ => false
    }
    if (fileSchema.isEmpty || fileSchema.fields.exists(field => !supported(field.dataType))) {
      return failed("schema contains a type outside the CSV conversion whitelist")
    }
    if (readSchema.fields.exists(field => !fileSchema.fields.exists(full =>
        full.name.equalsIgnoreCase(field.name) && full.dataType == field.dataType))) {
      return failed("projected schema is not present in the full file schema")
    }
    ValidationResult.succeeded
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
    if (!isSupportedCompression(properties)) {
      return failed("compression codec is not supported")
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
    if (!isSupportedCompression(properties)) {
      return failed("compression codec is not supported")
    }
    if (!properties.get("field_delimiter").exists(_.getBytes(StandardCharsets.UTF_8).length == 1)) {
      return failed("field delimiter must be one byte")
    }
    if (properties.get("escape").exists(value => value.nonEmpty &&
        value.getBytes(StandardCharsets.UTF_8).length != 1)) {
      return failed("escape delimiter must be empty or one byte")
    }
    val header = properties.get("header").flatMap(value => Try(value.toInt).toOption)
    if (!header.exists(_ >= 0)) {
      return failed("skip.header.line.count must be non-negative")
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
      case (Some(SparkCsvSource), Some(CsvCodec)) => validateCsv(properties)
      case (Some(HiveTextSource), Some(CsvCodec)) => validateCsv(properties)
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
    val files = partitions.iterator
      .map(_.asInstanceOf[FilePartition])
      .flatMap(_.files.iterator)
      .toSeq
    resolveInputCompression(files.map(_.filePath.toString), conf) match {
      case Left(reason) => ValidationResult.failed(s"Unsupported Text scan: $reason")
      case Right(NoCompression) => ValidationResult.succeeded
      case Right(_) if files.exists(file => file.start != 0 || file.length != file.fileSize) =>
        ValidationResult.failed("Unsupported Text scan: compressed input must use whole-file splits")
      case Right(_) => ValidationResult.succeeded
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
    normalized.get("compression").foreach { codec =>
      val codecClass = Try(CompressionCodecs.getCodecClassName(codec)).toOption.getOrElse {
        return failed(s"unknown compression codec $codec")
      }
      resolveCompressionCodec(codecClass).left.foreach(reason => return failed(reason))
    }
    ValidationResult.succeeded
  }

  def validateHiveWrite(properties: Map[String, String]): ValidationResult =
    if (properties.get(CodecKindKey).contains(CsvCodec)) validateCsv(properties, writing = true)
    else validateLazySimpleRead(properties)
}
