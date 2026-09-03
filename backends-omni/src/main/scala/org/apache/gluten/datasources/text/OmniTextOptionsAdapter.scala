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
package org.apache.gluten.datasources.text

import java.nio.charset.{Charset, StandardCharsets}

import org.apache.gluten.extension.ValidationResult

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.types.{StringType, StructField, StructType}
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

  val SparkTextSource = "SPARK_TEXT"
  val RawLineCodec = "RAW_LINE"
  val NoCompression = "NONE"

  final case class TextSourceDescriptor(
      sourceKind: String,
      codecKind: String,
      charset: String,
      wholeText: String,
      lineSeparator: Option[String],
      compressionCodec: String,
      fileSchema: StructType,
      readSchema: StructType) {

    def toProperties: Map[String, String] = Map(
      SourceKindKey -> sourceKind,
      CodecKindKey -> codecKind,
      CharsetKey -> charset,
      WholeTextKey -> wholeText,
      LineSeparatorKey -> lineSeparator.getOrElse(""),
      CompressionCodecKey -> compressionCodec,
      FileSchemaSizeKey -> fileSchema.size.toString,
      FileTypeKey -> fileSchema.fields.headOption.map(_.dataType.catalogString).getOrElse(""),
      ReadSchemaSizeKey -> readSchema.size.toString,
      ReadTypeKey -> readSchema.fields.headOption.map(_.dataType.catalogString).getOrElse(""))
  }

  private def normalize(options: Map[String, String]): Map[String, String] =
    options.map { case (key, value) => key.toLowerCase(java.util.Locale.ROOT) -> value }

  def fromSparkText(
      options: Map[String, String],
      fileSchema: StructType,
      readSchema: StructType): TextSourceDescriptor = {
    val normalized = normalize(options)
    val rawCharset = normalized.getOrElse("encoding", StandardCharsets.UTF_8.name())
    val charset = Try(Charset.forName(rawCharset).name()).getOrElse(rawCharset)
    val wholeText = normalized
      .get("wholetext")
      .map(value => Try(value.toBoolean).map(_.toString).getOrElse(value))
      .getOrElse("false")
    TextSourceDescriptor(
      SparkTextSource,
      RawLineCodec,
      charset,
      wholeText,
      normalized.get("linesep"),
      NoCompression,
      fileSchema,
      readSchema)
  }

  def validateRead(properties: Map[String, String]): ValidationResult = {
    def failed(reason: String): ValidationResult =
      ValidationResult.failed(s"Unsupported Spark Text scan: $reason")

    if (properties.get(SourceKindKey) != Some(SparkTextSource)) {
      return failed("source kind is not SPARK_TEXT")
    }
    if (properties.get(CodecKindKey) != Some(RawLineCodec)) {
      return failed("codec kind is not RAW_LINE")
    }
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
      return failed("compression is not supported in phase one")
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

  def validateInputPartitions(
      partitions: Seq[InputPartition],
      serializableHadoopConf: Option[SerializableConfiguration]): ValidationResult = {
    if (!partitions.forall(_.isInstanceOf[FilePartition])) {
      return ValidationResult.failed(
        "Unsupported Spark Text scan: input partitions cannot be inspected for compression")
    }
    val conf = serializableHadoopConf
      .map(_.value)
      .getOrElse(new org.apache.hadoop.conf.Configuration())
    val codecFactory = new CompressionCodecFactory(conf)
    Try {
      partitions.iterator
        .map(_.asInstanceOf[FilePartition])
        .flatMap(_.files.iterator)
        .map(file => new Path(file.filePath.toString))
        .find(path => codecFactory.getCodec(path) != null)
    } match {
      case Success(Some(path)) =>
        ValidationResult.failed(
          s"Unsupported Spark Text scan: compressed input is not supported in phase one: $path")
      case Success(None) => ValidationResult.succeeded
      case Failure(error) =>
        ValidationResult.failed(
          s"Unsupported Spark Text scan: unable to inspect input compression: " +
            error.getMessage)
    }
  }

  def validateWrite(
      fields: Array[StructField],
      options: Map[String, String]): ValidationResult = {
    def failed(reason: String): ValidationResult =
      ValidationResult.failed(s"Unsupported Spark Text write: $reason")

    if (fields.length != 1 || fields.head.dataType != StringType) {
      return failed("data schema must contain exactly one String column")
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
      return failed("compression is not supported in phase one")
    }
    ValidationResult.succeeded
  }
}
