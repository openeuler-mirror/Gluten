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
package org.apache.spark.sql.execution.datasources

import java.util.Properties

import org.apache.gluten.backendsapi.omni.OmniBackendSettings
import org.apache.gluten.datasources.text.OmniTextOptionsAdapter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
import org.apache.hadoop.hive.ql.plan.{FileSinkDesc, TableDesc}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.hive.execution.OmniHiveFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class OmniFileFormatWriterSuite extends AnyFunSuite {
  private val stringSchema = new StructType().add("value", StringType)

  private def textSink(serde: String): FileSinkDesc = {
    val properties = new Properties()
    properties.setProperty("serialization.lib", serde)
    properties.setProperty("columns", "value")
    properties.setProperty("columns.types", "string")
    val table = new TableDesc(classOf[TextInputFormat],
      classOf[HiveIgnoreKeyTextOutputFormat[_, _]], properties)
    new FileSinkDesc(new Path("/tmp/omni_text_write_validation"), table, false)
  }

  test("Hive Text planning and prepareWrite share compression and buffer validation") {
    Seq(OmniTextOptionsAdapter.LazySimpleSerdeClass, OmniTextOptionsAdapter.OpenCsvSerdeClass)
      .foreach { serde =>
        val sink = textSink(serde)
        val conf = new Configuration()
        assert(OmniHiveFileFormat.nativeTextWriteOptions(sink, stringSchema, conf).isRight)
        sink.setCompressed(true)
        sink.setCompressCodec(null)
        assert(OmniHiveFileFormat.nativeTextWriteOptions(sink, stringSchema, conf).isLeft)
        sink.setCompressCodec("org.apache.hadoop.io.compress.BZip2Codec")
        assert(OmniHiveFileFormat.nativeTextWriteOptions(sink, stringSchema, conf).isLeft)
        Seq("Snappy" -> "snappy", "Lz4" -> "lz4").foreach { case (codec, key) =>
          sink.setCompressCodec(s"org.apache.hadoop.io.compress.${codec}Codec")
          val bufferKey = s"io.compression.codec.$key.buffersize"
          conf.setInt(bufferKey, 512 * 1024)
          val options = OmniHiveFileFormat.nativeTextWriteOptions(sink, stringSchema, conf)
            .fold(reason => fail(reason), identity)
          assert(options(OmniTextOptionsAdapter.CompressionBlockSizeKey) == "524288")
          conf.setInt(bufferKey, 0)
          assert(OmniHiveFileFormat.nativeTextWriteOptions(sink, stringSchema, conf).isLeft)
        }
      }
  }

  test("Hive OpenCSV write validation excludes directory partitions but checks data types") {
    val conf = new SQLConf()
    conf.setConfString("spark.gluten.sql.native.hive.writer.enabled", "true")
    SQLConf.withExistingConf(conf) {
      val sink = textSink(OmniTextOptionsAdapter.OpenCsvSerdeClass)
      sink.getTableInfo.getProperties.setProperty("partition_columns", "p")
      val format = new OmniHiveFileFormat(sink)
      val partitioned = stringSchema.add("p", IntegerType)
      assert(OmniBackendSettings.supportWriteFilesExec(
        format, partitioned.fields, None, Map.empty).ok())
      assert(OmniBackendSettings.supportWriteFilesExec(
        format, stringSchema.fields, None, Map.empty).ok())
      val invalid = new StructType().add("value", IntegerType).add("p", IntegerType)
      assert(!OmniBackendSettings.supportWriteFilesExec(
        format, invalid.fields, None, Map.empty).ok())
    }
  }

  test("eligible Text write resolves its native format without a Spark local format property") {
    val result = OmniFileFormatWriter.resolveTextNativeFormat(
      true,
      new TextFileFormat(),
      stringSchema,
      Map.empty)

    assert(result.contains("text"))
  }

  test("Text execution-side resolution preserves the write gates") {
    val intSchema = new StructType().add("value", IntegerType)

    assert(OmniFileFormatWriter
      .resolveTextNativeFormat(false, new TextFileFormat(), stringSchema, Map.empty)
      .isEmpty)
    assert(OmniFileFormatWriter
      .resolveTextNativeFormat(true, new TextFileFormat(), intSchema, Map.empty)
      .isEmpty)
    assert(OmniFileFormatWriter
      .resolveTextNativeFormat(
        true,
        new TextFileFormat(),
        stringSchema,
        Map("compression" -> "gzip"))
      .contains("text"))
    assert(OmniFileFormatWriter
      .resolveTextNativeFormat(
        true,
        new TextFileFormat(),
        stringSchema,
        Map("compression" -> "bzip2"))
      .isEmpty)
  }

  test("execution-side Text resolution does not affect other file formats") {
    assert(OmniFileFormatWriter
      .resolveTextNativeFormat(true, new ParquetFileFormat(), stringSchema, Map.empty)
      .isEmpty)
  }
}
