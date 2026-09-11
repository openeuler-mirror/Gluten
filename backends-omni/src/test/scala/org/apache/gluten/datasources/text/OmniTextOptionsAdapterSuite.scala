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

import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.{
  DateType,
  IntegerType,
  StringType,
  StructField,
  StructType,
  TimestampType
}

import org.scalatest.funsuite.AnyFunSuite

class OmniTextOptionsAdapterSuite extends AnyFunSuite {
  private val stringSchema = new StructType().add("value", StringType)

  test("CSV aliases, header and projection retain the source and codec") {
    val schema = new StructType().add("name", StringType).add("age", IntegerType)
    val properties = OmniTextOptionsAdapter.fromSparkCsv(
      Map("SEP" -> "|", "header" -> "true", "nullValue" -> "NULL"), schema,
      new StructType().add("age", IntegerType))
    assert(OmniTextOptionsAdapter.validateRead(properties).ok())
    assert(properties(OmniTextOptionsAdapter.SourceKindKey) == "SPARK_CSV")
    assert(properties(OmniTextOptionsAdapter.CodecKindKey) == "CSV")
    assert(properties("field_delimiter") == "|")
    assert(properties("header") == "1")
    assert(properties("nullValue") == "NULL")
    val count = OmniTextOptionsAdapter.fromSparkCsv(Map.empty, schema, new StructType())
    assert(OmniTextOptionsAdapter.validateRead(count).ok())
    val writer = OmniTextOptionsAdapter.fromSparkCsv(
      Map("header" -> "true"), schema, schema, writing = true)
    assert(OmniTextOptionsAdapter.validateCsv(writer, writing = true).ok())
    assert(writer("header") == "0")
    assert(writer("text_emit_header") == "true")
  }

  test("CSV parser options outside the whitelist fail before native execution") {
    Seq(Map("multiLine" -> "true"), Map("sep" -> "||"), Map("mode" -> "FAILFAST"),
      Map("mode" -> "DROPMALFORMED"), Map("encoding" -> "UTF-16"),
      Map("lineSep" -> "|"), Map("comment" -> "#"),
      Map("enforceSchema" -> "false"), Map("maxColumns" -> "5"),
      Map("unescapedQuoteHandling" -> "RAISE_ERROR"), Map("quoteAll" -> "true"),
      Map("ignoreLeadingWhiteSpace" -> "true"), Map("emptyValue" -> "EMPTY"))
      .foreach { options =>
        val properties = OmniTextOptionsAdapter.fromSparkCsv(options, stringSchema, stringSchema)
        assert(!OmniTextOptionsAdapter.validateRead(properties).ok(), options.toString)
      }
  }

  test("CSV time types respect existing conversion boundaries") {
    val timestamp = new StructType().add("t", TimestampType)
    val date = new StructType().add("d", DateType)
    val timestampRead = OmniTextOptionsAdapter.fromSparkCsv(
      Map("timestampFormat" -> "yyyy/MM/dd HH:mm:ss"), timestamp, timestamp)
    val dateRead = OmniTextOptionsAdapter.fromSparkCsv(
      Map("dateFormat" -> "yyyy/MM/dd"), date, date)
    assert(OmniTextOptionsAdapter.validateRead(timestampRead).ok())
    assert(OmniTextOptionsAdapter.validateRead(dateRead).ok())
    assert(timestampRead("text_timestamp_format_0") == "yyyy/MM/dd HH:mm:ss")
    assert(dateRead(OmniTextOptionsAdapter.DateFormatKey) == "yyyy/MM/dd")
    assert(OmniTextOptionsAdapter.validateCsv(
      OmniTextOptionsAdapter.fromSparkCsv(Map.empty, date, date, writing = true),
      writing = true).ok())
  }

  test("OpenCSVSerde retains its own defaults and string-only schema") {
    val properties = new Properties()
    properties.setProperty("serialization.lib", OmniTextOptionsAdapter.OpenCsvSerdeClass)
    val descriptor = OmniTextOptionsAdapter.fromHiveText(
      new Configuration(), properties, stringSchema, stringSchema)
      .fold(reason => fail(reason), identity)
    assert(descriptor.toProperties("escape") == "\"")
    assert(OmniTextOptionsAdapter.validateRead(descriptor.toProperties).ok())
    val numeric = new StructType().add("value", IntegerType)
    val invalid = OmniTextOptionsAdapter.fromHiveText(
      new Configuration(), properties, numeric, numeric).fold(reason => fail(reason), identity)
    assert(!OmniTextOptionsAdapter.validateRead(invalid.toProperties).ok())
    properties.setProperty("skip.header.line.count", "1")
    assert(OmniTextOptionsAdapter.fromHiveText(
      new Configuration(), properties, stringSchema, stringSchema).isLeft)
  }

  test("default Spark Text descriptor is phase-one native compatible") {
    val descriptor = OmniTextOptionsAdapter.fromSparkText(Map.empty, stringSchema, stringSchema)
    val properties = descriptor.toProperties

    assert(properties(OmniTextOptionsAdapter.SourceKindKey) == "SPARK_TEXT")
    assert(properties(OmniTextOptionsAdapter.CodecKindKey) == "RAW_LINE")
    assert(properties(OmniTextOptionsAdapter.CharsetKey) == "UTF-8")
    assert(OmniTextOptionsAdapter.validateRead(properties).ok())
  }

  test("empty projection is accepted") {
    val descriptor = OmniTextOptionsAdapter.fromSparkText(
      Map.empty,
      stringSchema,
      StructType(Nil))
    assert(OmniTextOptionsAdapter.validateRead(descriptor.toProperties).ok())
  }

  test("boolean options are normalized before validation") {
    val descriptor = OmniTextOptionsAdapter.fromSparkText(
      Map("wholeText" -> "FALSE"),
      stringSchema,
      stringSchema)
    assert(OmniTextOptionsAdapter.validateRead(descriptor.toProperties).ok())
  }

  test("unsupported Spark Text read options are rejected") {
    val cases = Seq(
      Map("wholetext" -> "true") -> "wholetext",
      Map("encoding" -> "UTF-16") -> "UTF-8",
      Map("lineSep" -> "|") -> "lineSep")

    cases.foreach {
      case (options, expectedReason) =>
        val properties = OmniTextOptionsAdapter
          .fromSparkText(options, stringSchema, stringSchema)
          .toProperties
        val result = OmniTextOptionsAdapter.validateRead(properties)
        assert(!result.ok())
        assert(result.reason().contains(expectedReason))
    }
  }

  test("TextReadFormat without explicit source and codec is rejected") {
    val result = OmniTextOptionsAdapter.validateRead(Map.empty)
    assert(!result.ok())
    assert(result.reason().contains("source/codec combination"))
  }

  test("LazySimple descriptor preserves composed delimiter and null options") {
    val properties = new Properties()
    properties.setProperty("columns", "name,age")
    properties.setProperty("columns.types", "string:int")
    properties.setProperty("field.delim", "|")
    properties.setProperty("serialization.null.format", "NULL")
    properties.setProperty("timestamp.formats", "yyyy/MM/dd HH:mm:ss,yyyy-MM-dd HH:mm:ss")
    val schema = new StructType().add("name", StringType).add("age", IntegerType)

    val descriptor = OmniTextOptionsAdapter
      .fromHiveLazySimple(new Configuration(), properties, schema, schema)
      .fold(reason => fail(reason), identity)

    assert(descriptor.toProperties(OmniTextOptionsAdapter.SourceKindKey) == "HIVE_TEXT")
    assert(descriptor.toProperties(OmniTextOptionsAdapter.CodecKindKey) == "LAZY_SIMPLE")
    assert(descriptor.toProperties("field_delimiter") == "|")
    assert(descriptor.toProperties("nullValue") == "NULL")
    assert(descriptor.toProperties(OmniTextOptionsAdapter.TimestampFormatCountKey) == "2")
    assert(descriptor.toProperties("text_timestamp_format_0") == "yyyy/MM/dd HH:mm:ss")
    assert(descriptor.toProperties("text_timestamp_format_1") == "yyyy-MM-dd HH:mm:ss")
    assert(OmniTextOptionsAdapter.validateRead(descriptor.toProperties).ok())
  }

  test("read schema must be zero or one String column") {
    val wrongType = new StructType().add("value", IntegerType)
    val twoColumns = new StructType().add("left", StringType).add("right", StringType)

    Seq(wrongType, twoColumns).foreach { readSchema =>
      val properties = OmniTextOptionsAdapter
        .fromSparkText(Map.empty, stringSchema, readSchema)
        .toProperties
      assert(!OmniTextOptionsAdapter.validateRead(properties).ok())
    }
  }

  test("write gate accepts supported compression codecs") {
    val fields = Array(StructField("value", StringType))
    assert(OmniTextOptionsAdapter.validateWrite(fields, Map.empty).ok())
    Seq("gzip", "deflate", "snappy", "lz4").foreach { codec =>
      assert(OmniTextOptionsAdapter.validateWrite(
        fields, Map("compression" -> codec)).ok(), codec)
    }

    Seq(
      Map("encoding" -> "UTF-16"),
      Map("lineSep" -> "|"),
      Map("compression" -> "bzip2")).foreach { options =>
      assert(!OmniTextOptionsAdapter.validateWrite(fields, options).ok())
    }
  }

  test("write schema must contain exactly one String column") {
    assert(!OmniTextOptionsAdapter.validateWrite(Array.empty[StructField], Map.empty).ok())
    assert(!OmniTextOptionsAdapter
      .validateWrite(Array(StructField("value", IntegerType)), Map.empty)
      .ok())
  }
}
