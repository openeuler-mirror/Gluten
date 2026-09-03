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

import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class OmniFileFormatWriterSuite extends AnyFunSuite {
  private val stringSchema = new StructType().add("value", StringType)

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
      .isEmpty)
  }

  test("execution-side Text resolution does not affect other file formats") {
    assert(OmniFileFormatWriter
      .resolveTextNativeFormat(true, new ParquetFileFormat(), stringSchema, Map.empty)
      .isEmpty)
  }
}
