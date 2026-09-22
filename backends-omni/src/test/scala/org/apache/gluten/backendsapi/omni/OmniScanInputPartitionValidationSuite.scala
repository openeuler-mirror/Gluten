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

package org.apache.gluten.backendsapi.omni

import org.apache.gluten.backendsapi.BackendSettingsApi
import org.apache.gluten.datasources.text.OmniTextOptionsAdapter
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal, NamedExpression}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.{InSubqueryExec, LocalTableScanExec, SubqueryExec}
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.types.LongType
import org.apache.spark.util.SerializableConfiguration

import org.scalatest.funsuite.AnyFunSuite

class OmniScanInputPartitionValidationSuite extends AnyFunSuite {
  private val settings: BackendSettingsApi = OmniBackendSettings
  private val textProperties = Map(
    OmniTextOptionsAdapter.SourceKindKey -> OmniTextOptionsAdapter.SparkTextSource,
    OmniTextOptionsAdapter.CodecKindKey -> OmniTextOptionsAdapter.RawLineCodec)
  private val conf = Some(new SerializableConfiguration(new Configuration(false)))

  for (format <- Seq(ReadFileFormat.OrcReadFormat, ReadFileFormat.ParquetReadFormat)) {
    test(s"$format validation must not evaluate an unfinished DPP subquery") {
      val key = AttributeReference("key", LongType)()
      val subquery = SubqueryExec("dynamicpruning-test", LocalTableScanExec(Seq(key), Nil))
      val pending = InSubqueryExec(Literal(1L), subquery, NamedExpression.newExprId)
      val error = intercept[IllegalArgumentException] {
        pending.eval(InternalRow.empty)
      }
      assert(error.getMessage.contains("has not finished"))
      var evaluations = 0
      def runtimePartitions: Seq[InputPartition] = {
        evaluations += 1
        pending.eval(InternalRow.empty)
        Seq.empty
      }
      assert(settings.validateScanInputPartitions(format, runtimePartitions, Map.empty, conf).ok())
      assert(evaluations == 0)
      intercept[IllegalArgumentException] { pending.eval(InternalRow.empty) }
    }
  }

  test("Text validation still evaluates candidates once and rejects opaque partitions") {
    var evaluations = 0
    def candidates: Seq[InputPartition] = {
      evaluations += 1
      Seq(new InputPartition {})
    }
    val result = settings.validateScanInputPartitions(
      ReadFileFormat.TextReadFormat, candidates, textProperties, conf)
    assert(!result.ok())
    assert(result.reason().contains("cannot be inspected for compression"))
    assert(evaluations == 1)
  }

  test("Text validation accepts empty candidate sets and empty file partitions") {
    for (candidates <- Seq(Seq.empty[InputPartition], Seq(FilePartition(0, Array.empty)))) {
      assert(settings.validateScanInputPartitions(
        ReadFileFormat.TextReadFormat, candidates, textProperties, conf).ok())
    }
  }

  test("Text compression checks still reject unsupported and mixed codecs") {
    val hadoopConf = new Configuration(false)
    assert(OmniTextOptionsAdapter.resolveInputCompression(
      Seq("file:///part.txt"), hadoopConf).isRight)
    assert(OmniTextOptionsAdapter.resolveInputCompression(
      Seq("file:///part.gz"), hadoopConf) == Right(OmniTextOptionsAdapter.GzipCompression))
    assert(OmniTextOptionsAdapter.resolveInputCompression(
      Seq("file:///part.bz2"), hadoopConf).isLeft)
    assert(OmniTextOptionsAdapter.resolveInputCompression(
      Seq("file:///part.txt", "file:///part.gz"), hadoopConf).isLeft)
  }
}
