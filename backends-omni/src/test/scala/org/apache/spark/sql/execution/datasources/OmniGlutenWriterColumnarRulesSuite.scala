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

import org.apache.gluten.extension.ValidationResult

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.{ColumnarWriteFilesExec, LeafExecNode, SparkPlan}
import org.apache.spark.sql.types.StringType

import org.scalatest.funsuite.AnyFunSuite

class OmniGlutenWriterColumnarRulesSuite extends AnyFunSuite {

  private case class TestLeafExec(override val output: Seq[Attribute])
    extends LeafExecNode {
    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("TestLeafExec does not execute")
  }

  private case class TestColumnarWriteFilesExec(
      override val left: SparkPlan,
      override val right: SparkPlan)
    extends ColumnarWriteFilesExec(left, right) {

    override protected def doValidateInternal(): ValidationResult = ValidationResult.succeeded

    override protected def withNewChildrenInternal(
        newLeft: SparkPlan,
        newRight: SparkPlan): SparkPlan = copy(left = newLeft, right = newRight)
  }

  test("Text planned write validates the actual write input") {
    val value = AttributeReference("value", StringType)()
    val input = TestLeafExec(Seq(value))
    val plannedWrite = TestColumnarWriteFilesExec(input, ColumnarWriteFilesExec.NoopLeaf())

    assert(plannedWrite.output.isEmpty)
    assert(OmniGlutenWriterColumnarRules
      .resolveWriteValidationOutput(plannedWrite, textWrite = true) == input.output)
  }

  test("write validation output remains unchanged outside Text planned write") {
    val value = AttributeReference("value", StringType)()
    val input = TestLeafExec(Seq(value))
    val plannedWrite = TestColumnarWriteFilesExec(input, ColumnarWriteFilesExec.NoopLeaf())

    assert(OmniGlutenWriterColumnarRules
      .resolveWriteValidationOutput(plannedWrite, textWrite = false).isEmpty)
    assert(OmniGlutenWriterColumnarRules
      .resolveWriteValidationOutput(input, textWrite = true) == input.output)
  }
}
