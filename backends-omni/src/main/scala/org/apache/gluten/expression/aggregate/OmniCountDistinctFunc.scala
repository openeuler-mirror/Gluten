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
package org.apache.gluten.expression.aggregate

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{ArrayType, DataType, LongType}

/**
 * count(distinct x) rewritten as a single UDAF, eliminating the Expand operator.
 *
 * Unlike OmniCollectSet, the final result is a Long count while the two-phase intermediate
 * (shuffle) format is the distinct set itself, i.e. array of input values:
 *   - Partial stage:  raw T    -> array<T> (distinct set)   [matches Omni C++ CountDistinctAggregator]
 *   - Final stage:    array<T> -> Long (distinct count)
 *   - Complete stage: raw T    -> Long (distinct count)
 *
 * Row-based fallback semantics: update appends to an array buffer, merge concatenates,
 * evaluate = size(array_distinct(buffer)).
 */
case class OmniCountDistinct(child: Expression)
  extends DeclarativeAggregate
    with UnaryLike[Expression] {

  // Two-phase intermediate state: the distinct set (array of input values).
  protected lazy val buffer: AttributeReference =
    AttributeReference("buffer", ArrayType(child.dataType, false))()

  // Final result: distinct count.
  override def dataType: DataType = LongType

  override def nullable: Boolean = false

  override def aggBufferAttributes: Seq[AttributeReference] = Seq(buffer)

  override lazy val initialValues: Seq[Expression] =
    Seq(Literal.create(Array(), buffer.dataType))

  override lazy val updateExpressions: Seq[Expression] = Seq(
    If(
      IsNull(child),
      buffer,
      Concat(Seq(buffer, CreateArray(Seq(child), useStringTypeWhenEmpty = false))))
  )

  override lazy val mergeExpressions: Seq[Expression] = Seq(
    Concat(Seq(buffer.left, buffer.right))
  )

  override def defaultResult: Option[Literal] = Option(Literal(0L))

  // Row-based fallback evaluation: distinct count = size(array_distinct(buffer)).
  override lazy val evaluateExpression: Expression =
    Size(ArrayDistinct(buffer), false)

  override def prettyName: String = "omni_count_distinct"

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}
