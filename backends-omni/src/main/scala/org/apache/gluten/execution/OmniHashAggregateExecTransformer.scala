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
package org.apache.gluten.execution

import org.apache.gluten.substrait.{AggregationParams, SubstraitContext}
import org.apache.gluten.substrait.rel.RelNode

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.execution.SparkPlan

case class OmniHashAggregateExecTransformer(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends HashAggregateExecBaseTransformer(
    requiredChildDistributionExpressions,
    groupingExpressions,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    child) {
  override protected def getAggRel(
      context: SubstraitContext,
      operatorId: Long,
      aggParams: AggregationParams,
      input: RelNode,
      validation: Boolean): RelNode = null

  override def isStreaming: Boolean = false

  override def numShufflePartitions: Option[Int] = Option.empty

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

}
