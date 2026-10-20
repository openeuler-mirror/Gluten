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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LeafNode, Statistics}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.types._

object ExtendedAggUtils {
  /** Map/Array/Struct cannot be grouping keys for DedupLeftSemiJoin (Spark normalize + Omni native). */
  def containsUnsupportedDedupType(dt: DataType): Boolean = dt match {
    case _: MapType | _: ArrayType | _: StructType => true
    case _ => false
  }

  def supportsDedupLeftSemiJoinKeys(keys: Seq[Expression]): Boolean = {
    !keys.exists(k => containsUnsupportedDedupType(k.dataType))
  }

  def supportsDedupLeftSemiJoinGrouping(groupingExpressions: Seq[NamedExpression]): Boolean = {
    !groupingExpressions.exists(e => containsUnsupportedDedupType(e.dataType))
  }

  def normalizeGroupingExpressions(groupingExpressions: Seq[NamedExpression]): Seq[NamedExpression] = {
    groupingExpressions.map {
      e =>
        if (!containsUnsupportedDedupType(e.dataType) && needsFloatingNormalize(e.dataType)) {
          NormalizeFloatingNumbers.normalize(e) match {
            case n: NamedExpression => n
            case other => Alias(other, e.name)(exprId = e.exprId)
          }
        } else {
          e
        }
    }
  }

  private def needsFloatingNormalize(dt: DataType): Boolean = dt match {
    case FloatType | DoubleType => true
    case StructType(fields) => fields.exists(f => needsFloatingNormalize(f.dataType))
    case ArrayType(elementType, _) => needsFloatingNormalize(elementType)
    case _: MapType | _ => false
  }

  def supportsFilterPropagation(a: Aggregate): Boolean = {
    a.groupingExpressions.isEmpty &&
      a.aggregateExpressions.forall(
        !_.exists {
          case ae: AggregateExpression =>
            ae.aggregateFunction match {
              case _: Count | _: Sum | _: Average | _: Max | _: Min => false
              case _ => true
            }
          case _ => false
        }
      )
  }

  def supportsHashAggregate(aggregateBufferAttributes: Seq[Attribute]): Boolean = {
    Aggregate.supportsHashAggregate(aggregateBufferAttributes)
  }

  def supportsObjectHashAggregate(aggregateExpressions: Seq[AggregateExpression]): Boolean = {
    Aggregate.supportsObjectHashAggregate(aggregateExpressions)
  }

  def planPartialAggregateWithoutDistinct(
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): SparkPlan = {
    val completeAggregateExpressions = aggregateExpressions.map(_.copy(mode = Complete))
    createAggregate(
      requiredChildDistributionExpressions = None,
      groupingExpressions = groupingExpressions.map(_.toAttribute),
      aggregateExpressions = completeAggregateExpressions,
      aggregateAttributes = completeAggregateExpressions.map(_.resultAttribute),
      initialInputBufferOffset = groupingExpressions.length,
      resultExpressions = resultExpressions,
      child = child
    )
  }

  private def createAggregate(
      requiredChildDistributionExpressions: Option[Seq[Expression]] = None,
      isStreaming: Boolean = false,
      groupingExpressions: Seq[NamedExpression] = Nil,
      aggregateExpressions: Seq[AggregateExpression] = Nil,
      aggregateAttributes: Seq[Attribute] = Nil,
      initialInputBufferOffset: Int = 0,
      resultExpressions: Seq[NamedExpression] = Nil,
      child: SparkPlan): SparkPlan = {
    val useHash = supportsHashAggregate(
      aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes))

    if (useHash) {
      HashAggregateExec(
        requiredChildDistributionExpressions = requiredChildDistributionExpressions,
        isStreaming = isStreaming,
        numShufflePartitions = None,
        groupingExpressions = groupingExpressions,
        aggregateExpressions = mayRemoveAggFilters(aggregateExpressions),
        aggregateAttributes = aggregateAttributes,
        initialInputBufferOffset = initialInputBufferOffset,
        resultExpressions = resultExpressions,
        child = child
      )
    } else {
      val objectHashEnabled = child.conf.useObjectHashAggregation
      val useObjectHash = supportsObjectHashAggregate(aggregateExpressions)

      if (objectHashEnabled && useObjectHash) {
        ObjectHashAggregateExec(
          requiredChildDistributionExpressions = requiredChildDistributionExpressions,
          isStreaming = isStreaming,
          numShufflePartitions = None,
          groupingExpressions = groupingExpressions,
          aggregateExpressions = mayRemoveAggFilters(aggregateExpressions),
          aggregateAttributes = aggregateAttributes,
          initialInputBufferOffset = initialInputBufferOffset,
          resultExpressions = resultExpressions,
          child = child
        )
      } else {
        SortAggregateExec(
          requiredChildDistributionExpressions = requiredChildDistributionExpressions,
          isStreaming = isStreaming,
          numShufflePartitions = None,
          groupingExpressions = groupingExpressions,
          aggregateExpressions = mayRemoveAggFilters(aggregateExpressions),
          aggregateAttributes = aggregateAttributes,
          initialInputBufferOffset = initialInputBufferOffset,
          resultExpressions = resultExpressions,
          child = child
        )
      }
    }
  }

  private def mayRemoveAggFilters(exprs: Seq[AggregateExpression]): Seq[AggregateExpression] = {
    exprs.map {
      ae =>
        if (ae.filter.isDefined) {
          ae.mode match {
            case Partial | Complete => ae
            case _ => ae.copy(filter = None)
          }
        } else {
          ae
        }
    }
  }
}

case class DummyLogicalPlan() extends LeafNode {
  override def output: Seq[Attribute] = Nil

  override def computeStats(): Statistics = throw new UnsupportedOperationException
}
