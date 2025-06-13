/*
 * Copyright (C) 2024-2024. Huawei Technologies Co., Ltd. All rights reserved.
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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.OmniTopNTransformer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{LocalLimitExec, SortExec, SparkPlan, TakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.{ShuffleExchangeExec, ShuffleOrigin}

case class PushOrderedLimitThroughAgg(session: SparkSession)
  extends Rule[SparkPlan]
  with PredicateHelper {
  private object TakeOrderedAndProjectExecShim {
    def unapply(exec: TakeOrderedAndProjectExec)
        : Option[(Int, Seq[SortOrder], Seq[NamedExpression], SparkPlan, Int)] = {
      TakeOrderedAndProjectExec.unapply(exec).map {
        case (limit, sortOrder, projectList, child) => (limit, sortOrder, projectList, child, 0)
      }
    }
  }

  private object ShuffleExchangeExecShim {
    def unapply(exchange: ShuffleExchangeExec)
        : Option[(Partitioning, SparkPlan, ShuffleOrigin, Option[Long])] = {
      ShuffleExchangeExec.unapply(exchange).map {
        case (outputPartitioning, child, shuffleOrigin) =>
          (outputPartitioning, child, shuffleOrigin, None)
      }
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    val columnarConf = GlutenConfig.get
    // The two optimization principles are contrary and cannot be used at the same time.
    // reason: the pushOrderedLimitThroughAgg rule depends on the actual aggregation result in the partial phase.
    // However, if the partial phase is skipped, aggregation is not performed.
    if (
      !columnarConf.enablePushOrderedLimitThroughAgg || columnarConf.enableAdaptivePartialAggregation
    ) {
      return plan
    }

    val enableColumnarTopNSort: Boolean = columnarConf.enableColumnarTopNSort

    plan.transform {
      case orderAndProject @ TakeOrderedAndProjectExecShim(
            limit,
            sortOrder,
            projectList,
            orderAndProjectChild,
            offset) => {
        orderAndProjectChild match {
          case finalAgg @ HashAggregateExec(_, _, _, _, _, _, _, _, finalAggChild) =>
            finalAggChild match {
              case shuffleExchange @ ShuffleExchangeExecShim(_, shuffleExchangeChild, _, _) =>
                shuffleExchangeChild match {
                  case partialAgg @ HashAggregateExec(
                        _,
                        _,
                        _,
                        partialAggGroupingExpressions,
                        _,
                        _,
                        _,
                        _,
                        _) =>
                    val validSortOrder = sortOrder.takeWhile {
                      order =>
                        partialAggGroupingExpressions.exists(
                          attr => order.child.references.exists(ref => ref.name == attr.name))
                    }
                    if (validSortOrder.nonEmpty) {
                      val newTopNSort = if (enableColumnarTopNSort) {
                        OmniTopNTransformer(
                          limit,
                          validSortOrder,
                          global = false,
                          child = partialAgg);
                      } else {
                        val newSortExec = SortExec(
                          validSortOrder,
                          global = false,
                          child = partialAgg
                        )
                        LocalLimitExec(limit, child = newSortExec)
                      }
                      session.sparkContext.setLocalProperty(
                        "pushOrderedLimitThroughAggApplied",
                        "true");
                      TakeOrderedAndProjectExec(
                        limit,
                        sortOrder,
                        projectList,
                        child = HashAggregateExec(
                          finalAgg.requiredChildDistributionExpressions,
                          finalAgg.isStreaming,
                          finalAgg.numShufflePartitions,
                          finalAgg.groupingExpressions,
                          finalAgg.aggregateExpressions,
                          finalAgg.aggregateAttributes,
                          finalAgg.initialInputBufferOffset,
                          finalAgg.resultExpressions,
                          child = ShuffleExchangeExec(
                            shuffleExchange.outputPartitioning,
                            child = newTopNSort,
                            shuffleExchange.shuffleOrigin
                          )
                        )
                      )
                    } else {
                      orderAndProject
                    }

                  case _ => orderAndProject
                }
              case _ => orderAndProject
            }
          case _ => orderAndProject
        }
      }
    }
  }
}
