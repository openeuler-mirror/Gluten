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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.gluten.execution.{OmniTopNTransformer, SortExecTransformer, WindowExecTransformer, FilterExecTransformer}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, IntegerLiteral, LessThan, LessThanOrEqual, Literal, NamedExpression, Rank, RowNumber, WindowExpression}

case class RewriteTopNSort ()
  extends Rule[SparkPlan] {

  val columnarConf = GlutenConfig.get
  var enableColumnarTopNSort: Boolean = columnarConf.enableColumnarTopNSort
  val topNSortThreshold: Int = columnarConf.topNSortThreshold

  def isTopNExpression(expr: Expression): Boolean = expr match {
    case Alias(child, _) => isTopNExpression(child)
    case WindowExpression(_: Rank, _) => true
    case _ => false
  }

  def isStrictTopN(expr: Expression): Boolean = expr match {
    case Alias(child, _) => isStrictTopN(child)
    case WindowExpression(_: RowNumber, _) => true
    case _ => false
  }

  def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!enableColumnarTopNSort) {
      return plan
    }
    plan.transformUp {
      case f@FilterExecTransformer(condition, w@WindowExecTransformer(Seq(windowExpression), _, orderSpec, child: SparkPlan)) =>
        if (orderSpec.nonEmpty && isTopNExpression(windowExpression) && child.isInstanceOf[SortExecTransformer]) {
        val sort: SortExecTransformer = child.asInstanceOf[SortExecTransformer]
        var topn = Int.MaxValue
        val nonTopNConditions = splitConjunctivePredicates(condition).filter {
          case LessThan(e: NamedExpression, IntegerLiteral(n))
            if e.exprId == windowExpression.exprId =>
            topn = Math.min(topn, n - 1)
            false
          case GreaterThan(IntegerLiteral(n), e: NamedExpression)
            if e.exprId == windowExpression.exprId =>
            topn = Math.min(topn, n - 1)
            false
          case LessThanOrEqual(e: NamedExpression, IntegerLiteral(n))
            if e.exprId == windowExpression.exprId =>
            topn = Math.min(topn, n)
            false
          case EqualTo(e: NamedExpression, IntegerLiteral(n))
            if n == 1 && e.exprId == windowExpression.exprId =>
            topn = 1
            false
          case EqualTo(IntegerLiteral(n), e: NamedExpression)
            if n == 1 && e.exprId == windowExpression.exprId =>
            topn = 1
            false
          case GreaterThanOrEqual(IntegerLiteral(n), e: NamedExpression)
            if e.exprId == windowExpression.exprId =>
            topn = Math.min(topn, n)
            false
          case _ => true
        }
        // topn <= SQLConf.get.topNPushDownForWindowThreshold 100.
        val strictTopN = isStrictTopN(windowExpression)
        if (topn > 0 && topn <= topNSortThreshold) {
          val topNTransformer = OmniTopNTransformer(
            topn,
            w.orderSpec,
            global = sort.global,
            child = sort.child,
            isTopNSort = true,
            isStrictTopN = strictTopN,
            w.partitionSpec);
          val newCondition = if (nonTopNConditions.isEmpty) {
            Literal.TrueLiteral
          } else {
            nonTopNConditions.reduce(And)
          }
          val window = WindowExecTransformer(w.windowExpression, w.partitionSpec, w.orderSpec, topNTransformer)
          FilterExecTransformer(newCondition, window)
        } else {
          f
        }
      } else {
        f
      }
    }
  }
}