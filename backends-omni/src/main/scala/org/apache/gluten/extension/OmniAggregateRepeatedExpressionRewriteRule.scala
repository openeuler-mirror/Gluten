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
package org.apache.gluten.extension

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.AGGREGATE

import scala.collection.mutable

/**
 * Pull repeated complex aggregate sub-expressions into a child Project so Expand / HashAggregate
 * can consume the computed attributes directly.
 */
case class OmniAggregateRepeatedExpressionRewriteRule(spark: SparkSession)
  extends Rule[LogicalPlan]
  with Logging {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.resolved || !GlutenConfig.get.enableGluten || !GlutenConfig.get.enableColumnarHashAgg) {
      return plan
    }

    plan.transformUpWithPruning(_.containsPattern(AGGREGATE)) {
      case agg: Aggregate => rewriteAggregate(agg)
    }
  }

  private def rewriteAggregate(aggregate: Aggregate): LogicalPlan = {
    val aggregateExprs = aggregate.aggregateExpressions
      .filter(_.find(_.isInstanceOf[AggregateExpression]).isDefined)

    if (aggregateExprs.isEmpty) {
      return aggregate
    }

    val exprCounts = mutable.LinkedHashMap.empty[ExpressionEquals, (Expression, Int)]
    aggregateExprs.foreach(collectRepeatedExpressions(_, exprCounts))

    val repeatedExprs = exprCounts.valuesIterator.collect {
      case (expr, count) if count >= OmniAggregateRepeatedExpressionRewriteRule.reuseThreshold => expr
    }.toSeq

    if (repeatedExprs.isEmpty) {
      return aggregate
    }

    val selectedExprs = selectMaximalExpressions(repeatedExprs)
    if (selectedExprs.isEmpty) {
      return aggregate
    }

    val aliases = selectedExprs.zipWithIndex.map {
      case (expr, index) => Alias(expr, s"_omni_common_expr_$index")()
    }
    val replaceMap = aliases.map(alias => ExpressionEquals(alias.child) -> alias.toAttribute).toMap

    val newAggregateExpressions = aggregate.aggregateExpressions.map {
      expr => replaceCommonExprWithAttribute(expr, replaceMap).asInstanceOf[NamedExpression]
    }

    if (newAggregateExpressions == aggregate.aggregateExpressions) {
      return aggregate
    }

    val newChild = Project(aggregate.child.output ++ aliases, aggregate.child)
    aggregate.copy(aggregateExpressions = newAggregateExpressions, child = newChild)
  }

  private def collectRepeatedExpressions(
      expr: Expression,
      exprCounts: mutable.LinkedHashMap[ExpressionEquals, (Expression, Int)]): Unit = {
    if (isReusableExpression(expr)) {
      val key = ExpressionEquals(expr)
      exprCounts.get(key) match {
        case Some((existing, count)) =>
          exprCounts.update(key, (existing, count + 1))
        case None =>
          exprCounts.put(key, (expr, 1))
      }
    }
    expr.children.foreach(collectRepeatedExpressions(_, exprCounts))
  }

  private def isReusableExpression(expr: Expression): Boolean = {
    if (
      !expr.deterministic ||
      expr.foldable ||
      expr.isInstanceOf[Attribute] ||
      expr.isInstanceOf[BoundReference] ||
      expr.isInstanceOf[Literal] ||
      expr.isInstanceOf[AggregateExpression] ||
      expr.isInstanceOf[AggregateFunction] ||
      (expr.isInstanceOf[Unevaluable] && !expr.isInstanceOf[AttributeReference]) ||
      expr.find(isGroupingIdReference).isDefined
    ) {
      return false
    }

    expr.children.nonEmpty && expr.children.forall(child => !child.isInstanceOf[AggregateFunction])
  }

  private def isGroupingIdReference(expr: Expression): Boolean = {
    expr match {
      case attr: AttributeReference => attr.name == VirtualColumn.groupingIdName
      case _ => false
    }
  }

  private def selectMaximalExpressions(expressions: Seq[Expression]): Seq[Expression] = {
    expressions
      .sortBy(expr => (-expr.treeString.length, -expr.references.size))
      .foldLeft(Seq.empty[Expression]) {
        case (selected, candidate)
            if selected.exists(parent => containsEquivalentSubExpr(parent, candidate)) =>
          selected
        case (selected, candidate) =>
          selected :+ candidate
      }
  }

  private def containsEquivalentSubExpr(parent: Expression, candidate: Expression): Boolean = {
    parent.children.exists {
      child => child.semanticEquals(candidate) || containsEquivalentSubExpr(child, candidate)
    }
  }

  private def replaceCommonExprWithAttribute(
      expr: Expression,
      replaceMap: Map[ExpressionEquals, Attribute]): Expression = {
    replaceMap.get(ExpressionEquals(expr)) match {
      case Some(attribute) => attribute
      case None => expr.mapChildren(replaceCommonExprWithAttribute(_, replaceMap))
    }
  }
}

object OmniAggregateRepeatedExpressionRewriteRule {
  private[extension] def reuseThreshold: Int =
    GlutenConfig.get.omniAggregateRepeatedExpressionReuseThreshold
}
