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
package org.apache.gluten.expression

import org.apache.gluten.expression.ExpressionNames.REGR_SXX
import org.apache.gluten.expression.ExpressionNames.REGR_SYY

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, Expression, If, IsNull, Literal, Or}
import org.apache.spark.sql.types.DoubleType

/**
 * Omni backend: build measure arguments and substrait name override for regr_sxx/regr_syy
 * when the Spark plan uses RegrReplacement (single-column variance). We treat them as
 * two-argument aggregates (y, x) so Omni can filter by (x,y) pair non-nullity.
 *
 * Spark sends RegrReplacement(If(Or(IsNull(left), IsNull(right)), null, value)) where for regr_sxx
 * value is the independent variable (right / x), and for regr_syy value is the dependent variable
 * (left / y). The optimizer may commute Or, so we must not assume Or's left/right match (y, x);
 * we pair [[falseValue]] with the other ref from the Or to build Omni's (y, x) order.
 * When y and x are the same column (e.g. regr_sxx(c, c)), the optimizer may collapse the If to a bare
 * [[AttributeReference]]; we then treat (y, x) as (attr, attr) and map to regr_sxx.
 */
object OmniRegrMeasureBuilder {

  private val REGR_REPLACEMENT_CLASS_NAME =
    "org.apache.spark.sql.catalyst.expressions.aggregate.RegrReplacement"

  def isRegrReplacement(aggregateFunc: AggregateFunction): Boolean =
    aggregateFunc.getClass.getName == REGR_REPLACEMENT_CLASS_NAME

  /**
   * Spark may wrap the RegrReplacement child `If(Or(IsNull(y), IsNull(x)), null, value)` in one or
   * more [[Cast]] nodes for type coercion (e.g. BIGINT to DOUBLE). Strip those before matching.
   */
  def unwrapLeadingCasts(expr: Expression): Expression = expr match {
    case c: Cast => unwrapLeadingCasts(c.child)
    case e => e
  }

  /**
   * Partial aggregate child is expected as `If(cond, null, value)`, optionally wrapped in Cast(s).
   * Returns the value branch (fed to the single-column partial aggregate input).
   */
  def extractRegrReplacementPartialValueExpr(childExpr: Expression): Expression = {
    unwrapLeadingCasts(childExpr) match {
      case i: If => i.falseValue
      case ar: AttributeReference => ar
      case other =>
        throw new UnsupportedOperationException(
          s"RegrReplacement Partial expects If(cond, null, value) or a bare column ref (optionally wrapped " +
            s"in Cast), got ${other.getClass.getSimpleName}")
    }
  }

  /**
   * If the aggregate is RegrReplacement used for regr_sxx/regr_syy, return the substrait name
   * and for Partial/Complete the two child expressions (y, x) in order.
   * For PartialMerge/Final the caller uses inputAggBufferAttributes (3 cols) as usual.
   *
   * @return Some((substraitName, partialChildExprs)) where partialChildExprs is Some([y, x])
   *         for Partial/Complete; substraitName is "regr_sxx" or "regr_syy".
   */
  def getRegrReplacementOverride(aggregateFunc: AggregateFunction): Option[(String, Option[Seq[Expression]])] = {
    aggregateFunc match {
      case agg if isRegrReplacement(agg) =>
        parseRegrReplacementChild(aggregateFunc.children.head, aggregateFunc).map {
          case (name, yx) => (name, Some(yx))
        }
      case _ =>
        None
    }
  }

  /**
   * Substrait function name for regr_sxx vs regr_syy when the plan uses [[RegrReplacement]].
   */
  private def resolveRegrSubstraitName(aggregateFunc: AggregateFunction): String = {
    aggregateFunc.getClass.getName match {
      case "org.apache.spark.sql.catalyst.expressions.aggregate.RegrSXX" => REGR_SXX
      case "org.apache.spark.sql.catalyst.expressions.aggregate.RegrSYY" => REGR_SYY
      case _ =>
        // Spark 3.5: AggregateFunction overloads sql(Boolean); force Expression#sql to avoid ambiguity.
        val sql = (aggregateFunc: Expression).sql.toLowerCase
        if (sql.contains("regr_syy")) REGR_SYY else REGR_SXX
    }
  }

  /**
   * Parse RegrReplacement's child: If(Or(IsNull(ref1), IsNull(ref2)), null, value).
   * Returns (substraitName, Seq(yExpr, xExpr)) for Omni. Spark's replacement uses false = right (x)
   * for regr_sxx and false = left (y) for regr_syy; [[Or]] may be commuted, so we derive (y, x) from
   * [[falseValue]] and the other column in the Or, not from Or's left/right order.
   */
  private def parseRegrReplacementChild(
      child: Expression,
      aggregateFunc: AggregateFunction): Option[(String, Seq[Expression])] = {
    unwrapLeadingCasts(child) match {
      case ifExpr: If if (ifExpr.trueValue match { case Literal(null, _) => true; case _ => false }) =>
        val cond = ifExpr.predicate
        val falseValue = unwrapLeadingCasts(ifExpr.falseValue)
        val substraitName = resolveRegrSubstraitName(aggregateFunc)
        cond match {
          case or: Or =>
            (or.left, or.right) match {
              case (IsNull(ref1), IsNull(ref2)) =>
                val yx: Option[(Expression, Expression)] =
                  if (falseValue.semanticEquals(ref1) && falseValue.semanticEquals(ref2)) {
                    Some((ref1, ref2))
                  } else if (falseValue.semanticEquals(ref1)) {
                    Some(
                      if (substraitName == REGR_SYY) (falseValue, ref2) else (ref2, falseValue))
                  } else if (falseValue.semanticEquals(ref2)) {
                    Some(
                      if (substraitName == REGR_SYY) (falseValue, ref1) else (ref1, falseValue))
                  } else {
                    None
                  }
                yx.map { case (yExpr, xExpr) => (substraitName, Seq(yExpr, xExpr)) }
              case _ =>
                None
            }
          case _ =>
            None
        }
      case attr: AttributeReference =>
        // Same column for y and x: IF/OR folds away; partial child is just the value column.
        Some((REGR_SXX, Seq(attr, attr)))
      case _ =>
        None
    }
  }

  /** Input types for regr_sxx/regr_syy partial: two doubles (y, x). */
  def regrSxxSyyPartialInputTypes: Seq[org.apache.spark.sql.types.DataType] =
    Seq(DoubleType, DoubleType)
}
