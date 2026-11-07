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
import org.apache.gluten.expression.aggregate.OmniCountDistinct

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.AGGREGATE_EXPRESSION
import org.apache.spark.sql.types._

/**
 * By converting Count(with isDistinct=true) to a UDAF called OmniCountDistinct, we can avoid the
 * Expand operator in the physical plan, which would otherwise replicate each row N times for N
 * distinct aggregates (row amplification and OOM with dozens of count(distinct)).
 *
 * Only single-column count(distinct x) is rewritten; multi-column count(distinct a, b) keeps the
 * Expand path. Only data types supported by the Omni CountDistinctAggregator are rewritten; others
 * keep the Expand path.
 *
 * This rule takes no effect unless spark.gluten.enabled and
 * spark.gluten.sql.countDistinctWithoutExpand are both true.
 */
object OmniCountDistinctWithoutExpand extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    if (GlutenConfig.get.enableGluten && GlutenConfig.get.enableCountDistinctWithoutExpand) {
      plan.transformAllExpressionsWithPruning(_.containsPattern(AGGREGATE_EXPRESSION)) {
        case ae: AggregateExpression
            if ae.isDistinct && ae.aggregateFunction.isInstanceOf[Count] &&
              ae.aggregateFunction.children.size == 1 &&
              isSupportedType(ae.aggregateFunction.children.head.dataType) =>
          ae.copy(
            aggregateFunction = OmniCountDistinct(ae.aggregateFunction.children.head),
            isDistinct = false)
      }
    } else {
      plan
    }
  }

  /** Must stay in sync with CountDistinctAggregatorFactory in OmniOperator core. */
  private def isSupportedType(dataType: DataType): Boolean = dataType match {
    case _: BooleanType | _: ByteType | _: ShortType | _: IntegerType | _: LongType |
        _: FloatType | _: DoubleType | _: DateType | _: TimestampType | _: DecimalType |
        _: StringType | _: CharType | _: BinaryType =>
      true
    case _ => false
  }
}
