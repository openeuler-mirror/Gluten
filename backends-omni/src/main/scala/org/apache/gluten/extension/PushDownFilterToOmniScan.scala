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
import org.apache.gluten.datasources.orc.OmniOrcFileFormat
import org.apache.gluten.execution.{FileSourceScanExecTransformer, FilterExecTransformer}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Not, Or, PredicateHelper}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{BooleanType, DateType, DoubleType, IntegerType, LongType, ShortType}


object PushDownFilterToOmniScan extends Rule[SparkPlan] with PredicateHelper {

  private val enableVecPredicateFilter: Boolean =
    GlutenConfig.get.enabledVecPredicateFilter

  private val SUPPORTED_DATA_TYPES = Set(ShortType, IntegerType, LongType, DoubleType, BooleanType, DateType)

  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case filter: FilterExecTransformer if enableVecPredicateFilter =>
      filter.child match {
        case fileScan: FileSourceScanExecTransformer if fileScan.relation.fileFormat.isInstanceOf[OmniOrcFileFormat] =>
          val pushDownFilters = getPushedFilter(fileScan.dataFilters)
          val newScan = fileScan.copy(dataFilters = pushDownFilters)
          if (newScan.doValidate().ok()) {
            val pushDownFilterSet = pushDownFilters.toSet
            val newFilterConditions = splitConjunctivePredicates(filter.cond)
              .filterNot(pushDownFilterSet.contains)
            if (newFilterConditions.isEmpty) {
              newScan
            } else {
              filter.makeCopy(Array(newFilterConditions.reduceOption(And).get, newScan))
            }
          } else {
            filter
          }
        case _ => filter
      }
  }


  private def getPushedFilter(dataFilters: Seq[Expression]): Seq[Expression] = {
    dataFilters.filter(isAllSupportPushDown)
  }

  private def isAllSupportPushDown(condition: Expression): Boolean = {
    def isSingleExprDataTypeSupported(expr: Expression): Boolean = {
      SUPPORTED_DATA_TYPES.exists(_.equals(expr.dataType))
    }

    def isBinaryExprDataTypeSupported(left: Expression, right: Expression): Boolean = {
      isSingleExprDataTypeSupported(left) && isSingleExprDataTypeSupported(right)
    }

    def isBinaryExprAttributeSupported(left: Expression, right: Expression): Boolean = {
      (isAttribute(left) && isLiteral(right)) || (isAttribute(right) && isLiteral(left))
    }

    def isBinaryExprSupported(left: Expression, right: Expression): Boolean = {
      isBinaryExprDataTypeSupported(left, right) && isBinaryExprAttributeSupported(left, right)
    }

    def isAttribute(expr: Expression): Boolean = {
      expr.isInstanceOf[Attribute]
    }

    def isLiteral(expr: Expression): Boolean = {
      expr.isInstanceOf[Literal]
    }

    condition match {
      case equalTo: EqualTo =>
        isBinaryExprSupported(equalTo.left, equalTo.right)
      case greaterThan: GreaterThan =>
        isBinaryExprSupported(greaterThan.left, greaterThan.right)
      case greaterThanOrEqual: GreaterThanOrEqual =>
        isBinaryExprSupported(greaterThanOrEqual.left, greaterThanOrEqual.right)
      case lessThan: LessThan =>
        isBinaryExprSupported(lessThan.left, lessThan.right)
      case lessThanOrEqual: LessThanOrEqual =>
        isBinaryExprSupported(lessThanOrEqual.left, lessThanOrEqual.right)
      case isNotNull: IsNotNull =>
        isAttribute(isNotNull.child)
      case isNull: IsNull =>
        isAttribute(isNull.child)
      case and: And =>
        isAllSupportPushDown(and.left) && isAllSupportPushDown(and.right)
      case or: Or =>
        isAllSupportPushDown(or.left) && isAllSupportPushDown(or.right)
      case not: Not =>
        isAllSupportPushDown(not.child)
      case _ => false
    }
  }
}