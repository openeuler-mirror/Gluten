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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression.ConverterUtils.FunctionConfig
import org.apache.gluten.substrait.expression.ExpressionBuilder
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types.DataType

object AggregateFunctionsBuilder {
  def create(args: java.lang.Object, aggregateFunc: AggregateFunction): Long = {
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]

    // First handle the custom aggregate functions
    val substraitAggFuncName = getSubstraitFunctionName(aggregateFunc)

    // Check whether each backend supports this aggregate function.
    if (
      !BackendsApiManager.getValidatorApiInstance.doExprValidate(
        substraitAggFuncName,
        aggregateFunc)
    ) {
      throw new GlutenNotSupportException(s"Aggregate function not supported for $aggregateFunc.")
    }

    val inputTypes: Seq[DataType] = aggregateFunc.children.map(child => child.dataType)

    ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(substraitAggFuncName, inputTypes, FunctionConfig.REQ))
  }

  /**
   * Register aggregate function by explicit substrait name and input types.
   * Used only by backends that need to override the default name (e.g. Omni maps RegrReplacement
   * to regr_sxx/regr_syy with two args). Does not change getSubstraitFunctionName or create();
   * other aggregates are unaffected.
   */
  def createWithName(args: java.lang.Object, substraitName: String, inputTypes: Seq[DataType]): Long = {
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(substraitName, inputTypes, FunctionConfig.REQ))
  }

  def getSubstraitFunctionName(aggregateFunc: AggregateFunction): String = {
    if (SparkShimLoader.getSparkShims.isTrySum(aggregateFunc)) {
      return ExpressionNames.TRY_SUM
    }
    aggregateFunc match {
      case First(_, ignoreNulls) if ignoreNulls =>
        ExpressionNames.FIRST_IGNORE_NULL
      case Last(_, ignoreNulls) if ignoreNulls =>
        ExpressionNames.LAST_IGNORE_NULL
      case BitAndAgg(_) =>
        ExpressionNames.BIT_AND_AGG
      case BitOrAgg(_) =>
        ExpressionNames.BIT_OR_AGG
      case BitXorAgg(_) =>
        ExpressionNames.BIT_XOR_AGG
      case Corr(_, _, _) =>
        ExpressionNames.CORR
      case CovPopulation(_, _, _) =>
        ExpressionNames.COVAR_POP
      case CovSample(_, _, _) =>
        ExpressionNames.COVAR_SAMP
      case MaxBy(_, _) =>
        ExpressionNames.MAX_BY
      case MinBy(_, _) =>
        ExpressionNames.MIN_BY
      case HyperLogLogPlusPlus(_, _, _, _) =>
        ExpressionNames.APPROX_DISTINCT
      case CollectSet(_, _, _) =>
        ExpressionNames.COLLECT_SET
      case CollectList(_, _, _) =>
        ExpressionNames.COLLECT_LIST
      case Skewness(_, _) =>
        ExpressionNames.SKEWNESS
      case Kurtosis(_, _) =>
        ExpressionNames.KURTOSIS
      case ApproximatePercentile(_, _, _, _, _) =>
        ExpressionNames.APPROX_PERCENTILE
      case _ =>
        val nameOpt = ExpressionMappings.expressionsMap.get(aggregateFunc.getClass)
        if (nameOpt.isDefined) {
          nameOpt.get match {
            case ExpressionNames.UDAF_PLACEHOLDER => aggregateFunc.prettyName
            case name => name
          }
        } else {
          // RegrSxx/RegrSyy may not exist in all Spark versions (e.g. missing in some 3.5);
          // resolve by class name so Substrait gets regr_sxx/regr_syy when the class exists.
          aggregateFunc.getClass.getName match {
            case "org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate" =>
              ExpressionNames.BLOOM_FILTER_AGG
            case "org.apache.spark.sql.catalyst.expressions.aggregate.RegrCount" =>
              ExpressionNames.REGR_COUNT
            case "org.apache.spark.sql.catalyst.expressions.aggregate.RegrSlope" =>
              ExpressionNames.REGR_SLOPE
            case "org.apache.spark.sql.catalyst.expressions.aggregate.RegrIntercept" =>
              ExpressionNames.REGR_INTERCEPT
            case "org.apache.spark.sql.catalyst.expressions.aggregate.RegrR2" =>
              ExpressionNames.REGR_R2
            case "org.apache.spark.sql.catalyst.expressions.aggregate.RegrSXY" =>
              ExpressionNames.REGR_SXY
            case "org.apache.spark.sql.catalyst.expressions.aggregate.RegrReplacement" =>
              ExpressionNames.REGR_REPLACEMENT
            case "org.apache.spark.sql.catalyst.expressions.aggregate.RegrSXX" =>
              ExpressionNames.REGR_SXX
            case "org.apache.spark.sql.catalyst.expressions.aggregate.RegrSYY" =>
              ExpressionNames.REGR_SYY
            case "org.apache.spark.sql.catalyst.expressions.aggregate.RegrAvgX" =>
              ExpressionNames.REGR_AVGX
            case "org.apache.spark.sql.catalyst.expressions.aggregate.RegrAvgY" =>
              ExpressionNames.REGR_AVGY
            case _ =>
              throw new GlutenNotSupportException(
                s"Could not find a valid substrait mapping name for $aggregateFunc.")
          }
        }
    }
  }
}
