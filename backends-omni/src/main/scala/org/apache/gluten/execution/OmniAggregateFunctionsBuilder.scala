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

import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression.{AggregateFunctionsBuilder, ConverterUtils}
import org.apache.gluten.expression.ConverterUtils.FunctionConfig
import org.apache.gluten.substrait.expression.ExpressionBuilder
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  AggregateFunction,
  AggregateMode,
  Final,
  PartialMerge
}
import org.apache.spark.sql.expression.UDFResolver
import org.apache.spark.sql.hive.HiveUDAFInspector

import java.util.{HashMap => JHashMap}

/** Aggregation function builder for Omni backend dynamic UDAFs. */
object OmniAggregateFunctionsBuilder {

  def create(
      args: java.lang.Object,
      aggregateFunc: AggregateFunction,
      mode: AggregateMode): Long = {
    val functionMap = args.asInstanceOf[JHashMap[String, java.lang.Long]]
    val (sigName, aggFunc) =
      try {
        (AggregateFunctionsBuilder.getSubstraitFunctionName(aggregateFunc), aggregateFunc)
      } catch {
        case e: GlutenNotSupportException =>
          HiveUDAFInspector.getUDAFClassName(aggregateFunc) match {
            case Some(udafClass) if UDFResolver.UDAFNames.contains(udafClass) =>
              (udafClass, UDFResolver.getUdafExpression(udafClass)(aggregateFunc.children))
            case _ => throw e
          }
        case e: Throwable => throw e
      }

    val inputTypes =
      if (mode == PartialMerge || mode == Final) {
        aggFunc.inputAggBufferAttributes.map(_.dataType)
      } else {
        aggFunc.children.map(_.dataType)
      }

    ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(sigName, inputTypes, FunctionConfig.REQ))
  }
}
