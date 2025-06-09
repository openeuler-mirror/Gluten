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

import org.apache.gluten.expression.ConverterUtils
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, AggregateMode, CollectList, CollectSet, Final, Partial, Complete, PartialMerge}

import scala.collection.mutable.ListBuffer
case class OmniHashAggregateExecPullOutBaseHelper(
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute])
  extends HashAggregateExecPullOutBaseHelper {

   /** This method calculates the output attributes of Aggregation. */
    override protected def getAttrForAggregateExprs: List[Attribute] = {
      val aggregateAttr = new ListBuffer[Attribute]()
      val size = aggregateExpressions.size
      var resIndex = 0
      for (expIdx <- 0 until size) {
        val exp: AggregateExpression = aggregateExpressions(expIdx)
        resIndex = getAttrForAggregateExpr(exp, aggregateAttributes, aggregateAttr, resIndex)
      }
      aggregateAttr.toList
    }

    protected def getAttrForAggregateExpr(
          exp: AggregateExpression,
          aggregateAttributeList: Seq[Attribute],
          aggregateAttr: ListBuffer[Attribute],
          index: Int): Int = {
        var resIndex = index
        val mode = exp.mode
        val aggregateFunc = exp.aggregateFunction
        if (!checkAggFuncModeSupport(aggregateFunc, mode)) {
          throw new UnsupportedOperationException(
          s"Unsupported aggregate mode: $mode for ${aggregateFunc.prettyName}")
        }
        mode match {
          case Partial | PartialMerge =>
            val aggBufferAttr = aggregateFunc.inputAggBufferAttributes
            for (index <- aggBufferAttr.indices) {
              val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(index))
              aggregateAttr += attr
            }
            resIndex += aggBufferAttr.size
            resIndex
          case Final | Complete =>
            aggregateAttr += aggregateAttributeList(resIndex)
            resIndex += 1
            resIndex
          case other =>
            throw new UnsupportedOperationException(s"Unsupported aggregate mode: $other.")
        }

    private def checkAggFuncModeSupport(
                                        aggFunc: aggregateFunction,
                                        mode: AggregateMode): Boolean = {
     aggFunc match {
        case _: CollectList | _: CollectSet =>
            mode match {
                case Partial | Final => true
                case _ => false
            }
        case bloom if bloom.getClass.getSimpleName.equals("BloomFilterAggregate") =>
            mode match {
                case Partial | Final => true
                case _ => false
            }
        case _ =>
            mode match {
                case Partial | PartialMerge | Final => true
                case _ => false
            }
        }
    }
}
