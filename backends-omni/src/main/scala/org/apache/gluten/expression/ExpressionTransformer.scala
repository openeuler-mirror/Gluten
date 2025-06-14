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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression.ConverterUtils.FunctionConfig
import org.apache.gluten.substrait.expression._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import java.lang.{Long => JLong}
import java.util.{ArrayList => JArrayList, HashMap => JHashMap}

import scala.collection.JavaConverters._
import scala.language.existentials

case class OmniAliasTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: Expression)
  extends UnaryExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    child.doTransform(args)
  }
}

case class OmniFromUnixTimeTransformer(
    substraitExprName: String,
    children: Seq[ExpressionTransformer],
    original: FromUnixTime)
  extends ExpressionTransformer {

  private val timeFormatSet: Set[String] = Set("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd")
  private val timeZoneSet: Set[String] = Set("GMT+08:00", "Asia/Shanghai")

  private def unsupportedUnixTimeFunction(timeFormat: String, timeZone: String): Unit = {
    if (GlutenConfig.get.timeParserPolicy == "LEGACY") {
      throw new GlutenNotSupportException(s"Unsupported Time Parser Policy: LEGACY")
    }
    if (!timeZoneSet.contains(timeZone)) {
      throw new GlutenNotSupportException(s"Unsupported Time Zone: $timeZone")
    }
    if (!timeFormatSet.contains(timeFormat)) {
      throw new GlutenNotSupportException(s"Unsupported Time Format: $timeFormat")
    }
  }

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val timeZone = original.timeZoneId.getOrElse("")
    unsupportedUnixTimeFunction(original.format.toString, timeZone)
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val funcName: String =
      ConverterUtils.makeFuncName(substraitExprName, original.children.map(_.dataType))
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, funcName)
    val newChildren =
      children :+ LiteralTransformer(Literal(UTF8String.fromString(timeZone), StringType))
    val childNodes = newChildren.map(_.doTransform(args)).asJava
    val typeNode = ConverterUtils.getTypeNode(dataType, nullable)
    ExpressionBuilder.makeScalarFunction(functionId, childNodes, typeNode)
  }
}

case class OmniHashExpressionTransformer(
    substraitExprName: String,
    children: Seq[ExpressionTransformer],
    original: HashExpression[_])
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // As of Spark 3.3, there are 3 kinds of HashExpression.
    // HiveHash is not supported in native backend and will fail native validation.
    val (seedNode, seedType) = original match {
      case XxHash64(_, seed) =>
        (ExpressionBuilder.makeLongLiteral(seed), LongType)
      case Murmur3Hash(_, seed) =>
        (ExpressionBuilder.makeIntLiteral(seed), IntegerType)
      case HiveHash(_) =>
        (ExpressionBuilder.makeIntLiteral(0), IntegerType)
    }
    val nodes = new JArrayList[ExpressionNode]()
    // Seed as the final argument

    children.foreach(
      expression => {
        nodes.add(expression.doTransform(args))
      })
    nodes.add(seedNode)
    val childrenTypes = seedType +: original.children.map(child => child.dataType)
    val functionMap = args.asInstanceOf[JHashMap[String, JLong]]
    val functionName =
      ConverterUtils.makeFuncName(substraitExprName, childrenTypes, FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, nodes, typeNode)
  }
}
