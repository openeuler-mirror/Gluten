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
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.`type`.{ListNode, MapNode, TypeBuilder}
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode, OmniStringViewLiteralNode, StructLiteralNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import com.google.common.collect.Lists

case class ChildTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: Expression)
  extends UnaryExpressionTransformer {
  override def dataType: DataType = child.dataType

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    child.doTransform(args)
  }
}

case class CastTransformer(substraitExprName: String, child: ExpressionTransformer, original: Cast)
  extends UnaryExpressionTransformer {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val typeNode = ConverterUtils.getTypeNode(dataType, original.nullable)
    ExpressionBuilder.makeCast(
      typeNode,
      child.doTransform(args),
      SparkShimLoader.getSparkShims.withAnsiEvalMode(original))
  }
}

case class StringViewToOmniVarcharCastTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: Expression)
  extends UnaryExpressionTransformer {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // Downgrade target is standard VARCHAR (variation 0), not a special variation.
    val typeNode = TypeBuilder.makeString(original.nullable)
    ExpressionBuilder.makeCast(typeNode, child.doTransform(args), false)
  }
}

// Emits a string literal as an Omni StringView literal (substrait Literal type_variation_reference
// 21) so it resolves the native {SV,SV} comparison when compared with a StringView column. Built
// directly because StringLiteralNode.updateLiteralBuilder only sets the string value and drops the
// type node's variation. The rule only wraps non-null string literals, so `value` is non-null here.
case class StringViewLiteralTransformer(original: StringViewLiteral) extends LeafExpressionTransformer {
  override def substraitExprName: String = "literal"
  // Use the named, serializable OmniStringViewLiteralNode (not an anonymous ExpressionNode, which is
  // not Serializable and breaks Spark task-closure serialization of the substrait plan tree).
  override def doTransform(args: java.lang.Object): ExpressionNode =
    new OmniStringViewLiteralNode(original.literal.value.toString)
}

case class ExplodeTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: Explode)
  extends UnaryExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode: ExpressionNode = child.doTransform(args)

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(substraitExprName, Seq(original.child.dataType)))

    val expressionNodes = Lists.newArrayList(childNode)
    val childTypeNode = ConverterUtils.getTypeNode(original.child.dataType, original.child.nullable)
    childTypeNode match {
      case l: ListNode =>
        ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, l.getNestedType)
      case m: MapNode =>
        ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, m.getNestedType)
      case _ =>
        throw new GlutenNotSupportException(s"explode($childTypeNode) not supported yet.")
    }
  }
}

case class CheckOverflowTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: CheckOverflow)
  extends UnaryExpressionTransformer {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    BackendsApiManager.getTransformerApiInstance.createCheckOverflowExprNode(
      args,
      substraitExprName,
      child.doTransform(args),
      original.child.dataType,
      original.dataType,
      original.nullable,
      original.nullOnOverflow)
  }
}

case class GetStructFieldTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: GetStructField)
  extends BinaryExpressionTransformer {
  override def left: ExpressionTransformer = child
  override def right: ExpressionTransformer = LiteralTransformer(original.ordinal)

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.doTransform(args)
    childNode match {
      case node: StructLiteralNode =>
        node.getFieldLiteral(original.ordinal)
      case _ =>
        super.doTransform(args)
    }
  }
}
