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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.expression.{ConverterUtils, ExpressionConverter, ExpressionTransformer}
import org.apache.gluten.substrait.`type`.TypeBuilder
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution._

import scala.collection.JavaConverters._

case class OmniFilterExecTransformer(condition: Expression, child: SparkPlan, projectList: Seq[NamedExpression])
  extends FilterExecTransformerBase(condition, child) {

  // use condition as filters directly
  // consider push down filters to scan in rule
  override protected def getRemainingCondition: Expression = condition

  override protected def withNewChildInternal(newChild: SparkPlan): OmniFilterExecTransformer =
    copy(child = newChild)

  override def getRelNode(
                  context: SubstraitContext,
                  condExpr: Expression,
                  originalInputAttributes: Seq[Attribute],
                  operatorId: Long,
                  input: RelNode,
                  validation: Boolean): RelNode = {
    assert(condExpr != null)
    val args = context.registeredFunction
    val condExprNode = ExpressionConverter
      .replaceWithExpressionTransformer(condExpr, attributeSeq = originalInputAttributes)
      .doTransform(args)

    if (!validation) {
      val columnarProjExprs: Seq[ExpressionTransformer] = ExpressionConverter
        .replaceWithExpressionTransformer(projectList, originalInputAttributes)
      val projExprNodeList = columnarProjExprs.map(_.doTransform(args)).asJava

      val relNode = RelBuilder.makeProjectRel(
        null,
        projExprNodeList,
        context,
        operatorId,
        false
      )
      val enhancement = BackendsApiManager.getTransformerApiInstance.packPBMessage(relNode.toProtobuf)
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(enhancement)
      RelBuilder.makeFilterRel(input, condExprNode, extensionNode, context, operatorId)
    } else {
      // there has be validated before so it will use super RelNode don't need add project
      val inputTypeNodeList = originalInputAttributes
        .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        .asJava
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.packPBMessage(
          TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeFilterRel(input, condExprNode, extensionNode, context, operatorId)
    }
  }

  override def output: Seq[Attribute] = {
    projectList.map(f => f.toAttribute)
  }
}