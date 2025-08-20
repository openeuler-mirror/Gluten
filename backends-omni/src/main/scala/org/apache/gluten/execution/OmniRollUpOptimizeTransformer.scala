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

import com.google.protobuf.Any
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.expression.{AggregateFunctionsBuilder, ConverterUtils, ExpressionConverter}
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.substrait.`type`.{TypeBuilder, TypeNode}
import org.apache.gluten.substrait.expression.{AggregateFunctionNode, ExpressionBuilder, ExpressionNode}
import org.apache.gluten.substrait.extensions.{AdvancedExtensionNode, ExtensionBuilder}
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}
import org.apache.gluten.substrait.{AggregationParams, SubstraitContext}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types._

import java.util.{ArrayList => JArrayList, List => JList}
import scala.collection.JavaConverters._

case class OmniRollUpOptimizeTransformer(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    expandExecTransformer: ExpandExecTransformer,
    child: SparkPlan)
  extends HashAggregateExecBaseTransformer(
    requiredChildDistributionExpressions,
    groupingExpressions,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    child) {

  override def output: Seq[Attribute] = {
    // TODO: We should have a check to make sure the returned schema actually matches the output
    //  data. Since "resultExpressions" is not actually in used by Velox.
    super.output
  }

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = expandExecTransformer.asInstanceOf[TransformSupport].transform(context)
    val aggParams = new AggregationParams
    val operatorId = context.nextOperatorId(this.nodeName)
    val relNode = getAggRel(context, operatorId, aggParams, childCtx.root)
    TransformContext(output, relNode)
  }

  // Create aggregate function node and add to list.
  private def addFunctionNode(
      args: java.lang.Object,
      aggregateFunction: AggregateFunction,
      childrenNodeList: JList[ExpressionNode],
      aggregateMode: AggregateMode,
      aggregateNodeList: JList[AggregateFunctionNode]): Unit = {
    def addFunctionNodeWithAggFunctionInputAggBuf(): Unit = {
      val aggFunctionInputAggBufferAttributeFTuple = aggregateFunction.inputAggBufferAttributes
        .map {
          expr =>
            validDataType(expr.dataType)
            (ConverterUtils.getTypeNode(expr.dataType, expr.nullable), expr.name)
        }
        .foldLeft((new JArrayList[TypeNode](), new JArrayList[String]()))(
          (left, right) => {
            left._1.add(right._1)
            left._2.add(right._2)
            left
          })
      aggregateNodeList.add(
        ExpressionBuilder.makeAggregateFunction(
          AggregateFunctionsBuilder.create(args, aggregateFunction),
          childrenNodeList,
          modeToKeyWord(aggregateMode),
          TypeBuilder.makeStruct(
            false,
            aggFunctionInputAggBufferAttributeFTuple._1,
            aggFunctionInputAggBufferAttributeFTuple._2)
        ))
    }

    aggregateMode match {
      case Partial | PartialMerge =>
        addFunctionNodeWithAggFunctionInputAggBuf()
      case Final | Complete =>
        validDataType(aggregateFunction.dataType)
        aggregateNodeList.add(
          ExpressionBuilder.makeAggregateFunction(
            AggregateFunctionsBuilder.create(args, aggregateFunction),
            childrenNodeList,
            modeToKeyWord(aggregateMode),
            ConverterUtils.getTypeNode(aggregateFunction.dataType, aggregateFunction.nullable)
          )
        )
      case other =>
        throw new UnsupportedOperationException(s"$other mode is not supported in Omni backend")
    }
  }

  protected def checkAggFuncSupport(aggFunc: AggregateFunction, mode: AggregateMode): Boolean = {
    val alwaysSupported = Set(
      classOf[Sum],
      classOf[Min],
      classOf[Max],
      classOf[Count],
      classOf[Average],
      classOf[First],
      classOf[StddevSamp]
    )

    val completeOnlySupported = Set(
      classOf[Sum],
      classOf[Min],
      classOf[Max],
      classOf[Count],
      classOf[Average],
      classOf[First]
    )

    val supported = mode match {
      case Final | PartialMerge | Partial => alwaysSupported
      case Complete => completeOnlySupported
      case other => Set.empty[Class[_]]
    }

    if (supported.exists(_.isInstance(aggFunc))) {
      true
    } else {
      false
    }
  }

  override protected def doValidateInternal(): ValidationResult = {
    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId(this.nodeName)
    val aggParams = new AggregationParams
    val relNode = getAggRel(substraitContext, operatorId, aggParams, null, validation = true)

    def doValidateInput(input: Seq[Attribute]): ValidationResult = {
      var info = ""
      input.map(_.dataType).foreach {
        dataType =>
          if (BackendsApiManager.getValidatorApiInstance.doSchemaValidate(dataType).isDefined) {
            info += s"Unsupported expr source datatype: $dataType \n"
          }
      }
      if (info.nonEmpty) {
        ValidationResult.failed(info)
      } else {
        ValidationResult.succeeded
      }
    }

    val inputValidateResult = doValidateInput(child.output)
    if (!inputValidateResult.ok()) {
      return inputValidateResult
    }

    val unsupportedAggExprs = aggregateAttributes.filterNot(attr => checkType(attr.dataType))
    if (unsupportedAggExprs.nonEmpty) {
      return ValidationResult.failed(
        "Found unsupported data type in aggregation expression: " +
          unsupportedAggExprs
            .map(attr => s"${attr.name}#${attr.exprId.id}:${attr.dataType}")
            .mkString(", "))
    }
    val unsupportedGroupExprs = groupingExpressions.filterNot(attr => checkType(attr.dataType))
    if (unsupportedGroupExprs.nonEmpty) {
      return ValidationResult.failed(
        "Found unsupported data type in grouping expression: " +
          unsupportedGroupExprs
            .map(attr => s"${attr.name}#${attr.exprId.id}:${attr.dataType}")
            .mkString(", "))
    }
    aggregateExpressions.foreach {
      expr =>
        if (!checkAggFuncModeSupport(expr.aggregateFunction, expr.mode)) {
          return ValidationResult.failed(
            s"Unsupported aggregate mode: ${expr.mode} for ${expr.aggregateFunction.prettyName}")
        }
    }

    aggregateExpressions.foreach {
      expr =>
        if (!checkAggFuncSupport(expr.aggregateFunction, expr.mode)) {
          return ValidationResult.failed(
            s"Unsupported aggregate function: ${expr.mode} for ${expr.aggregateFunction.prettyName}")
        }
    }

    doNativeValidation(substraitContext, relNode)
  }

  override protected def checkType(dataType: DataType): Boolean = {
    dataType match {
      case ShortType | IntegerType | LongType | TimestampType | DoubleType | BooleanType |
          StringType | DateType =>
        true
      case _: DecimalType => true
      case _ => false
    }
  }

  private def validDataType(dataType: DataType): Unit = {
    if (BackendsApiManager.getValidatorApiInstance.doSchemaValidate(dataType).isDefined) {
      throw new UnsupportedOperationException(
        s"Unsupported agg function output datatype: $dataType")
    }
  }

  /**
   * Create and return the Rel for the this aggregation.
   *
   * @param context
   *   the Substrait context
   * @param operatorId
   *   the operator id
   * @param aggParams
   *   the params for aggregation mainly used for metrics updating
   * @param input
   *   tht input rel node
   * @param validation
   *   whether this is for native validation
   * @return
   *   the rel node for this aggregation
   */
  override protected def getAggRel(
      context: SubstraitContext,
      operatorId: Long,
      aggParams: AggregationParams,
      input: RelNode = null,
      validation: Boolean = false): RelNode = {
    val originalInputAttributes = expandExecTransformer.output
    val aggRel = getAggRelInternal(
      context,
      originalInputAttributes,
      operatorId,
      input,
      validation)
    context.registerAggregationParam(operatorId, aggParams)
    aggRel
  }

  private def getAggRelInternal(
      context: SubstraitContext,
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode = null,
      validation: Boolean): RelNode = {
    val args = context.registeredFunction
    // Get the grouping nodes.
    // Use 'child.output' as based Seq[Attribute], the originalInputAttributes
    // may be different for each backend.
    val groupingList = groupingExpressions
      .map(
        ExpressionConverter
          .replaceWithExpressionTransformer(_, expandExecTransformer.output)
          .doTransform(args))
      .asJava
    // Get the aggregate function nodes.
    val aggFilterList = new JArrayList[ExpressionNode]()
    val aggregateFunctionList = new JArrayList[AggregateFunctionNode]()
    aggregateExpressions.foreach(
      aggExpr => {
        if (aggExpr.filter.isDefined) {
          val exprNode = ExpressionConverter
            .replaceWithExpressionTransformer(aggExpr.filter.get, expandExecTransformer.output)
            .doTransform(args)
          aggFilterList.add(exprNode)
        } else {
          // The number of filters should be aligned with that of aggregate functions.
          aggFilterList.add(null)
        }
        val aggregateFunc = aggExpr.aggregateFunction
        val childrenNodes = aggExpr.mode match {
          case Partial | Complete =>
            aggregateFunc.children.toList.map(
              expr => {
                ExpressionConverter
                  .replaceWithExpressionTransformer(expr, originalInputAttributes)
                  .doTransform(args)
              })
          case PartialMerge | Final =>
            aggregateFunc.inputAggBufferAttributes.toList.map(
              attr => {
                ExpressionConverter
                  .replaceWithExpressionTransformer(attr, originalInputAttributes)
                  .doTransform(args)
              })
          case other =>
            throw new UnsupportedOperationException(s"$other not supported.")
        }
        addFunctionNode(
          args,
          aggregateFunc,
          childrenNodes.asJava,
          aggExpr.mode,
          aggregateFunctionList)
      })
    val extensionNode = getAdvancedExtension(validation, input, originalInputAttributes)
    RelBuilder.makeAggregateRel(
      input,
      groupingList,
      aggregateFunctionList,
      aggFilterList,
      extensionNode,
      context,
      operatorId)
  }

  private def getAdvancedExtension(
      validation: Boolean = false,
      expandNode: RelNode = null,
      originalInputAttributes: Seq[Attribute] = Seq.empty): AdvancedExtensionNode = {
    val enhancement = if (validation) {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = originalInputAttributes
        .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        .asJava
      BackendsApiManager.getTransformerApiInstance.packPBMessage(
        TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf)
    } else {
      null
    }
    val anyRel = Any.pack(expandNode.toProtobuf)
    ExtensionBuilder.makeAdvancedExtension(anyRel, enhancement)
  }

  def isStreaming: Boolean = false

  def numShufflePartitions: Option[Int] = Some(0)

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

}
