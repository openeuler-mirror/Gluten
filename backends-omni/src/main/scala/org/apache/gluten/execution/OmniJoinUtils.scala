package org.apache.gluten.execution

import com.google.protobuf.Any
import io.substrait.proto.JoinRel
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.expression.ExpressionTransformer
import org.apache.spark.sql.catalyst.expressions.NamedExpression

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.Seq
import org.apache.gluten.expression.{AttributeReferenceTransformer, ExpressionConverter}
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.ExpressionNode
import org.apache.gluten.substrait.extensions.{AdvancedExtensionNode, ExtensionBuilder}
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}
import org.apache.gluten.utils.SubstraitUtil
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.types.DataType

object OmniJoinUtils {
  private def transformKeys(
      keyExprs: Seq[Expression],
      inputNode: RelNode,
      inputNodeOutput: Seq[Attribute],
      substraitContext: SubstraitContext): (Seq[(ExpressionNode, DataType)], RelNode, Seq[Attribute]) = {
    val keys = keyExprs.map {
        case a: AttributeReference =>
          // The selection index for original AttributeReference is unchanged.
          (
            ExpressionConverter
              .replaceWithExpressionTransformer(a, inputNodeOutput)
              .asInstanceOf[AttributeReferenceTransformer]
              .doTransform(substraitContext.registeredFunction),
            a.dataType)
        case expr =>
          (
            ExpressionConverter
              .replaceWithExpressionTransformer(expr, inputNodeOutput)
              .doTransform(substraitContext.registeredFunction),
            expr.dataType)
    }
    (
      keys,
      inputNode,
      inputNodeOutput
    )
  }

  private def createJoinExtensionNode(
      joinParameters: Any,
      output: RelNode): AdvancedExtensionNode = {
    // Use field [optimization] in a extension node
    // to send some join parameters through Substrait plan.
    if (output == null) {
      ExtensionBuilder.makeAdvancedExtension(joinParameters, null)
    } else {
      val enhancement = BackendsApiManager.getTransformerApiInstance.packPBMessage(output.toProtobuf)
      ExtensionBuilder.makeAdvancedExtension(joinParameters, enhancement)
    }
  }

  // scalastyle:off argcount
  def createJoinRel(
      streamedKeyExprs: Seq[Expression],
      buildKeyExprs: Seq[Expression],
      condition: Option[Expression],
      substraitJoinType: JoinRel.JoinType,
      exchangeTable: Boolean,
      joinType: JoinType,
      joinParameters: Any,
      inputStreamedRelNode: RelNode,
      inputBuildRelNode: RelNode,
      inputStreamedOutput: Seq[Attribute],
      inputBuildOutput: Seq[Attribute],
      substraitContext: SubstraitContext,
      operatorId: java.lang.Long,
      projectList: Seq[NamedExpression],
      validation: Boolean = false
      ): RelNode = {
    // scalastyle:on argcount
    // transform join keys.
    val (streamedKeys, streamedRelNode, streamedOutput) = transformKeys(
      streamedKeyExprs,
      inputStreamedRelNode,
      inputStreamedOutput,
      substraitContext)

    val (buildKeys, buildRelNode, buildOutput) = transformKeys(
      buildKeyExprs,
      inputBuildRelNode,
      inputBuildOutput,
      substraitContext)

    // Combine join keys to make a single expression.
    val joinExpressionNode = streamedKeys
      .zip(buildKeys)
      .map {
        case ((leftKey, leftType), (rightKey, rightType)) =>
          HashJoinLikeExecTransformer.makeEqualToExpression(
            leftKey,
            leftType,
            rightKey,
            rightType,
            substraitContext.registeredFunction)
      }
      .reduce(
        (l, r) =>
          HashJoinLikeExecTransformer.makeAndExpression(l, r, substraitContext.registeredFunction))

    // Create post-join filter, which will be computed in hash join.
    val postJoinFilter =
      condition.map {
        SubstraitUtil.toSubstraitExpression(_, streamedOutput ++ buildOutput, substraitContext)
      }

    val args = substraitContext.registeredFunction
    var attributeSeq: Seq[Attribute] = null
    if (exchangeTable) {
      attributeSeq = buildOutput ++ streamedOutput
    } else {
      attributeSeq = streamedOutput ++ buildOutput
    }
    var relNode: RelNode = null
    if (projectList != null) {
      val columnarProjExprs: Seq[ExpressionTransformer] = ExpressionConverter
        .replaceWithExpressionTransformer(projectList, attributeSeq)
      val projExprNodeList = columnarProjExprs.map(_.doTransform(args)).asJava

      relNode = RelBuilder.makeProjectRel(
        null,
        projExprNodeList,
        substraitContext,
        operatorId,
        false
      )
    }

    // Create JoinRel.
    val joinRel = RelBuilder.makeJoinRel(
      streamedRelNode,
      buildRelNode,
      substraitJoinType,
      joinExpressionNode,
      postJoinFilter.orNull,
      createJoinExtensionNode(joinParameters, relNode),
      substraitContext,
      operatorId
    )

    joinRel
  }
}