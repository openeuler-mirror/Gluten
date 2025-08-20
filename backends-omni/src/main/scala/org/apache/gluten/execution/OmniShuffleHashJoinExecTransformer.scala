package org.apache.gluten.execution

import com.google.protobuf.{Any, StringValue}
import io.substrait.proto.JoinRel
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.substrait.{JoinParams, SubstraitContext}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.Seq

case class OmniShuffledHashJoinExecTransformer(
    leftKeys: Seq[Expression], 
    rightKeys: Seq[Expression], 
    joinType: JoinType, 
    buildSide: BuildSide, 
    condition: Option[Expression], 
    left: SparkPlan, 
    right: SparkPlan, 
    isSkewJoin: Boolean,
    projectList: Seq[NamedExpression])
  extends ShuffledHashJoinExecTransformerBase(
    leftKeys, 
    rightKeys, 
    joinType, 
    buildSide, 
    condition, 
    left, 
    right, 
    isSkewJoin) {
    
  override protected lazy val substraitJoinType: JoinRel.JoinType = joinType match {
    case _: InnerLike =>
      JoinRel.JoinType.JOIN_TYPE_INNER
    case FullOuter => 
      JoinRel.JoinType.JOIN_TYPE_OUTER
    case LeftOuter =>
      JoinRel.JoinType.JOIN_TYPE_LEFT
    case RightOuter =>
      JoinRel.JoinType.JOIN_TYPE_RIGHT
    case LeftSemi | ExistenceJoin(_) =>
      JoinRel.JoinType.JOIN_TYPE_LEFT_SEMI
    case LeftAnti =>
      JoinRel.JoinType.JOIN_TYPE_LEFT_ANTI
    case _ =>
      JoinRel.JoinType.UNRECOGNIZED
  }

  private lazy val isBuildLeft: Int = buildSide match {
    case BuildLeft => 1
    case BuildRight => 0
  }

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val streamedPlanContext = streamedPlan.asInstanceOf[TransformSupport].transform(context)
    val (inputStreamedRelNode, inputStreamedOutput) =
      (streamedPlanContext.root, streamedPlanContext.outputAttributes)
    
    val buildPlanContext = buildPlan.asInstanceOf[TransformSupport].transform(context)
    val (inputBuildRelNode, inputBuildOutput) =
      (buildPlanContext.root, buildPlanContext.outputAttributes)

    // Get the operator id of this Join.
    val operatorId = context.nextOperatorId(this.nodeName)

    val joinParams = new JoinParams
    joinParams.streamPreProjectionNeeded = false
    joinParams.buildPreProjectionNeeded = false
    joinParams.isBHJ = false

    if (condition.isDefined) {
      joinParams.isWithCondition = true
    }

    val joinRel = OmniJoinUtils.createJoinRel(
      streamedKeyExprs,
      buildKeyExprs,
      condition,
      substraitJoinType,
      needSwitchChildren,
      joinType,
      genJoinParameters(),
      inputStreamedRelNode,
      inputBuildRelNode,
      inputStreamedOutput,
      inputBuildOutput,
      context,
      operatorId,
      projectList
    )

    context.registerJoinParam(operatorId, joinParams)

    JoinUtils.createTransformContext(
      needSwitchChildren,
      output,
      joinRel,
      inputStreamedOutput,
      inputBuildOutput
    )
  }

  override def output: Seq[Attribute] = {
    if (projectList == null) {
      super.output
    } else {
      projectList.map(f => f.toAttribute)
    }
  }

  override def genJoinParameters(): Any = {
    val (isBHJ, isNullAwareAntiJoin, buildHashTableId) = genJoinParametersInternal()
    // Start with "JoinParameters:"
    val joinParametersStr = new StringBuffer("JoinParameters:")
    // isBHJ: 0 for SHJ, 1 for BHJ
    // isNullAwareAntiJoin: 0 for false, 1 for true
    // buildHashTableId: the unique id for the hash table of build plan
    // isBuildLeft: 0 for BuildLeft, 1 for BuildRight
    joinParametersStr
      .append("isBHJ=")
      .append(isBHJ)
      .append("\n")
      .append("isNullAwareAntiJoin=")
      .append(isNullAwareAntiJoin)
      .append("\n")
      .append("buildHashTableId=")
      .append(buildHashTableId)
      .append("\n")
      .append("isBuildLeft=")
      .append(isBuildLeft)
      .append("\n")
      .append("isExistenceJoin=")
      .append(if (joinType.isInstanceOf[ExistenceJoin]) 1 else 0)
      .append("\n")
    val message = StringValue
      .newBuilder()
      .setValue(joinParametersStr.toString)
      .build()
    BackendsApiManager.getTransformerApiInstance.packPBMessage(message)
  }

  override def genJoinParametersInternal(): (Int, Int, String) = {
    (0, 0, "")
  }

  override protected def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan = 
    copy(left = newLeft, right = newRight)
}