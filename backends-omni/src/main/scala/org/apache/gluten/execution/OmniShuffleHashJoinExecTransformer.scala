package org.apache.gluten.execution

import io.substrait.proto.JoinRel
import org.apache.gluten.extension.ValidationResult
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.SparkPlan

case class OmniShuffledHashJoinExecTransformer(
    leftKeys: Seq[Expression], 
    rightKeys: Seq[Expression], 
    joinType: JoinType, 
    buildSide: BuildSide, 
    condition: Option[Expression], 
    left: SparkPlan, 
    right: SparkPlan, 
    isSkewJoin: Boolean)
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
      if (needSwitchChildren) {
        JoinRel.JoinType.JOIN_TYPE_RIGHT
      } else {
        JoinRel.JoinType.JOIN_TYPE_LEFT
      }
    case RightOuter =>
      if (needSwitchChildren) {
        JoinRel.JoinType.JOIN_TYPE_LEFT
      } else {
        JoinRel.JoinType.JOIN_TYPE_RIGHT
      }
    case LeftSemi | ExistenceJoin(_) =>
      if (needSwitchChildren) {
        JoinRel.JoinType.JOIN_TYPE_RIGHT_SEMI
      } else {
        JoinRel.JoinType.JOIN_TYPE_LEFT_SEMI
      }
    case LeftAnti =>
      JoinRel.JoinType.JOIN_TYPE_LEFT_ANTI
    case _ =>
      JoinRel.JoinType.UNRECOGNIZED
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (joinType == LeftSemi || joinType == LeftAnti) {
      return ValidationResult.failed(s"LeftSemi and LeftAnti is not supported in OmniShuffledHashJoinExecTransformer")
    }

    super.doValidateInternal();
  }

  override protected def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan = 
    copy(left = newLeft, right = newRight)
}