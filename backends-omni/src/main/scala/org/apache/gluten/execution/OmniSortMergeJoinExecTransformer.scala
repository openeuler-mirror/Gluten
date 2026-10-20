package org.apache.gluten.execution

import io.substrait.proto.JoinRel
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.backendsapi.omni.OmniValidatorApi
import org.apache.gluten.extension.ValidationResult
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.SparkPlan

case class OmniSortMergeJoinExecTransformer(
    leftKeys: Seq[Expression], 
    rightKeys: Seq[Expression], 
    joinType: JoinType, 
    condition: Option[Expression], 
    left: SparkPlan, 
    right: SparkPlan, 
    isSkewJoin: Boolean = false, 
    projectList: Seq[NamedExpression] = null)
  extends SortMergeJoinExecTransformerBase(
    leftKeys, 
    rightKeys, 
    joinType, 
    condition, 
    left, 
    right, 
    isSkewJoin, 
    projectList) {
    
  override protected def doValidateInternal(): ValidationResult = {
    if (substraitJoinType == JoinRel.JoinType.JOIN_TYPE_RIGHT) {
      return ValidationResult
        .failed(s"SMJ unsupported join type of $joinType for substrait: $substraitJoinType")
    }

    val validator = BackendsApiManager.getValidatorApiInstance.asInstanceOf[OmniValidatorApi]
    validator.doSortMergeJoinValidate(leftKeys, rightKeys, left, right) match {
      case Some(reason) =>
        ValidationResult.failed(s"Found schema check failure for SMJ due to: $reason")
      case None =>
        super.doValidateInternal()
    }
  }

  override protected def withNewChildrenInternal(
    newLeft: SparkPlan, 
    newRight: SparkPlan): OmniSortMergeJoinExecTransformer = 
    copy(left = newLeft, right = newRight)
}