package org.apache.gluten.execution

import io.substrait.proto.JoinRel
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

    super.doValidateInternal()
  }

  override protected def withNewChildrenInternal(
    newLeft: SparkPlan, 
    newRight: SparkPlan): OmniSortMergeJoinExecTransformer = 
    copy(left = newLeft, right = newRight)
}