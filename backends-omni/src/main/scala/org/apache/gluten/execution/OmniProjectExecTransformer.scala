package org.apache.gluten.execution

import org.apache.gluten.extension.ValidationResult
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.execution.SparkPlan

case class OmniProjectExecTransformer(projectList: Seq[NamedExpression], child: SparkPlan)
  extends ProjectExecTransformerBase(projectList, child) {

  override protected def withNewChildInternal(newChild: SparkPlan): OmniProjectExecTransformer =
    copy(child = newChild)


  override protected def doValidateInternal(): ValidationResult = {
    ValidationResult.failed("project unsupported yet")
  }

}