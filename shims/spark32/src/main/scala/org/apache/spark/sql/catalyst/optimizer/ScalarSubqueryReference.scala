package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{ExprId, LeafExpression, Unevaluable}
import org.apache.spark.sql.types.DataType

case class ScalarSubqueryReference(
    subqueryIndex: Int,
    headerIndex: Int,
    dataType: DataType,
    exprId: ExprId)
  extends LeafExpression
  with Unevaluable {
  override def nullable: Boolean = true
}
