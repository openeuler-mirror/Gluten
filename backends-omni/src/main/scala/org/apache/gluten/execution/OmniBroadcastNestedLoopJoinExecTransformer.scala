package org.apache.gluten.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.vectorized.ColumnarBatch

case class OmniBroadcastNestedLoopJoinExecTransformer(
    left: SparkPlan, 
    right: SparkPlan, 
    buildSide: BuildSide, 
    joinType: JoinType, 
    condition: Option[Expression])
  extends BroadcastNestedLoopJoinExecTransformer(
    left, 
    right, 
    buildSide, 
    joinType, 
    condition) {
    
  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    val streamedRDD = getColumnarInputRDDs(streamedPlan)
    val broadcast = buildPlan.executeBroadcast[BuildSideRelation]()
    val broadcastRDD = OmniBroadcastBuildSideRDD(sparkContext, broadcast)
    streamedRDD :+ broadcastRDD
  }

  override protected def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): OmniBroadcastNestedLoopJoinExecTransformer = 
    copy(left = newLeft, right = newRight)
}