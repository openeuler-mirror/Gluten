package org.apache.gluten.execution

import com.google.protobuf.{Any, StringValue}
import io.substrait.proto.JoinRel
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.extension.ValidationResult
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.vectorized.ColumnarBatch

case class OmniBroadcastHashJoinExecTransformer(
    leftKeys: Seq[Expression], 
    rightKeys: Seq[Expression], 
    joinType: JoinType, 
    buildSide: BuildSide, 
    condition: Option[Expression], 
    left: SparkPlan, 
    right: SparkPlan, 
    isNullAwareAntiJoin: Boolean)
  extends BroadcastHashJoinExecTransformerBase(
    leftKeys, 
    rightKeys, 
    joinType, 
    buildSide, 
    condition, 
    left, 
    right, 
    isNullAwareAntiJoin) {
    
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

  override protected def doValidateInternal(): ValidationResult = {
    if (isNullAwareAntiJoin) {
      return ValidationResult.failed(s"isNullAwareAntiJoin is not supported in OmniBroadcastHashJoinExecTransformer")
    }

    super.doValidateInternal();
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
    (1, if (isNullAwareAntiJoin) 1 else 0, buildHashTableId)
  }

  override protected def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan = 
    copy(left = newLeft, right = newRight)

  /**
   * Returns all the RDDs of ColumnarBatch which generates the input rows.
   *
   * @note
   * Right row we support up to two RDDs
   */
  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    val streamedRDD = getColumnarInputRDDs(streamedPlan)
    val broadcast = buildPlan.executeBroadcast[BuildSideRelation]()
    val broadcastRDD = OmniBroadcastBuildSideRDD(sparkContext, broadcast)
    streamedRDD :+ broadcastRDD
  }
}