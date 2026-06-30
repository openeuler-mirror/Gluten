package org.apache.spark.sql.execution.adaptive

import org.apache.gluten.config.GlutenConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.{ShufflePartitionSpec, SparkPlan}
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/** Spark AQE coalesce rule with an optional second pass for Omni. */
case class CoalesceShufflePartitions(session: SparkSession) extends AQEShuffleReadRule {
  override val supportedShuffleOrigins: Seq[ShuffleOrigin] =
    Seq(ENSURE_REQUIREMENTS, REPARTITION_BY_COL, REBALANCE_PARTITIONS_BY_NONE,
      REBALANCE_PARTITIONS_BY_COL)

  override def isSupported(shuffle: ShuffleExchangeLike): Boolean =
    shuffle.outputPartitioning != SinglePartition && super.isSupported(shuffle)

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.coalesceShufflePartitionsEnabled ||
        !plan.collectLeaves().forall(_.isInstanceOf[QueryStageExec])) return plan
    def collectInfos(p: SparkPlan): Seq[ShuffleStageInfo] = p match {
      case ShuffleStageInfo(stage, specs) => Seq(new ShuffleStageInfo(stage, specs))
      case _ => p.children.flatMap(collectInfos)
    }
    val infos = collectInfos(plan)
    if (!infos.forall(s => isSupported(s.shuffleStage.shuffle))) return plan
    val minNumPartitions = conf.getConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM).getOrElse {
      if (conf.getConf(SQLConf.COALESCE_PARTITIONS_PARALLELISM_FIRST)) session.sparkContext.defaultParallelism else 1
    }
    val advisoryTargetSize = conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES)
    val minPartitionSize = if (Utils.isTesting) {
      conf.getConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE).min(advisoryTargetSize / 5)
    } else conf.getConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE)
    val stats = infos.map(_.shuffleStage.mapStats)
    val inputs = infos.map(_.partitionSpecs)
    val first = ShufflePartitionsUtil.coalescePartitions(stats, inputs,
      advisoryTargetSize, minNumPartitions, minPartitionSize)
    val ratio = conf.getConf(GlutenConfig.COLUMNAR_OMNI_AQE_COALESCE_PARTITIONS_RATIO)
    val specs = if (first.nonEmpty && ratio != 1.0) {
      val base = first.map(_.size).min
      val original = infos.map(_.shuffleStage.shuffle.numPartitions).min
      val adjusted = math.max(1, math.min(original, math.round(base * ratio).toInt))
      ShufflePartitionsUtil.coalescePartitions(stats, inputs, advisoryTargetSize,
        adjusted, minPartitionSize)
    } else first
    if (specs.nonEmpty) updateShuffleReads(plan,
      infos.zip(specs).map { case (i, s) => i.shuffleStage.id -> s }.toMap) else plan
  }

  private def updateShuffleReads(plan: SparkPlan,
      specs: Map[Int, Seq[ShufflePartitionSpec]]): SparkPlan = plan match {
    case ShuffleStageInfo(stage, _) =>
      specs.get(stage.id).map(s => AQEShuffleReadExec(stage, s)).getOrElse(plan)
    case other => other.mapChildren(updateShuffleReads(_, specs))
  }
}

private class ShuffleStageInfo(val shuffleStage: ShuffleQueryStageExec,
    val partitionSpecs: Option[Seq[ShufflePartitionSpec]])
private object ShuffleStageInfo {
  def unapply(plan: SparkPlan): Option[(ShuffleQueryStageExec,
      Option[Seq[ShufflePartitionSpec]])] = plan match {
    case stage: ShuffleQueryStageExec => Some((stage, None))
    case AQEShuffleReadExec(s: ShuffleQueryStageExec, specs) => Some((s, Some(specs)))
    case _ => None
  }
}
