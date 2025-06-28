package org.apache.gluten.metrics

import org.apache.gluten.metrics.Metrics.SingleMetric
import org.apache.gluten.substrait.JoinParams
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.utils.SparkMetricsUtil
import org.apache.spark.task.TaskResources

import java.util

trait JoinMetricsUpdater extends MetricsUpdater {
  def updateJoinMetrics(
    joinMetrics: java.util.ArrayList[OperatorMetrics], 
    singleMetrics: SingleMetric, 
    joinParams: JoinParams): Unit
}

abstract class JoinMetricsUpdaterBase(val metrics: Map[String, SQLMetric]) 
  extends JoinMetricsUpdater {
  val numOutputRows: SQLMetric = metrics("numOutputRows")
  val numOutputVectors: SQLMetric = metrics("numOutputVectorBatches")
  val numOutputBytes: SQLMetric = metrics("numOutputBytes")

  final override def updateJoinMetrics(
    joinMetrics: util.ArrayList[OperatorMetrics], 
    singleMetrics: SingleMetric, 
    joinParams: JoinParams): Unit = {
    val postProjectMetrics = joinMetrics.remove(0);
    numOutputRows += postProjectMetrics.getNumOutputRows
    numOutputVectors += postProjectMetrics.getNumOutputVecBatches
    numOutputBytes += postProjectMetrics.getNumOutputBytes

    updateJoinMetricsInternal(joinMetrics, joinParams)
  }

  protected def updateJoinMetricsInternal(
    joinMetrics: util.ArrayList[OperatorMetrics], 
    joinParams: JoinParams): Unit
}

class HashJoinMetricsUpdater(override val metrics: Map[String, SQLMetric])
  extends JoinMetricsUpdaterBase(metrics) {
  val hashBuildInputRows: SQLMetric = metrics("hashBuildInputRows")
  val hashBuildNumInputVecBatches: SQLMetric = metrics("hashBuildNumInputVecBatches")
  val hashBuildAddInputTime: SQLMetric = metrics("hashBuildAddInputTime")
  val hashBuildGetOutputTime: SQLMetric = metrics("hashBuildGetOutputTime")

  val lookUpJoinInputRows: SQLMetric = metrics("lookUpJoinInputRows")
  val lookUpJoinOutputRows: SQLMetric = metrics("lookUpJoinOutputRows")
  val lookUpJoinNumInputVecBatches: SQLMetric = metrics("lookUpJoinNumInputVecBatches")
  val lookUpJoinNumOutputVecBatches: SQLMetric = metrics("lookUpJoinNumOutputVecBatches")
  val lookUpJoinAddInputTime: SQLMetric = metrics("lookUpJoinAddInputTime")
  val lookUpJoinGetOutputTime: SQLMetric = metrics("lookUpJoinGetOutputTime")

  override protected def updateJoinMetricsInternal(
    joinMetrics: util.ArrayList[OperatorMetrics], 
    joinParams: JoinParams): Unit = {
    var idx = 0;
    // hashBuild
    val hashBuildMetrics = joinMetrics.get(idx)
    hashBuildInputRows += hashBuildMetrics.getBuildInputRows
    hashBuildNumInputVecBatches += hashBuildMetrics.getBuildNumInputVecBatches
    hashBuildAddInputTime += hashBuildMetrics.getBuildAddInputTime
    hashBuildGetOutputTime += hashBuildMetrics.getBuildGetOutputTime

    idx += 1
    // lookUpJoin
    val hashProbeMetrics = joinMetrics.get(idx);
    lookUpJoinInputRows += hashProbeMetrics.getLookupInputRows
    lookUpJoinOutputRows += hashProbeMetrics.getLookupOutputRows
    lookUpJoinNumInputVecBatches += hashProbeMetrics.getLookupNumOutputVecBatches
    lookUpJoinNumOutputVecBatches += hashProbeMetrics.getLookupNumOutputVecBatches
    lookUpJoinAddInputTime += hashProbeMetrics.getLookupAddInputTime
    lookUpJoinGetOutputTime += hashProbeMetrics.getLookupGetOutputTime

    if (TaskResources.inSparkTask()) {
      SparkMetricsUtil.incMemoryBytesSpilled(
        TaskResources.getLocalTaskContext().taskMetrics(), 
        hashProbeMetrics.getSpilledInputBytes)
      SparkMetricsUtil.incDiskBytesSpilled(
        TaskResources.getLocalTaskContext().taskMetrics(), 
        hashProbeMetrics.getSpilledBytes)
      SparkMetricsUtil.incMemoryBytesSpilled(
        TaskResources.getLocalTaskContext().taskMetrics(), 
        hashBuildMetrics.getSpilledInputBytes)
      SparkMetricsUtil.incDiskBytesSpilled(
        TaskResources.getLocalTaskContext().taskMetrics(), 
        hashBuildMetrics.getSpilledBytes)
    }
  }
}

class SortMergeJoinMetricsUpdater(override val metrics: Map[String, SQLMetric])
  extends JoinMetricsUpdaterBase(metrics) {

  override protected def updateJoinMetricsInternal(
    joinMetrics: util.ArrayList[OperatorMetrics],
    joinParams: JoinParams): Unit = {
    val numOutputRows: SQLMetric = metrics("numOutputRows")
    val numOutputVectorBatches: SQLMetric = metrics("numOutputVectorBatches")
    val numOutputBytes: SQLMetric = metrics("numOutputBytes")
    val getOutputCpuCount: SQLMetric = metrics("getOutputCpuCount")
    val getOutputTime: SQLMetric = metrics("getOutputTime")

    val addInputTime: SQLMetric = metrics("addInputTime")
    val addInputCpuCount: SQLMetric = metrics("addInputCpuCount")
    val numInputVectorBatches: SQLMetric = metrics("numInputVectorBatches")
    val numInputRows: SQLMetric = metrics("numInputRows")
    val numInputBytes: SQLMetric = metrics("numInputBytes")


    val idx = 0
    val operatorMetrics = joinMetrics.get(idx)
    numOutputRows += operatorMetrics.getNumOutputRows
    numOutputVectorBatches += operatorMetrics.getNumOutputVecBatches
    numOutputBytes += operatorMetrics.getNumOutputBytes
    getOutputCpuCount += operatorMetrics.getOutputCpuCount
    getOutputTime += operatorMetrics.getGetOutputTime

    addInputTime += operatorMetrics.getAddInputTime
    addInputCpuCount += operatorMetrics.getInputCpuCount
    numInputVectorBatches += operatorMetrics.getBuildNumInputVecBatches
    numInputRows += operatorMetrics.getNumInputRows
    numInputBytes += operatorMetrics.getNumInputBytes
  }
}