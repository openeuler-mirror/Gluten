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
  val numOutputVectors: SQLMetric = metrics("numOutputVectors")
  val numOutputBytes: SQLMetric = metrics("numOutputBytes")

  final override def updateJoinMetrics(
    joinMetrics: util.ArrayList[OperatorMetrics], 
    singleMetrics: SingleMetric, 
    joinParams: JoinParams): Unit = {
    assert(joinParams.postProjectionNeeded)
    val postProjectMetrics = joinMetrics.remove(0);
    numOutputRows += postProjectMetrics.getOutputRows()
    numOutputVectors += postProjectMetrics.getNumOutputVecBatches()
    numOutputBytes += postProjectMetrics.getOutputBytes()

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

  val hashProbeInputRows: SQLMetric = metrics("hashProbeInputRows")
  val hashProbeOutputRows: SQLMetric = metrics("hashProbeOutputRows")
  val hashProbeNumInputVecBatches: SQLMetric = metrics("hashProbeNumInputVecBatches")
  val hashProbeNumOutputVecBatches: SQLMetric = metrics("hashProbeNumOutputVecBatches")
  val hashProbeAddInputTime: SQLMetric = metrics("hashProbeAddInputTime")
  val hashProbeGetOutputTime: SQLMetric = metrics("hashProbeGetOutputTime")

  override protected def updateJoinMetricsInternal(
    joinMetrics: util.ArrayList[OperatorMetrics], 
    joinParams: JoinParams): Unit = {
    var idx = 0;
    // HashProbe
    val hashProbeMetrics = joinMetrics.get(idx);
    hashProbeInputRows += hashProbeMetrics.getLookupInputRows
    hashProbeOutputRows += hashProbeMetrics.getLookupOutputRows
    hashProbeNumInputVecBatches += hashProbeMetrics.getLookupNumOutputVecBatches
    hashProbeNumOutputVecBatches += hashProbeMetrics.getLookupNumOutputVecBatches
    hashProbeAddInputTime += hashProbeMetrics.getLookupAddInputTime
    hashProbeGetOutputTime += hashProbeMetrics.getLookupGetOutputTime
    idx += 1

    // hashBuild
    val hashBuildMetrics = joinMetrics.get(idx)
    hashBuildInputRows += hashBuildMetrics.getBuildInputRows
    hashBuildNumInputVecBatches += hashBuildMetrics.getBuildNumInputVecBatches
    hashBuildAddInputTime += hashBuildMetrics.getBuildAddInputTime
    hashBuildGetOutputTime += hashBuildMetrics.getBuildGetOutputTime
    idx += 1

    if (TaskResources.inSparkTask()) {
      SparkMetricsUtil.incMemoryBytesSpilled(
        TaskResources.getLocalTaskContext().taskMetrics(), 
        hashProbeMetrics.getSpilledInputBytes())
      SparkMetricsUtil.incDiskBytesSpilled(
        TaskResources.getLocalTaskContext().taskMetrics(), 
        hashProbeMetrics.getSpilledBytes())
      SparkMetricsUtil.incMemoryBytesSpilled(
        TaskResources.getLocalTaskContext().taskMetrics(), 
        hashBuildMetrics.getSpilledInputBytes())
      SparkMetricsUtil.incDiskBytesSpilled(
        TaskResources.getLocalTaskContext().taskMetrics(), 
        hashBuildMetrics.getSpilledBytes())
    }
  }
}

class SortMergeJoinMetricsUpdater(override val metrics: Map[String, SQLMetric])
  extends JoinMetricsUpdaterBase(metrics) {
  val cpuCount: SQLMetric = metrics("cpuCount")
  val wallNanos: SQLMetric = metrics("wallNanos")
  val peakMemoryBytes: SQLMetric = metrics("peakMemoryBytes")
  val numMemoryAllocations: SQLMetric = metrics("numMemoryAllocations")

  val streamPreProjectionCpuCount: SQLMetric = metrics("streamPreProjectionCpuCount")
  val streamPreProjectionWallNanos: SQLMetric = metrics("streamPreProjectionWallNanos")
  val bufferPreProjectionCpuCount: SQLMetric = metrics("bufferPreProjectionCpuCount")
  val bufferPreProjectionWallNanos: SQLMetric = metrics("bufferPreProjectionWallNanos")

  override protected def updateJoinMetricsInternal(
    joinMetrics: util.ArrayList[OperatorMetrics], 
    joinParams: JoinParams): Unit = {
    var idx = 0
    val smjMetrics = joinMetrics.get(0)
    cpuCount += smjMetrics.getCpuCount()
    wallNanos += smjMetrics.getWallNanos()
    peakMemoryBytes += smjMetrics.getPeakMemoryBytes()
    numMemoryAllocations += smjMetrics.getNumMemoryAllocations()
    idx += 1

    if (joinParams.buildPreProjectionNeeded) {
      bufferPreProjectionCpuCount += joinMetrics.get(idx).getCpuCount()
      bufferPreProjectionWallNanos += joinMetrics.get(idx).getWallNanos()
      idx += 1
    }

    if (joinParams.streamPreProjectionNeeded) {
      streamPreProjectionCpuCount += joinMetrics.get(idx).getCpuCount()
      streamPreProjectionWallNanos += joinMetrics.get(idx).getWallNanos()
      idx += 1
    }
  }
}