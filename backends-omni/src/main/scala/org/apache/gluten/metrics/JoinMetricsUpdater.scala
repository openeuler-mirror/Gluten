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
  val postProjectionCpuCount: SQLMetric = metrics("postProjectionCpuCount")
  val postProjectionWallNanos: SQLMetric = metrics("postProjectionWallNanos")
  val numOutputRows: SQLMetric = metrics("numOutputRows")
  val numOutputVectors: SQLMetric = metrics("numOutputVectors")
  val numOutputBytes: SQLMetric = metrics("numOutputBytes")

  final override def updateJoinMetrics(
    joinMetrics: util.ArrayList[OperatorMetrics], 
    singleMetrics: SingleMetric, 
    joinParams: JoinParams): Unit = {
    assert(joinParams.postProjectionNeeded)
    val postProjectMetrics = joinMetrics.remove(0);
    postProjectionCpuCount += postProjectMetrics.getCpuCount()
    postProjectionWallNanos += postProjectMetrics.getWallNanos()
    numOutputRows += postProjectMetrics.getOutputRows()
    numOutputVectors += postProjectMetrics.getOutputVectors()
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
  val hashBuildOutputRows: SQLMetric = metrics("hashBuildOutputRows")
  val hashBuildOutputVectors: SQLMetric = metrics("hashBuildOutputVectors")
  val hashBuildOutputBytes: SQLMetric = metrics("hashBuildOutputBytes")
  val hashBuildCpuCount: SQLMetric = metrics("hashBuildCpuCount")
  val hashBuildWallNanos: SQLMetric = metrics("hashBuildWallNanos")
  val hashBuildPeakMemoryBytes: SQLMetric = metrics("hashBuildPeakMemoryBytes")
  val hashBuildNumMemoryAllocations: SQLMetric = metrics("hashBuildNumMemoryAllocations")
  val hashBuildSpilledBytes: SQLMetric = metrics("hashBuildSpilledBytes")
  val hashBuildSpilledRows: SQLMetric = metrics("hashBuildSpilledRows")
  val hashBuildSpilledPartitions: SQLMetric = metrics("hashBuildSpilledPartitions")
  val hashBuildSpilledFiles: SQLMetric = metrics("hashBuildSpilledFiles")

  val hashProbeInputRows: SQLMetric = metrics("hashProbeInputRows")
  val hashProbeOutputRows: SQLMetric = metrics("hashProbeOutputRows")
  val hashProbeOutputVectors: SQLMetric = metrics("hashProbeOutputVectors")
  val hashProbeOutputBytes: SQLMetric = metrics("hashProbeOutputBytes")
  val hashProbeCpuCount: SQLMetric = metrics("hashProbeCpuCount")
  val hashProbeWallNanos: SQLMetric = metrics("hashProbeWallNanos")
  val hashProbePeakMemoryBytes: SQLMetric = metrics("hashProbePeakMemoryBytes")
  val hashProbeNumMemoryAllocations: SQLMetric = metrics("hashProbeNumMemoryAllocations")
  val hashProbeSpilledBytes: SQLMetric = metrics("hashProbeSpilledBytes")
  val hashProbeSpilledRows: SQLMetric = metrics("hashProbeSpilledRows")
  val hashProbeSpilledPartitions: SQLMetric = metrics("hashProbeSpilledPartitions")
  val hashProbeSpilledFiles: SQLMetric = metrics("hashProbeSpilledFiles")

  // The number of rows which were passed through without any processing
  // after filter was pushed down.
  val hashProbeReplacedWithDynamicFilterRows: SQLMetric = metrics("hashProbeReplacedWithDynamicFilterRows")

  // The number of dynamic filters this join generated for push down.
  val hashProbeDynamicFiltersProduced: SQLMetric = metrics("hashProbeDynamicFiltersProduced")

  val streamPreProjectionCpuCount: SQLMetric = metrics("streamPreProjectionCpuCount")
  val streamPreProjectionWallNanos: SQLMetric = metrics("streamPreProjectionWallNanos")

  val buildPreProjectionCpuCount: SQLMetric = metrics("buildPreProjectionCpuCount")
  val buildPreProjectionWallNanos: SQLMetric = metrics("buildPreProjectionWallNanos")

  override protected def updateJoinMetricsInternal(
    joinMetrics: util.ArrayList[OperatorMetrics], 
    joinParams: JoinParams): Unit = {
    var idx = 0;
    // HashProbe
    val hashProbeMetrics = joinMetrics.get(idx);
    hashProbeInputRows += hashProbeMetrics.getInputRows()
    hashProbeOutputRows += hashProbeMetrics.getOutputRows()
    hashProbeOutputVectors += hashProbeMetrics.getOutputVectors()
    hashProbeOutputBytes += hashProbeMetrics.getOutputBytes()
    hashProbeCpuCount += hashProbeMetrics.getCpuCount()
    hashProbeWallNanos += hashProbeMetrics.getWallNanos()
    hashProbePeakMemoryBytes += hashProbeMetrics.getPeakMemoryBytes()
    hashProbeNumMemoryAllocations += hashProbeMetrics.getNumMemoryAllocations()
    hashProbeSpilledBytes += hashProbeMetrics.getSpilledBytes()
    hashProbeSpilledRows += hashProbeMetrics.getSpilledRows()
    hashProbeSpilledPartitions += hashProbeMetrics.getSpilledPartitions()
    hashProbeSpilledFiles += hashProbeMetrics.getSpilledFiles()
    hashProbeReplacedWithDynamicFilterRows += hashProbeMetrics.getNumReplacedWithDynamicFilterRows()
    hashProbeDynamicFiltersProduced += hashProbeMetrics.getNumDynamicFiltersProduced()
    idx += 1

    // hashBuild
    val hashBuildMetrics = joinMetrics.get(idx)
    hashBuildInputRows += hashBuildMetrics.getInputRows()
    hashBuildOutputRows += hashBuildMetrics.getOutputRows()
    hashProbeOutputVectors += hashBuildMetrics.getOutputVectors()
    hashBuildOutputBytes += hashBuildMetrics.getOutputBytes()
    hashBuildCpuCount += hashBuildMetrics.getCpuCount()
    hashBuildWallNanos += hashBuildMetrics.getWallNanos()
    hashBuildPeakMemoryBytes += hashBuildMetrics.getPeakMemoryBytes()
    hashBuildNumMemoryAllocations += hashBuildMetrics.getNumMemoryAllocations()
    hashBuildSpilledBytes += hashBuildMetrics.getSpilledBytes()
    hashBuildSpilledRows += hashBuildMetrics.getSpilledRows()
    hashBuildSpilledPartitions += hashBuildMetrics.getSpilledPartitions()
    hashBuildSpilledFiles += hashBuildMetrics.getSpilledFiles()
    idx += 1

    if (joinParams.buildPreProjectionNeeded) {
      buildPreProjectionCpuCount += joinMetrics.get(idx).getCpuCount()
      buildPreProjectionWallNanos += joinMetrics.get(idx).getWallNanos()
      idx += 1
    }

    if (joinParams.streamPreProjectionNeeded) {
      streamPreProjectionCpuCount += joinMetrics.get(idx).getCpuCount()
      streamPreProjectionWallNanos += joinMetrics.get(idx).getWallNanos()
      idx += 1
    }

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