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
  val numOutputBytes: SQLMetric = metrics("numOutputBytes")

  final override def updateJoinMetrics(
    joinMetrics: util.ArrayList[OperatorMetrics], 
    singleMetrics: SingleMetric, 
    joinParams: JoinParams): Unit = {

    updateJoinMetricsInternal(joinMetrics, joinParams)
  }

  protected def updateJoinMetricsInternal(
    joinMetrics: util.ArrayList[OperatorMetrics], 
    joinParams: JoinParams): Unit
}

class HashJoinMetricsUpdater(override val metrics: Map[String, SQLMetric])
  extends JoinMetricsUpdaterBase(metrics) {
  val numOutputVectorBatches: SQLMetric = metrics("numOutputVectorBatches")

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

  val numSpilledBytes: SQLMetric = metrics("numSpilledBytes")
  val numSpilledRows: SQLMetric = metrics("numSpilledRows")
  val numSpilledFiles: SQLMetric = metrics("numSpilledFiles")

  override protected def updateJoinMetricsInternal(
    joinMetrics: util.ArrayList[OperatorMetrics],
    joinParams: JoinParams): Unit = {
    if (joinMetrics == null || joinMetrics.isEmpty) {
      return
    }

    // Native exports HashBuilder and LookUpJoin as two suites of the same HashJoin node.
    // Each suite fills only its own lookup* or build* fields; unordered_map order is not
    // probe-then-build. Sum specialized fields across all suites (extra non-join suites
    // leave lookup*/build* at 0).
    var lookupInputRows = 0L
    var lookupOutputRows = 0L
    var lookupNumInputVecBatches = 0L
    var lookupNumOutputVecBatches = 0L
    var lookupAddInputTime = 0L
    var lookupGetOutputTime = 0L
    var lookupOutputBytes = 0L
    var buildInputRows = 0L
    var buildNumInputVecBatches = 0L
    var buildAddInputTime = 0L
    var buildGetOutputTime = 0L
    var spilledBytes = 0L
    var spilledRows = 0L
    var spilledFiles = 0L
    var spilledInputBytes = 0L

    var i = 0
    while (i < joinMetrics.size()) {
      val m = joinMetrics.get(i)
      lookupInputRows += m.getLookupInputRows
      lookupOutputRows += m.getLookupOutputRows
      lookupNumInputVecBatches += m.getLookupNumInputVecBatches
      lookupNumOutputVecBatches += m.getLookupNumOutputVecBatches
      lookupAddInputTime += m.getLookupAddInputTime
      lookupGetOutputTime += m.getLookupGetOutputTime
      if (m.getLookupOutputRows > 0) {
        lookupOutputBytes += m.getNumOutputBytes
      }
      buildInputRows += m.getBuildInputRows
      buildNumInputVecBatches += m.getBuildNumInputVecBatches
      buildAddInputTime += m.getBuildAddInputTime
      buildGetOutputTime += m.getBuildGetOutputTime
      spilledBytes += m.getSpilledBytes
      spilledRows += m.getSpilledRows
      spilledFiles += m.getSpilledFiles
      spilledInputBytes += m.getSpilledInputBytes
      i += 1
    }

    if (lookupInputRows == 0L && lookupOutputRows == 0L && buildInputRows == 0L &&
        joinMetrics.size() >= 2) {
      val a = joinMetrics.get(0)
      val b = joinMetrics.get(1)
      val (probe, build) =
        if (a.getNumOutputRows >= b.getNumOutputRows) (a, b) else (b, a)
      lookupInputRows = probe.getNumInputRows
      lookupOutputRows = probe.getNumOutputRows
      lookupNumInputVecBatches = probe.getNumInputVecBatches
      lookupNumOutputVecBatches = probe.getNumOutputVecBatches
      lookupAddInputTime = probe.getAddInputTime
      lookupGetOutputTime = probe.getGetOutputTime
      lookupOutputBytes = probe.getNumOutputBytes
      buildInputRows = build.getNumInputRows
      buildNumInputVecBatches = build.getNumInputVecBatches
      buildAddInputTime = build.getAddInputTime
      buildGetOutputTime = build.getGetOutputTime
    }

    lookUpJoinInputRows += lookupInputRows
    lookUpJoinOutputRows += lookupOutputRows
    lookUpJoinNumInputVecBatches += lookupNumInputVecBatches
    lookUpJoinNumOutputVecBatches += lookupNumOutputVecBatches
    lookUpJoinAddInputTime += lookupAddInputTime
    lookUpJoinGetOutputTime += lookupGetOutputTime
    hashBuildInputRows += buildInputRows
    hashBuildNumInputVecBatches += buildNumInputVecBatches
    hashBuildAddInputTime += buildAddInputTime
    hashBuildGetOutputTime += buildGetOutputTime
    numOutputRows += lookupOutputRows
    numOutputBytes += lookupOutputBytes
    numOutputVectorBatches += lookupNumOutputVecBatches
    numSpilledBytes += spilledBytes
    numSpilledRows += spilledRows
    numSpilledFiles += spilledFiles

    if (TaskResources.inSparkTask()) {
      SparkMetricsUtil.incMemoryBytesSpilled(
        TaskResources.getLocalTaskContext().taskMetrics(),
        spilledInputBytes)
      SparkMetricsUtil.incDiskBytesSpilled(
        TaskResources.getLocalTaskContext().taskMetrics(),
        spilledBytes)
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