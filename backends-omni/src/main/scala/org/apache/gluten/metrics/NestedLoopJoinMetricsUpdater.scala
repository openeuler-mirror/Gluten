package org.apache.gluten.metrics

import org.apache.spark.sql.execution.metric.SQLMetric

class NestedLoopJoinMetricsUpdater(val metrics: Map[String, SQLMetric]) extends MetricsUpdater {

  override def updateNativeMetrics(opMetrics: IOperatorMetrics): Unit = {
    if (opMetrics != null) {
      val operatorMetrics = opMetrics.asInstanceOf[OperatorMetrics]
      metrics("numOutputRows") += operatorMetrics.getOutputRows
      metrics("outputVectors") += operatorMetrics.getNumOutputVecBatches
      metrics("outputBytes") += operatorMetrics.getOutputBytes
      metrics("cpuCount") += operatorMetrics.getCpuCount
      metrics("cpuNanos") += operatorMetrics.getCpuNanos
      metrics("peakMemoryBytes") += operatorMetrics.getPeakMemoryBytes
      metrics("numMemoryAllocations") += operatorMetrics.getNumMemoryAllocations
    }
  }
}