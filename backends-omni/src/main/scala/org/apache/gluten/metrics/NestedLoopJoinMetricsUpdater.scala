package org.apache.gluten.metrics

import org.apache.spark.sql.execution.metric.SQLMetric

class NestedLoopJoinMetricsUpdater(val metrics: Map[String, SQLMetric]) extends MetricsUpdater {

  override def updateNativeMetrics(opMetrics: IOperatorMetrics): Unit = {
    if (opMetrics != null) {
      val operatorMetrics = opMetrics.asInstanceOf[OperatorMetrics]
      metrics("numOutputRows") += operatorMetrics.getNumOutputRows
      metrics("numOutputVectorBatches") += operatorMetrics.getNumOutputVecBatches
      metrics("numOutputBytes") += operatorMetrics.getNumOutputBytes

      metrics("numInputRows") += operatorMetrics.getNumInputRows
      metrics("numInputVectorBatches") += operatorMetrics.getNumInputVecBatches
      metrics("numInputBytes") += operatorMetrics.getNumInputBytes

      metrics("getOutputCpuCount") += operatorMetrics.getOutputCpuCount
      metrics("addInputCpuCount") += operatorMetrics.getInputCpuCount
      metrics("getOutputTime") += operatorMetrics.getGetOutputTime
      metrics("addInputTime") += operatorMetrics.getAddInputTime
    }
  }
}