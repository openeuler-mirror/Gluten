/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gluten.metrics

import org.apache.gluten.substrait.AggregationParams
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.utils.SparkMetricsUtil
import org.apache.spark.task.TaskResources

trait OmniHashAggregateMetricsUpdater extends MetricsUpdater {
  def updateAggregationMetrics(
    aggregationMetrics: java.util.ArrayList[OperatorMetrics],
    aggParams: AggregationParams): Unit
}

class OmniHashAggregateMetricsUpdaterImpl(val metrics: Map[String, SQLMetric])
  extends OmniHashAggregateMetricsUpdater {
  val numOutputRows: SQLMetric = metrics("numOutputRows")
  val numOutputVecBatches: SQLMetric = metrics("numOutputVecBatches")
  val numOutputBytes: SQLMetric = metrics("numOutputBytes")

  val numInputRows: SQLMetric = metrics("numInputRows")
  val numInputVecBatches: SQLMetric = metrics("numInputVecBatches")
  val numInputBytes: SQLMetric = metrics("numInputBytes")

  val addInputCount: SQLMetric = metrics("addInputCount")
  val addInputTime: SQLMetric = metrics("addInputTime")
  val getOutputCount: SQLMetric = metrics("getOutputCount")
  val getOutputTime: SQLMetric = metrics("getOutputTime")

  override def updateAggregationMetrics(
    aggregationMetrics: java.util.ArrayList[OperatorMetrics],
    aggParams: AggregationParams): Unit = {
    var idx = 0

    val aggMetrics = aggregationMetrics.get(idx)
    numOutputRows += aggMetrics.getNumOutputRows
    numOutputVecBatches += aggMetrics.getNumOutputVecBatches
    numOutputBytes += aggMetrics.getNumOutputBytes
    numInputRows += aggMetrics.getNumInputRows
    numInputVecBatches += aggMetrics.getNumInputVecBatches
    numInputBytes += aggMetrics.getNumInputBytes

    getOutputCount += aggMetrics.getOutputCpuCount
    getOutputTime += aggMetrics.getGetOutputTime
    addInputTime += aggMetrics.getAddInputTime
    addInputCount += aggMetrics.getInputCpuCount
    idx += 1

    if (TaskResources.inSparkTask()) {
      SparkMetricsUtil.incMemoryBytesSpilled(
        TaskResources.getLocalTaskContext().taskMetrics(),
        aggMetrics.getSpilledInputBytes)
      SparkMetricsUtil.incDiskBytesSpilled(
        TaskResources.getLocalTaskContext().taskMetrics(),
        aggMetrics.getSpilledBytes)
    }
  }
}



