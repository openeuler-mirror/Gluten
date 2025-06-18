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
  val aggOutputRows: SQLMetric = metrics("aggOutputRows")
  val aggOutputVectors: SQLMetric = metrics("aggOutputVectors")
  val aggOutputBytes: SQLMetric = metrics("aggOutputBytes")
  val aggCpuCount: SQLMetric = metrics("aggCpuCount")
  val aggWallNanos: SQLMetric = metrics("aggCpuNanos")

  val rowConstructionCpuCount: SQLMetric = metrics("rowConstructionCpuCount")
  val rowConstructionWallNanos: SQLMetric = metrics("rowConstructionCpuNanos")

  val extractionCpuCount: SQLMetric = metrics("extractionCpuCount")
  val extractionWallNanos: SQLMetric = metrics("extractionCpuNanos")

  override def updateAggregationMetrics(
    aggregationMetrics: java.util.ArrayList[OperatorMetrics],
    aggParams: AggregationParams): Unit = {
    var idx = 0

    if (aggParams.extractionNeeded) {
      extractionCpuCount += aggregationMetrics.get(idx).getCpuCount
      extractionWallNanos += aggregationMetrics.get(idx).getCpuNanos
      idx += 1
    }

    val aggMetrics = aggregationMetrics.get(idx)
    aggOutputRows += aggMetrics.getOutputRows
    aggOutputVectors += aggMetrics.getNumOutputVecBatches
    aggOutputBytes += aggMetrics.getOutputBytes
    aggCpuCount += aggMetrics.getCpuCount
    aggWallNanos += aggMetrics.getCpuNanos
    idx += 1

    if (aggParams.rowConstructionNeeded) {
      rowConstructionCpuCount += aggregationMetrics.get(idx).getCpuCount
      rowConstructionWallNanos += aggregationMetrics.get(idx).getCpuNanos
      idx += 1
    }
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



