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

import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.utils.SparkInputMetricsUtil.InputMetricsWrapper

class OmniHiveTableScanMetricsUpdater(@transient val metrics: Map[String, SQLMetric])
  extends MetricsUpdater {

  val outputRows: SQLMetric = metrics("numOutputRows")
  val outputVectors: SQLMetric = metrics("outputVectors")
  val outputBytes: SQLMetric = metrics("outputBytes")
  val numInputBytes: SQLMetric = metrics("numInputBytes")
  val totalScanTime: SQLMetric = metrics("totalScanTime")

  override def updateInputMetrics(inputMetrics: InputMetricsWrapper): Unit = {
    inputMetrics.bridgeIncBytesRead(numInputBytes.value)
    inputMetrics.bridgeIncRecordsRead(outputRows.value)
  }

  override def updateNativeMetrics(opMetrics: IOperatorMetrics): Unit = {
    if (opMetrics != null) {
      val m = opMetrics.asInstanceOf[OperatorMetrics]
      outputRows += m.getNumOutputRows
      outputVectors += m.getNumOutputVecBatches
      OmniRowCountPerVecBatchMetrics.update(metrics, m.getNumOutputRows, m.getNumOutputVecBatches)
      outputBytes += m.getNumOutputBytes
      numInputBytes += m.getNumInputBytes
      totalScanTime += m.getScanTime
    }
  }
}
