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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.metric.SQLMetric

class OmniSortMetricsUpdater(val metrics: Map[String, SQLMetric]) extends MetricsUpdater with Logging {
  override def updateNativeMetrics(opMetrics: IOperatorMetrics): Unit = {
    if (opMetrics != null) {
      val operatorMetrics = opMetrics.asInstanceOf[OperatorMetrics]
      metrics("numInputVectorBatches") += operatorMetrics.getNumInputVecBatches
      metrics("numInputRows") += operatorMetrics.getNumInputRows
      metrics("numInputBytes") += operatorMetrics.getNumInputBytes
      metrics("addInputCpuCount") += operatorMetrics.getInputCpuCount
      metrics("addInputTime") += operatorMetrics.getAddInputTime

      // todo omniCodegenTime
      metrics("numOutputRows") += operatorMetrics.getNumOutputRows
      metrics("numOutputBytes") += operatorMetrics.getNumOutputBytes
      metrics("numOutputVectorBatches") += operatorMetrics.getNumOutputVecBatches
      metrics("getOutputCpuCount") += operatorMetrics.getOutputCpuCount
      metrics("getOutputTime") += operatorMetrics.getGetOutputTime
    }
  }
}
