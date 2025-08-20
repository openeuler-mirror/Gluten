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

case class OmniInputIteratorMetricsUpdater(metrics: Map[String, SQLMetric], forBroadcast: Boolean)
  extends MetricsUpdater {
  override def updateNativeMetrics(opMetrics: IOperatorMetrics): Unit = {
    if (opMetrics != null) {
      val operatorMetrics = opMetrics.asInstanceOf[OperatorMetrics]
      metrics("numInputRows") += operatorMetrics.getNumInputRows
      metrics("numInputVectorBatches") += operatorMetrics.getNumInputVecBatches
      metrics("numInputBytes") += operatorMetrics.getNumInputBytes
      metrics("addInputCpuCount") += operatorMetrics.getInputCpuCount
      metrics("addInputTime") += operatorMetrics.getAddInputTime

      if (!forBroadcast) {
        metrics("numOutputRows") += operatorMetrics.getNumOutputRows
        metrics("numOutputVectorBatches") += operatorMetrics.getNumOutputVecBatches
        metrics("numOutputBytes") += operatorMetrics.getNumOutputBytes
        metrics("getOutputCpuCount") += operatorMetrics.getOutputCpuCount
        metrics("getOutputTime") += operatorMetrics.getGetOutputTime
      }
    }
  }
}
