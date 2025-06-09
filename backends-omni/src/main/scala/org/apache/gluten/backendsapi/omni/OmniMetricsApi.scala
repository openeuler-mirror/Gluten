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
package org.apache.gluten.backendsapi.omni

import org.apache.gluten.backendsapi.MetricsApi
import org.apache.gluten.metrics.{NestedLoopJoinMetricsUpdater, _}
import org.apache.gluten.substrait.{AggregationParams, JoinParams}

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.utils.SparkInputMetricsUtil.InputMetricsWrapper

import java.lang.{Long => JLong}
import java.util.{List => JList, Map => JMap}

class MockMetricsUpdater(@transient val metrics: Map[String, SQLMetric] = Map.empty)
  extends MetricsUpdater {

  override def updateInputMetrics(inputMetrics: InputMetricsWrapper): Unit = {}

  override def updateNativeMetrics(opMetrics: IOperatorMetrics): Unit = {}
}

object MockMetricsUpdater {}

class OmniMetricsApiImpl extends MetricsApi with Logging {
  override def metricsUpdatingFunction(
      child: SparkPlan,
      relMap: JMap[JLong, JList[JLong]],
      joinParamsMap: JMap[JLong, JoinParams],
      aggParamsMap: JMap[JLong, AggregationParams]): IMetrics => Unit = {
    OmniMetricsUtil.genMetricsUpdatingFunction(child, relMap, joinParamsMap, aggParamsMap);
  }

  override def genBatchScanTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map.empty

  // Reuse OmniFileSourceScanMetricsUpdater for BatchScanTransformer
  override def genBatchScanTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new MockMetricsUpdater()

  override def genHiveTableScanTransformerMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] = Map.empty[String, SQLMetric]

  // Reuse OmniFileSourceScanMetricsUpdater for HiveTableScanTransformer
  override def genHiveTableScanTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new MockMetricsUpdater()

  override def genFileSourceScanTransformerMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] = Map.empty[String, SQLMetric]

  override def genFileSourceScanTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new MockMetricsUpdater()

  override def genFilterTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map.empty[String, SQLMetric]

  override def genFilterTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric],
      extraMetrics: Seq[(String, SQLMetric)] = Seq.empty): MetricsUpdater = new MockMetricsUpdater(
    metrics)
  override def genProjectTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map.empty[String, SQLMetric]

  override def genProjectTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric],
      extraMetrics: Seq[(String, SQLMetric)] = Seq.empty): MetricsUpdater = new MockMetricsUpdater()

  override def genHashAggregateTransformerMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] = Map.empty[String, SQLMetric]

  override def genHashAggregateTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new MockMetricsUpdater()

  override def genExpandTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map.empty[String, SQLMetric]

  override def genExpandTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new MockMetricsUpdater()

  override def genCustomExpandMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map.empty[String, SQLMetric]

  override def genColumnarShuffleExchangeMetrics(
      sparkContext: SparkContext,
      isSort: Boolean): Map[String, SQLMetric] = {

    Map(
      "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
      "bytesSpilled" -> SQLMetrics.createSizeMetric(sparkContext, "shuffle bytes spilled"),
      "splitTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime_split"),
      "spillTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "shuffle spill time"),
      "compressTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime_compress"),
      "avgReadBatchNumRows" -> SQLMetrics
        .createAverageMetric(sparkContext, "avg read batch num rows"),
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "numMergedVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of merged vecBatches"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions")
    )
  }

  override def genWindowTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map.empty[String, SQLMetric]

  override def genWindowTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new MockMetricsUpdater()

  override def genColumnarToRowMetrics(sparkContext: SparkContext): Map[String, SQLMetric] = {
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of input batches"),
      "omniColumnarToRowTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omniColumnar to row")
    )
  }

  override def genRowToColumnarMetrics(sparkContext: SparkContext): Map[String, SQLMetric] = {
    Map(
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
      "rowToOmniColumnarTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in row to OmniColumnar")
    )
  }


  override def genLimitTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map.empty[String, SQLMetric]

  override def genLimitTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new MockMetricsUpdater()

  override def genSortTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] = {
    Map(
      "addInputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni addInput"),
      "numInputVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of input vecBatches"),
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "omniCodegenTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni codegen"),
      "getOutputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni getOutput"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputDataSize" -> SQLMetrics.createSizeMetric(sparkContext, "output data size"),
      "numOutputVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of output vecBatches"),
      "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"))
  }

  override def genSortTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new OmniSortMetricsUpdater(metrics)

  override def genSortMergeJoinTransformerMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "streamedAddInputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni streamed addInput"),
    "streamedCodegenTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni streamed codegen"),
    "bufferedAddInputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni buffered addInput"),
    "bufferedCodegenTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni buffered codegen"),
    "getOutputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni buffered getOutput"),
    "numOutputVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of output vecBatches"),
    "numMergedVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of merged vecBatches"),
    "numStreamVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of streamed vecBatches"),
    "numBufferVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of buffered vecBatches")
  )

  override def genSortMergeJoinTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new SortMergeJoinMetricsUpdater(metrics)

  override def genColumnarBroadcastExchangeMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "collectTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to collect"),
      "broadcastTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to broadcast")
    )

  override def genColumnarSubqueryBroadcastMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] = Map(
    "dataSize" -> SQLMetrics.createMetric(sparkContext, "data size (bytes)"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "collectTime" -> SQLMetrics.createMetric(sparkContext, "time to collect (ms)")
  )

  override def genHashJoinTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "hashBuildInputRows" -> SQLMetrics.createMetric(sparkContext, "number of hash build input rows"),
      "hashBuildOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of hash build output rows"),
      "hashBuildOutputVectors" -> SQLMetrics.createMetric(sparkContext, "number of hash build output vectors"),
      "hashBuildOutputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of hash build output bytes"),
      "hashBuildCpuCount" -> SQLMetrics.createMetric(sparkContext, "hash build cpu wall time cout"),
      "hashBuildWallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of hash build"),
      "hashBuildPeakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "hash build peak memory bytes"),
      "hashBuildNumMemoryAllocations" -> SQLMetrics.createMetric(sparkContext, "number of hash build memory allocations"),
      "hashBuildSpilledBytes" -> SQLMetrics.createSizeMetric(sparkContext, "bytes written for spilling of hash build"),
      "hashBuildSpilledRows" -> SQLMetrics.createMetric(sparkContext, "total rows written for spilling of hash build"),
      "hashBuildSpilledPartitions" -> SQLMetrics.createMetric(sparkContext, "total spilled partitions  of hash build"),
      "hashBuildSpilledFiles" -> SQLMetrics.createMetric(sparkContext, "total spilled files  of hash build"),
      "hashProbeInputRows" -> SQLMetrics.createMetric(sparkContext, "number of hash probe input rows"),
      "hashProbeOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of hash probe output rows"),
      "hashProbeOutputVectors" -> SQLMetrics.createMetric(sparkContext, "number of hash probe output vectors"),
      "hashProbeOutputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of hash probe output bytes"),
      "hashProbeCpuCount" -> SQLMetrics.createMetric(sparkContext, "hash probe cpu wall time cout"),
      "hashProbeWallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of hash probe"),
      "hashProbePeakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "hash probe peak memory bytes"),
      "hashProbeNumMemoryAllocations" -> SQLMetrics.createMetric(sparkContext, "number of hash probe memory allocations"),
      "hashProbeSpilledBytes" -> SQLMetrics.createSizeMetric(sparkContext, "bytes written for spilling of hash probe"),
      "hashProbeSpilledRows" -> SQLMetrics.createMetric(sparkContext, "total rows written for spilling of hash probe"),
      "hashProbeSpilledPartitions" -> SQLMetrics.createMetric(sparkContext, "total spilled partitions of hash probe"),
      "hashProbeSpilledFiles" -> SQLMetrics.createMetric(sparkContext, "total spilled files of hash probe"),
      "hashProbeReplacedWithDynamicFilterRows" -> SQLMetrics.createMetric(sparkContext, "number of hash probe replaced with dynamic filter rows"),
      "hashProbeDynamicFiltersProduced" -> SQLMetrics.createMetric(sparkContext, "number of hash probe dynamic filters produced"),
      "streamPreProjectionCpuCount" -> SQLMetrics.createMetric(sparkContext, "stream preProject cpu wall time count"),
      "streamPreProjectionWallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of stream preProjection"),
      "buildPreProjectionCpuCount" -> SQLMetrics.createMetric(sparkContext, "preProject cpu wall time count"),
      "buildPreProjectionWallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of build preProjection"),
      "postProjectionCpuCount" -> SQLMetrics.createMetric(sparkContext, "postProject cpu wall time count"),
      "postProjectionWallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of postProjection"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "numOutputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "numOutputBytes" -> SQLMetrics.createMetric(sparkContext, "number of output bytes")
    )

  override def genHashJoinTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new HashJoinMetricsUpdater(metrics)

  override def genNestedLoopJoinTransformerMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] = 
      Map(
        "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
        "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
        "outputBytes" -> SQLMetrics.createMetric(sparkContext, "number of output bytes"),
        "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of NestedLoopJoin"),
        "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
        "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
        "numMemoryAllocations" -> SQLMetrics.createMetric(sparkContext, "number of memory allocations")
      )

  override def genNestedLoopJoinTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new NestedLoopJoinMetricsUpdater(metrics)

  override def genSampleTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map.empty

  override def genSampleTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new MockMetricsUpdater()

  override def genUnionTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map.empty

  override def genUnionTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new MockMetricsUpdater()

  override def genInputIteratorTransformerMetrics(
      child: SparkPlan,
      sparkContext: SparkContext,
      forBroadcast: Boolean): Map[String, SQLMetric] = Map.empty

  override def genInputIteratorTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric],
      forBroadcast: Boolean): MetricsUpdater = new MockMetricsUpdater()

  override def genWriteFilesTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map.empty

  override def genWriteFilesTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new MockMetricsUpdater()
}
