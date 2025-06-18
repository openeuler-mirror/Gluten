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
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.{ColumnarInputAdapter, SparkPlan}
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
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "rawInputRows" -> SQLMetrics.createMetric(sparkContext, "number of raw input rows"),
      "rawInputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of raw input bytes"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "scanTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of scan"),
      "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of scan and filter"),
      "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
      "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
      "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of files read"),
      "metadataTime" -> SQLMetrics.createTimingMetric(sparkContext, "metadata time"),
      "filesSize" -> SQLMetrics.createSizeMetric(sparkContext, "size of files read"),
      "numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions read"),
      "pruningTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "dynamic partition pruning time"),
      "numMemoryAllocations" -> SQLMetrics.createMetric(
        sparkContext,
        "number of memory allocations"),
      "numDynamicFiltersAccepted" -> SQLMetrics.createMetric(
        sparkContext,
        "number of dynamic filters accepted"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "skippedSplits" -> SQLMetrics.createMetric(sparkContext, "number of skipped splits"),
      "processedSplits" -> SQLMetrics.createMetric(sparkContext, "number of processed splits"),
      "preloadSplits" -> SQLMetrics.createMetric(sparkContext, "number of preloaded splits"),
      "skippedStrides" -> SQLMetrics.createMetric(sparkContext, "number of skipped row groups"),
      "processedStrides" -> SQLMetrics.createMetric(sparkContext, "number of processed row groups"),
      "remainingFilterTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "remaining filter time"),
      "ioWaitTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "io wait time"),
      "storageReadBytes" -> SQLMetrics.createSizeMetric(sparkContext, "storage read bytes"),
      "localReadBytes" -> SQLMetrics.createSizeMetric(sparkContext, "local ssd read bytes"),
      "ramReadBytes" -> SQLMetrics.createSizeMetric(sparkContext, "ram read bytes")
    )


  override def genFileSourceScanTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new MockMetricsUpdater()

  override def genFilterTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes")
    )

  override def genFilterTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric],
      extraMetrics: Seq[(String, SQLMetric)] = Seq.empty): MetricsUpdater =
    new OmniFilterMetricsUpdater(metrics, extraMetrics)

  override def genProjectTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "numOutputVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes")
    )

  override def genProjectTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric],
      extraMetrics: Seq[(String, SQLMetric)] = Seq.empty): MetricsUpdater = new OmniProjectMetricsUpdater(metrics, extraMetrics)

  override def genHashAggregateTransformerMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] = Map(
    "aggOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "aggOutputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
    "aggOutputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
    "aggCpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu time count"),
    "aggCpuNanos" -> SQLMetrics.createTimingMetric(sparkContext, "time of aggregation"),
    "rowConstructionCpuCount" -> SQLMetrics.createMetric(
      sparkContext,
      "rowConstruction cpu time count"),
    "rowConstructionCpuNanos" -> SQLMetrics.createTimingMetric(
      sparkContext,
      "time of rowConstruction"),
    "extractionCpuCount" -> SQLMetrics.createMetric(
      sparkContext,
      "extraction cpu time count"),
    "extractionCpuNanos" -> SQLMetrics.createTimingMetric(
      sparkContext,
      "time of extraction")
  )

  override def genHashAggregateTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new OmniHashAggregateMetricsUpdaterImpl(metrics)

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
      "splitTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_split"),
      "spillTime" -> SQLMetrics.createTimingMetric(sparkContext, "shuffle spill time"),
      "compressTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_compress"),
      "avgReadBatchNumRows" -> SQLMetrics
        .createAverageMetric(sparkContext, "avg read batch num rows"),
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "numMergedVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of merged vecBatches"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions")
    )
  }

  override def genWindowTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "inputVectors" -> SQLMetrics.createMetric(sparkContext, "number of input vectors"),
      "inputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of input bytes"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "cpuNanos" -> SQLMetrics.createTimingMetric(sparkContext, "time of window"),
      "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count")
    )

  override def genWindowTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new OmniWindowMetricsUpdater(metrics)

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
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "cpuNanos" -> SQLMetrics.createTimingMetric(sparkContext, "time of limit"),
      "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count")
    )

  override def genLimitTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new OmniLimitMetricsUpdater(metrics)

  override def genSortTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] = {
    Map(
      "addInputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni addInput"),
      "numInputVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of input vecBatches"),
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
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
    "numOutputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
    "numOutputBytes" -> SQLMetrics.createMetric(sparkContext, "number of output bytes"),
    "streamedAddInputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni streamed addInput"),
    "streamedCodegenTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni streamed codegen"),
    "bufferedAddInputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni buffered addInput"),
    "bufferedCodegenTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni buffered codegen"),
    "getOutputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni buffered getOutput"),
    "numOutputVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of output vecBatches"),
    "numMergedVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of merged vecBatches"),
    "numStreamVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of streamed vecBatches"),
    "numBufferVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of buffered vecBatches"),
    "streamPreProjectionCpuCount" -> SQLMetrics.createMetric(sparkContext, "number of streamed cpu count"),
    "streamPreProjectionWallNanos" -> SQLMetrics.createMetric(sparkContext, "time in streamed pre projection"),
    "bufferPreProjectionCpuCount" -> SQLMetrics.createMetric(sparkContext, "number of buffer pre projection cpu count"),
    "bufferPreProjectionWallNanos" -> SQLMetrics.createMetric(sparkContext, "time in buffer pre projection")
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
      "hashBuildNumInputVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of hash build input vector batches"),
      "hashBuildAddInputTime" -> SQLMetrics.createSizeMetric(sparkContext, "time of hash build input"),
      "hashBuildGetOutputTime" -> SQLMetrics.createMetric(sparkContext, "time of hash build output"),

      "hashProbeInputRows" -> SQLMetrics.createMetric(sparkContext, "number of hash probe input rows"),
      "hashProbeOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of hash probe output rows"),
      "hashProbeNumInputVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of hash probe input vector batches"),
      "hashProbeNumOutputVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of hash probe output vector batches"),
      "hashProbeAddInputTime" -> SQLMetrics.createSizeMetric(sparkContext, "time of hash probe input"),
      "hashProbeGetOutputTime" -> SQLMetrics.createMetric(sparkContext, "time of hash probe output"),

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
        "cpuNanos" -> SQLMetrics.createTimingMetric(sparkContext, "time of NestedLoopJoin"),
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
      forBroadcast: Boolean): Map[String, SQLMetric] = {
    def metricsPlan(plan: SparkPlan): SparkPlan = {
      plan match {
        case ColumnarInputAdapter(child) => metricsPlan(child)
        case q: QueryStageExec => metricsPlan(q.plan)
        case _ => plan
      }
    }

    val outputMetrics = if (forBroadcast) {
      metricsPlan(child).metrics
        .filterKeys(key => key.equals("numOutputRows") || key.equals("outputVectors"))
    } else {
      Map(
        "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
        "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors")
      )
    }

    Map(
      "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
      "cpuNanos" -> SQLMetrics.createTimingMetric(sparkContext, "time of input iterator")
    ) ++ outputMetrics
  }

  override def genInputIteratorTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric],
      forBroadcast: Boolean): MetricsUpdater = new OmniInputIteratorMetricsUpdater(metrics, forBroadcast)

  override def genWriteFilesTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map.empty

  override def genWriteFilesTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new MockMetricsUpdater()
}
