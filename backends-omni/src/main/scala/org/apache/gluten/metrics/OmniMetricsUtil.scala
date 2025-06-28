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

import org.apache.gluten.execution._
import org.apache.gluten.substrait.{AggregationParams, JoinParams}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlan

import java.lang.{Long => JLong}
import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}

object OmniMetricsUtil extends Logging {

  /**
   * Generate the function which updates metrics fetched from certain iterator to transformers.
   *
   * @param child
   *   the child spark plan
   * @param relMap
   *   the map between operator index and its rels
   * @param joinParamsMap
   *   the map between operator index and join parameters
   * @param aggParamsMap
   *   the map between operator index and aggregation parameters
   */
  def genMetricsUpdatingFunction(
      child: SparkPlan,
      relMap: JMap[JLong, JList[JLong]],
      joinParamsMap: JMap[JLong, JoinParams],
      aggParamsMap: JMap[JLong, AggregationParams]): IMetrics => Unit = {
    def treeifyMetricsUpdaters(plan: SparkPlan): MetricsUpdaterTree = {
      plan match {
        case j: HashJoinLikeExecTransformer =>
          MetricsUpdaterTree(
            j.metricsUpdater(),
            List(treeifyMetricsUpdaters(j.buildPlan), treeifyMetricsUpdaters(j.streamedPlan)))
        case smj: SortMergeJoinExecTransformer =>
          MetricsUpdaterTree(
            smj.metricsUpdater(),
            List(treeifyMetricsUpdaters(smj.bufferedPlan), treeifyMetricsUpdaters(smj.streamedPlan)))
        case t: TransformSupport if t.metricsUpdater() == MetricsUpdater.None =>
          assert(t.children.size == 1, "MetricsUpdater.None can only be used on unary operator")
          treeifyMetricsUpdaters(t.children.head)
        case t: TransformSupport =>
          // Reversed children order to match the traversal code.
          MetricsUpdaterTree(t.metricsUpdater(), t.children.reverse.map(treeifyMetricsUpdaters).toList)
        case _ =>
          MetricsUpdaterTree(MetricsUpdater.Terminate, List())
      }
    }
    val mut: MetricsUpdaterTree = treeifyMetricsUpdaters(child)
    genMetricsUpdatingFunction(
      mut,
      relMap,
      JLong.valueOf(relMap.size() - 1),
      joinParamsMap,
      aggParamsMap)
  }

  /**
   * Merge several suites of metrics together.
   *
   * @param operatorMetrics
   *   : a list of metrics to merge
   * @return
   *   the merged metrics
   */
  private def mergeMetrics(operatorMetrics: JList[OperatorMetrics]): OperatorMetrics = {
    if (operatorMetrics.size() == 0) {
      return null
    }

    // We are accessing the metrics from end to start. So the input metrics are got from the
    // last suite of metrics, and the output metrics are got from the first suite.
    val inputRows = operatorMetrics.get(operatorMetrics.size() - 1).getNumInputRows
    val inputVectors = operatorMetrics.get(operatorMetrics.size() - 1).getNumInputVecBatches
    val inputBytes = operatorMetrics.get(operatorMetrics.size() - 1).getNumInputBytes
    val inputCpuCount = operatorMetrics.get(operatorMetrics.size() - 1).getInputCpuCount
    val rawInputRows = operatorMetrics.get(operatorMetrics.size() - 1).getRawInputRows
    val rawInputBytes = operatorMetrics.get(operatorMetrics.size() - 1).getRawInputBytes
    val addInputTime = operatorMetrics.get(operatorMetrics.size() - 1).getAddInputTime


    val outputRows = operatorMetrics.get(0).getNumOutputRows
    val outputVectors = operatorMetrics.get(0).getNumOutputVecBatches
    val outputBytes = operatorMetrics.get(0).getNumOutputBytes
    val outputCpuCount = operatorMetrics.get(0).getCpuCount
    val getOutputTime = operatorMetrics.get(0).getGetOutputTime;

    val physicalWrittenBytes = operatorMetrics.get(0).getPhysicalWrittenBytes
    val writeIOTime = operatorMetrics.get(0).getWriteIOTime

    var cpuCount: Long = 0
    var cpuNanos: Long = 0
    var peakMemoryBytes: Long = 0
    var numMemoryAllocations: Long = 0
    var spilledInputBytes: Long = 0
    var spilledBytes: Long = 0
    var spilledRows: Long = 0
    var spilledPartitions: Long = 0
    var spilledFiles: Long = 0
    var numDynamicFiltersProduced: Long = 0
    var numDynamicFiltersAccepted: Long = 0
    var numReplacedWithDynamicFilterRows: Long = 0
    var flushRowCount: Long = 0
    var loadedToValueHook: Long = 0
    var scanTime: Long = 0
    var skippedSplits: Long = 0
    var processedSplits: Long = 0
    var skippedStrides: Long = 0
    var processedStrides: Long = 0
    var remainingFilterTime: Long = 0
    var ioWaitTime: Long = 0
    var storageReadBytes: Long = 0
    var localReadBytes: Long = 0
    var ramReadBytes: Long = 0
    var preloadSplits: Long = 0
    var numWrittenFiles: Long = 0

    val buildInputRows = operatorMetrics.get(operatorMetrics.size() - 1).getBuildInputRows
    val buildNumInputVecBatches = operatorMetrics.get(operatorMetrics.size() - 1).getBuildNumInputVecBatches
    val buildAddInputTime = operatorMetrics.get(operatorMetrics.size() - 1).getBuildAddInputTime
    val buildGetOutputTime = operatorMetrics.get(operatorMetrics.size() - 1).getBuildGetOutputTime
    val lookupInputRows = operatorMetrics.get(0).getLookupInputRows
    val lookupNumInputVecBatches = operatorMetrics.get(0).getLookupNumInputVecBatches
    val lookupOutputRows = operatorMetrics.get(0).getLookupOutputRows
    val lookupNumOutputVecBatches = operatorMetrics.get(0).getLookupNumOutputVecBatches
    val lookupAddInputTime = operatorMetrics.get(0).getLookupAddInputTime
    val lookupGetOutputTime = operatorMetrics.get(0).getLookupGetOutputTime
    val metricsIterator = operatorMetrics.iterator()
    while (metricsIterator.hasNext) {
      val metrics = metricsIterator.next()
      cpuCount += metrics.getCpuCount
      cpuNanos += metrics.getCpuNanos
      peakMemoryBytes = peakMemoryBytes.max(metrics.getPeakMemoryBytes)
      numMemoryAllocations += metrics.getNumMemoryAllocations
      spilledInputBytes += metrics.getSpilledInputBytes
      spilledBytes += metrics.getSpilledBytes
      spilledRows += metrics.getSpilledRows
      spilledPartitions += metrics.getSpilledPartitions
      spilledFiles += metrics.getSpilledFiles
      numDynamicFiltersProduced += metrics.getNumDynamicFiltersProduced
      numDynamicFiltersAccepted += metrics.getNumDynamicFiltersAccepted
      numReplacedWithDynamicFilterRows += metrics.getNumReplacedWithDynamicFilterRows
      flushRowCount += metrics.getFlushRowCount
      loadedToValueHook += metrics.getLoadedToValueHook
      scanTime += metrics.getScanTime
      skippedSplits += metrics.getSkippedSplits
      processedSplits += metrics.getProcessedSplits
      skippedStrides += metrics.getSkippedStrides
      processedStrides += metrics.getProcessedStrides
      remainingFilterTime += metrics.getRemainingFilterTime
      ioWaitTime += metrics.getIoWaitTime
      storageReadBytes += metrics.getStorageReadBytes
      localReadBytes += metrics.getLocalReadBytes
      ramReadBytes += metrics.getRamReadBytes
      preloadSplits += metrics.getPreloadSplits
      numWrittenFiles += metrics.getNumWrittenFiles
  }

    new OperatorMetrics(
      inputRows,
      inputVectors,
      inputBytes,
      rawInputRows,
      rawInputBytes,
      outputRows,
      outputVectors,
      outputBytes,
      cpuCount,
      cpuNanos,
      inputCpuCount,
      outputCpuCount,
      peakMemoryBytes,
      numMemoryAllocations,
      spilledInputBytes,
      spilledBytes,
      spilledRows,
      spilledPartitions,
      spilledFiles,
      numDynamicFiltersProduced,
      numDynamicFiltersAccepted,
      numReplacedWithDynamicFilterRows,
      flushRowCount,
      loadedToValueHook,
      scanTime,
      skippedSplits,
      processedSplits,
      skippedStrides,
      processedStrides,
      remainingFilterTime,
      ioWaitTime,
      storageReadBytes,
      localReadBytes,
      ramReadBytes,
      preloadSplits,
      physicalWrittenBytes,
      writeIOTime,
      numWrittenFiles,

      addInputTime,
      getOutputTime,

      buildInputRows,
      buildNumInputVecBatches,
      buildAddInputTime,
      buildGetOutputTime,

      lookupInputRows,
      lookupNumInputVecBatches,
      lookupOutputRows,
      lookupNumOutputVecBatches,
      lookupAddInputTime,
      lookupGetOutputTime
    )
  }

  // FIXME: Metrics updating code is too magical to maintain. Tree-walking algorithm should be made
  //  more declarative than by counting down these counters that don't have fixed definition.
  /**
   * @return
   *   operator index and metrics index
   */
  def updateTransformerMetricsInternal(
      mutNode: MetricsUpdaterTree,
      relMap: JMap[JLong, JList[JLong]],
      operatorIdx: JLong,
      metrics: Metrics,
      metricsIdx: Int,
      joinParamsMap: JMap[JLong, JoinParams],
      aggParamsMap: JMap[JLong, AggregationParams]): (JLong, Int) = {
    logDebug(s"mutNode.update is : ${mutNode.updater.toString} " +
      s"and muNode child is ${mutNode.children.toString()}")
    if (mutNode.updater == MetricsUpdater.Terminate) {
      return (operatorIdx, metricsIdx)
    }
    val operatorMetrics = new JArrayList[OperatorMetrics]()
    var curMetricsIdx = metricsIdx
    relMap
      .get(operatorIdx)
      .forEach(
        _ => {
          operatorMetrics.add(metrics.genOperatorMetrics(curMetricsIdx))
          curMetricsIdx -= 1
        })
    mutNode.updater match {
      case ju: HashJoinMetricsUpdater =>
        // JoinRel outputs two suites of metrics respectively for hash build and hash probe.
        // Therefore, fetch one more suite of metrics here.
        operatorMetrics.add(metrics.genOperatorMetrics(curMetricsIdx))
        curMetricsIdx -= 1
        ju.updateJoinMetrics(
          operatorMetrics,
          metrics.getSingleMetrics,
          joinParamsMap.get(operatorIdx))
      case smj: SortMergeJoinMetricsUpdater =>
        smj.updateJoinMetrics(
          operatorMetrics,
          metrics.getSingleMetrics,
          joinParamsMap.get(operatorIdx))
      case u: OmniUnionMetricsUpdater =>
        // JoinRel outputs two suites of metrics respectively for hash build and hash probe.
        // Therefore, fetch one more suite of metrics here.
        operatorMetrics.add(metrics.genOperatorMetrics(curMetricsIdx))
        curMetricsIdx -= 1
        u.updateUnionMetrics(operatorMetrics)
      case hau: OmniHashAggregateMetricsUpdater =>
        hau.updateAggregationMetrics(operatorMetrics, aggParamsMap.get(operatorIdx))
      case lu: OmniLimitMetricsUpdater =>
        // Limit over Sort is converted to TopN node in omni, so there is only one suite of metrics
        // for the two transformers. We do not update metrics for limit and leave it for sort.
        if (!mutNode.children.head.updater.isInstanceOf[OmniSortMetricsUpdater]) {
          val opMetrics: OperatorMetrics = mergeMetrics(operatorMetrics)
          lu.updateNativeMetrics(opMetrics)
        }
      case u =>
        val opMetrics: OperatorMetrics = mergeMetrics(operatorMetrics)
        u.updateNativeMetrics(opMetrics)
    }

    var newOperatorIdx: JLong = operatorIdx - 1
    var newMetricsIdx: Int =
      if (
        mutNode.updater.isInstanceOf[OmniLimitMetricsUpdater] &&
        mutNode.children.head.updater.isInstanceOf[OmniSortMetricsUpdater]
      ) {
        // This suite of metrics is not consumed.
        metricsIdx
      } else {
        curMetricsIdx
      }

    mutNode.children.foreach {
      child =>
        val result = updateTransformerMetricsInternal(
          child,
          relMap,
          newOperatorIdx,
          metrics,
          newMetricsIdx,
          joinParamsMap,
          aggParamsMap)
        newOperatorIdx = result._1
        newMetricsIdx = result._2
    }
    (newOperatorIdx, newMetricsIdx)
  }

  /**
   * Get a function which would update the metrics of transformers.
   *
   * @param mutNode
   *   the metrics updater tree built from the original plan
   * @param relMap
   *   the map between operator index and its rels
   * @param operatorIdx
   *   the index of operator
   * @param joinParamsMap
   *   the map between operator index and join parameters
   * @param aggParamsMap
   *   the map between operator index and aggregation parameters
   *
   * @return
   *   A recursive function updating the metrics of operator(transformer) and its children.
   */
  def genMetricsUpdatingFunction(
      mutNode: MetricsUpdaterTree,
      relMap: JMap[JLong, JList[JLong]],
      operatorIdx: JLong,
      joinParamsMap: JMap[JLong, JoinParams],
      aggParamsMap: JMap[JLong, AggregationParams]): IMetrics => Unit = {
    imetrics =>
      try {
        val metrics = imetrics.asInstanceOf[Metrics]
        val numNativeMetrics = metrics.getInputRows().length
        if (numNativeMetrics == 0) {
          logInfo("numNativeMetrics size is 0.")
        } else {
          updateTransformerMetricsInternal(
            mutNode,
            relMap,
            operatorIdx,
            metrics,
            numNativeMetrics - 1,
            joinParamsMap,
            aggParamsMap)
        }
      } catch {
        case e: Exception =>
          logWarning(s"Updating native metrics failed due to ${e.getCause}.")
          ()
      }
  }
}
