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
package org.apache.spark.shuffle

import com.huawei.boostkit.spark.jni.SparkJniWrapper
import com.huawei.boostkit.spark.vectorized.SplitResult
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.utils.OmniAdaptorUtil

import org.apache.spark._
import org.apache.spark.internal.config.SHUFFLE_COMPRESS
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.celeborn.CelebornShuffleHandle
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf

import java.io.IOException

import nova.hetu.omniruntime.vector.VecBatch

class OmniCelebornColumnarShuffleWriter[K, V](
    shuffleId: Int,
    handle: CelebornShuffleHandle[K, V, V],
    context: TaskContext,
    celebornConf: CelebornConf,
    client: ShuffleClient,
    writeMetrics: ShuffleWriteMetricsReporter)
  extends CelebornColumnarShuffleWriter[K, V](
    shuffleId,
    handle,
    context,
    celebornConf,
    client,
    writeMetrics) {

  private val columnarConf = GlutenConfig.get

  private val shuffleSpillBatchRowNum = columnarConf.omniColumnarShuffleSpillBatchRowNum
  private val shuffleTaskSpillMemoryThreshold = columnarConf.omniColumnarShuffleTaskSpillMemoryThreshold
  private val shuffleExecutorSpillMemoryThreshold = columnarConf.omniColumnarSpillMemPctThreshold *
    conf.getSizeAsBytes("spark.memory.offHeap.size", "1g")
  private val shuffleCompressBlockSize = columnarConf.omniColumnarShuffleCompressBlockSize

  private val handleRow = columnarConf.enableOmniRowShuffle &&
    dep.nativePartitioning.getSchema.length > columnarConf.omniRowShuffleColumnsThreshold

  private val shuffleNativeBufferSize = {
    val bufferSize = GlutenConfig.get.shuffleWriterBufferSize
    val maxBatchSize = GlutenConfig.get.maxBatchSize
    if (bufferSize > maxBatchSize) {
      logInfo(
        s"${GlutenConfig.SHUFFLE_WRITER_BUFFER_SIZE.key} ($bufferSize) exceeds max " +
          s" batch size. Limited to ${GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key} ($maxBatchSize).")
      maxBatchSize
    } else {
      bufferSize
    }
  }

  private val shuffleCompressionCodec =
    if (conf.getBoolean(SHUFFLE_COMPRESS.key, SHUFFLE_COMPRESS.defaultValue.get)) {
      GlutenShuffleUtils.getCompressionCodec(conf)
    } else {
      "uncompressed"
    }

  private val jniWrapper = new SparkJniWrapper()

  private var nativeSplitter: Long = -1L

  private var splitResult: SplitResult = _

  @throws[IOException]
  override def internalWrite(records: Iterator[Product2[K, V]]): Unit = {
    if (!records.hasNext) {
      handleEmptyIterator()
      return
    }

    while (records.hasNext) {
      val cb = records.next()._2.asInstanceOf[ColumnarBatch]
      if (cb.numRows == 0 || cb.numCols == 0) {
        logInfo(s"Skip ColumnarBatch of ${cb.numRows} rows, ${cb.numCols} cols")
      } else {
        initShuffleWriter(cb)
        val startTime = System.nanoTime()
        val input = OmniAdaptorUtil.transColBatchToOmniVecs(cb)
        for (col <- 0 until cb.numCols()) {
          dep.metrics("dataSize").add(input(col).getRealValueBufCapacityInBytes)
          dep.metrics("dataSize").add(input(col).getRealNullBufCapacityInBytes)
          dep.metrics("dataSize").add(input(col).getRealOffsetBufCapacityInBytes)
        }
        val vb = new VecBatch(input, cb.numRows())
        if (!handleRow) {
          jniWrapper.split(nativeSplitter, vb.getNativeVectorBatch)
        } else {
          jniWrapper.rowSplit(nativeSplitter, vb.getNativeVectorBatch)
        }
        dep.metrics("splitTime").add(System.nanoTime() - startTime)
        dep.metrics("numInputRows").add(cb.numRows)
        writeMetrics.incRecordsWritten(cb.numRows())
      }
    }

    if (nativeSplitter == -1L) {
      handleEmptyIterator()
      return
    }

    val startTime = System.nanoTime()
    splitResult =
      if (!handleRow) {
        jniWrapper.stop(nativeSplitter)
      } else {
        jniWrapper.rowStop(nativeSplitter)
      }
    dep.metrics("splitTime").add(
      System.nanoTime() - startTime - splitResult.getTotalSpillTime - splitResult.getTotalWriteTime -
        splitResult.getTotalComputePidTime)
    dep.metrics("spillTime").add(splitResult.getTotalSpillTime)
    dep.metrics("bytesSpilled").add(splitResult.getTotalBytesSpilled)
    dep.metrics("dataSize").add(splitResult.getPartitionLengths.sum)
    writeMetrics.incBytesWritten(splitResult.getTotalBytesWritten)
    writeMetrics.incWriteTime(splitResult.getTotalWriteTime + splitResult.getTotalSpillTime)

    partitionLengths = splitResult.getPartitionLengths
    pushMergedDataToCeleborn()
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
  }

  override def createShuffleWriter(columnarBatch: ColumnarBatch): Unit = {
    nativeSplitter = jniWrapper.makeForRSS(
      dep.nativePartitioning,
      shuffleNativeBufferSize,
      shuffleCompressionCodec,
      shuffleCompressBlockSize,
      shuffleSpillBatchRowNum,
      shuffleTaskSpillMemoryThreshold,
      shuffleExecutorSpillMemoryThreshold,
      celebornPartitionPusher)
    nativeShuffleWriter = nativeSplitter
  }

  override def closeShuffleWriter(): Unit = {
    if (nativeSplitter != -1L) {
      jniWrapper.close(nativeSplitter)
      nativeSplitter = -1L
    }
  }
}
