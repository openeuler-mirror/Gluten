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

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

import nova.hetu.omniruntime.vector.VecBatch

import java.io.IOException

class OmniColumnarShuffleWriter[K, V](
    shuffleBlockResolver: IndexShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, V],
    mapId: Long,
    writeMetrics: ShuffleWriteMetricsReporter)
  extends ShuffleWriter[K, V]
  with Logging {

  private val dep = handle.dependency.asInstanceOf[ColumnarShuffleDependency[K, V, V]]

  private val blockManager = SparkEnv.get.blockManager

  private var stopping = false

  private var mapStatus: MapStatus = _

  private val localDirs = blockManager.diskBlockManager.localDirs.mkString(",")

  private val conf = SparkEnv.get.conf

  val columnarConf = GlutenConfig.get
  val shuffleSpillBatchRowNum = columnarConf.omniColumnarShuffleSpillBatchRowNum
  val shuffleTaskSpillMemoryThreshold = columnarConf.omniColumnarShuffleTaskSpillMemoryThreshold
  val shuffleExecutorSpillMemoryThreshold = columnarConf.omniColumnarSpillMemPctThreshold *
    conf.getSizeAsBytes("spark.memory.offHeap.size", "1g")
  val shuffleCompressBlockSize = columnarConf.omniColumnarShuffleCompressBlockSize

  val shuffleNativeBufferSize = {
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

  val shuffleCompressionCodec =
    if (conf.getBoolean("spark.shuffle.compress", true)) {
      GlutenShuffleUtils.getCompressionCodec(conf)
    } else {
      "uncompressed" // uncompressed
    }

  private val jniWrapper = new SparkJniWrapper()

  private var nativeSplitter: Long = 0

  // TODO: use GlutenSplitResult
  private var splitResult: SplitResult = _

  private var partitionLengths: Array[Long] = _

  private val handleRow = dep.serializer.asInstanceOf[OmniColumnarBatchSerializer].isRowShuffle()

  @throws[IOException]
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    if (!records.hasNext) {
      partitionLengths = new Array[Long](dep.partitioner.numPartitions)
      shuffleBlockResolver.writeMetadataFileAndCommit(
        dep.shuffleId,
        mapId,
        partitionLengths,
        Array[Long](),
        null)
      mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
      return
    }

    val dataTmp = Utils.tempFileWith(shuffleBlockResolver.getDataFile(dep.shuffleId, mapId))
    if (nativeSplitter == 0) {
      nativeSplitter = jniWrapper.make(
        dep.nativePartitioning,
        shuffleNativeBufferSize,
        shuffleCompressionCodec,
        dataTmp.getAbsolutePath,
        blockManager.subDirsPerLocalDir,
        localDirs,
        shuffleCompressBlockSize,
        shuffleSpillBatchRowNum,
        shuffleTaskSpillMemoryThreshold,
        shuffleExecutorSpillMemoryThreshold
      )
    }

    while (records.hasNext) {
      val cb = records.next()._2.asInstanceOf[ColumnarBatch]
      if (cb.numRows == 0 || cb.numCols == 0) {
        logInfo(s"Skip ColumnarBatch of ${cb.numRows} rows, ${cb.numCols} cols")
      } else {
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
        writeMetrics.incRecordsWritten(cb.numRows)
      }
    }
    val startTime = System.nanoTime()
    if (!handleRow) {
      splitResult = jniWrapper.stop(nativeSplitter)
    } else {
      splitResult = jniWrapper.rowStop(nativeSplitter)
    }

    dep
      .metrics("splitTime")
      .add(
        System.nanoTime() - startTime - splitResult.getTotalSpillTime -
          splitResult.getTotalWriteTime - splitResult.getTotalComputePidTime)
    dep.metrics("spillTime").add(splitResult.getTotalSpillTime)
    dep.metrics("bytesSpilled").add(splitResult.getTotalBytesSpilled)
    writeMetrics.incBytesWritten(splitResult.getTotalBytesWritten)
    writeMetrics.incWriteTime(splitResult.getTotalWriteTime + splitResult.getTotalSpillTime)

    partitionLengths = splitResult.getPartitionLengths
    try {
      shuffleBlockResolver.writeMetadataFileAndCommit(
        dep.shuffleId,
        mapId,
        partitionLengths,
        Array[Long](),
        dataTmp)
    } finally {
      if (dataTmp.exists() && !dataTmp.delete()) {
        logError(s"Error while deleting temp file ${dataTmp.getAbsolutePath}")
      }
    }
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        None
      } else {
        stopping = true
        if (success) {
          Option(mapStatus)
        } else {
          None
        }
      }
    } finally {
      if (nativeSplitter != 0) {
        jniWrapper.close(nativeSplitter)
        nativeSplitter = 0
      }
    }
  }

  // VisibleForTesting
  def getPartitionLengths: Array[Long] = partitionLengths

}
