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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.runtime.OmniRuntimes

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.gluten.vectorized.{JniByteInputStream, JniByteInputStreams, ShuffleColumnarBatchOutIterator}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch
import java.util.concurrent.atomic.AtomicBoolean

import com.huawei.boostkit.spark.jni.SparkJniWrapper

import java.io._
import java.nio.ByteBuffer

import scala.reflect.ClassTag

class OmniColumnarBatchSerializer(
    readBatchNumRows: SQLMetric,
    numOutputRows: SQLMetric,
    isRowShuffle: Boolean = false)
  extends Serializer
  with Serializable {

  /** Creates a new [[SerializerInstance]]. */
  override def newInstance(): SerializerInstance =
    new ColumnarBatchSerializerInstance(isRowShuffle, readBatchNumRows, numOutputRows)

  def isRowShuffle(): Boolean = {
    isRowShuffle
  }

  override def supportsRelocationOfSerializedObjects: Boolean = true
}

private class ColumnarBatchSerializerInstance(
    isRowShuffle: Boolean,
    readBatchNumRows: SQLMetric,
    numOutputRows: SQLMetric)
  extends SerializerInstance
  with Logging {
  private val columnarConf = GlutenConfig.get
  private val conf = SparkEnv.get.conf

  private val shuffleCompressBlockSize = columnarConf.omniColumnarShuffleCompressBlockSize
  private val enableShuffleCompress = conf.getBoolean("spark.shuffle.compress", defaultValue = true)

  private val jniWrapper = new SparkJniWrapper()

  val shuffleCompressionCodec =
    if (enableShuffleCompress) {
      GlutenShuffleUtils.getCompressionCodec(conf)
    } else {
      "uncompressed" // uncompressed
    }

  override def deserializeStream(in: InputStream): DeserializationStream = {
    new DeserializationStream {

      private var numBatchesTotal: Long = _
      private var numRowsTotal: Long = _

      // Otherwise calling close() twice would cause resource ID not found error.
      private val closeCalled: AtomicBoolean = new AtomicBoolean(false)

      private val dIn: JniByteInputStream = JniByteInputStreams.create(in)

      // Resolve the per-task OmniRuntime
      // (TaskResources keyed on backend + "omni-shuffle-reader"; symmetric with
      // the writer-side "omni-shuffle-writer" in OmniShuffleUtil.scala) and
      // obtain its OmniTaskMemoryPool address via the gluten-side getPoolAddress
      // bridge. Passed as nmmHandle to makeNativeDeserializer so the native
      // ShuffleReaderDeserializer + DecompressionStream Vector ctors allocate
      // against the per-task pool; downstream operators such as the lookup join
      // output builder inherit that pool from the deserialized vectors. Returns 0
      // in test setups where no Spark task is active; the native side then falls
      // back to the unaccounted no-pool Vector ctor, as on the writer side.
      private val taskPoolAddress: Long =
        try {
          val runtime = OmniRuntimes.contextInstance(
            BackendsApiManager.getBackendName,
            "omni-shuffle-reader")
          runtime.getPoolAddress()
        } catch {
          case _: IllegalStateException => 0L  // not in a Spark task (test path)
        }

      private val shuffleReaderHandle =
        jniWrapper.makeNativeDeserializer(
          dIn, shuffleCompressionCodec, shuffleCompressBlockSize, isRowShuffle, taskPoolAddress)
      private var cb: ColumnarBatch = _
      private val wrappedOut: ShuffleColumnarBatchOutIterator = new ShuffleColumnarBatchOutIterator(shuffleReaderHandle)

      override def asIterator: Iterator[Any] = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def readKey[T: ClassTag](): T = {
        // We skipped serialization of the key in writeKey(), so just return a dummy value since
        // this is going to be discarded anyways.
        null.asInstanceOf[T]
      }

      @throws(classOf[EOFException])
      override def readValue[T: ClassTag](): T = {
        val batch = {
          val maybeBatch =
            try {
              wrappedOut.next()
            } catch {
              case ioe: IOException =>
                this.close()
                logError("Failed to load next RecordBatch", ioe)
                throw ioe
            }
          if (maybeBatch == null) {
            // EOF reached
            this.close()
            throw new EOFException
          }
          maybeBatch
        }
        val numRows = batch.numRows()
        logDebug(s"Read ColumnarBatch of $numRows rows")
        numBatchesTotal += 1
        numRowsTotal += numRows
        batch.asInstanceOf[T]
      }

      override def readObject[T: ClassTag](): T = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def close(): Unit = {
        if (numBatchesTotal > 0) {
          readBatchNumRows.set(numRowsTotal.toDouble / numBatchesTotal)
        }
        numOutputRows += numRowsTotal
        dIn.close()
        if (!closeCalled.compareAndSet(false, true)) {
          return
        }
        jniWrapper.closeDeserializer(shuffleReaderHandle)
      }
    }
  }

  override def serialize[T: ClassTag](t: T): ByteBuffer =
    throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException

  override def serializeStream(s: OutputStream): SerializationStream =
    throw new UnsupportedOperationException
}
