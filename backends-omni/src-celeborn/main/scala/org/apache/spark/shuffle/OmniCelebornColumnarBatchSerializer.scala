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

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.gluten.vectorized.{JniByteInputStream, JniByteInputStreams, ShuffleColumnarBatchOutIterator}
import org.apache.spark.sql.execution.metric.SQLMetric

import org.apache.celeborn.client.read.CelebornInputStream

import com.huawei.boostkit.spark.jni.SparkJniWrapper

import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean

import scala.reflect.ClassTag

class OmniCelebornColumnarBatchSerializer(
    readBatchNumRows: SQLMetric,
    numOutputRows: SQLMetric,
    isRowShuffle: Boolean = false)
  extends Serializer
  with Serializable {

  override def newInstance(): SerializerInstance =
    new OmniCelebornColumnarBatchSerializerInstance(readBatchNumRows, numOutputRows, isRowShuffle)

  override def supportsRelocationOfSerializedObjects: Boolean = true
}

private class OmniCelebornColumnarBatchSerializerInstance(
    readBatchNumRows: SQLMetric,
    numOutputRows: SQLMetric,
    isRowShuffle: Boolean)
  extends SerializerInstance
  with Logging {

  private val columnarConf = GlutenConfig.get
  private val conf = SparkEnv.get.conf
  private val shuffleCompressBlockSize = columnarConf.omniColumnarShuffleCompressBlockSize
  private val enableShuffleCompress = conf.getBoolean("spark.shuffle.compress", defaultValue = true)
  private val jniWrapper = new SparkJniWrapper()

  private val shuffleCompressionCodec =
    if (enableShuffleCompress) {
      GlutenShuffleUtils.getCompressionCodec(conf)
    } else {
      "uncompressed"
    }

  override def deserializeStream(in: InputStream): DeserializationStream = {
    new DeserializationStream {
      private var numBatchesTotal: Long = _
      private var numRowsTotal: Long = _
      private val closeCalled: AtomicBoolean = new AtomicBoolean(false)
      private val isEmptyStream: Boolean = in.equals(CelebornInputStream.empty())
      private val dIn: JniByteInputStream =
        if (isEmptyStream) null else JniByteInputStreams.create(in)
      private val shuffleReaderHandle =
        if (isEmptyStream) {
          -1L
        } else {
          jniWrapper.makeNativeDeserializer(
            dIn,
            shuffleCompressionCodec,
            shuffleCompressBlockSize,
            isRowShuffle)
        }
      private val wrappedOut: ShuffleColumnarBatchOutIterator =
        if (isEmptyStream) null else new ShuffleColumnarBatchOutIterator(shuffleReaderHandle)

      override def asIterator: Iterator[Any] =
        throw new UnsupportedOperationException

      override def asKeyValueIterator: Iterator[(Any, Any)] = new Iterator[(Any, Any)] {
        private var gotNext = false
        private var nextValue: (Any, Any) = _
        private var finished = false

        def getNext: (Any, Any) = {
          try {
            (readKey[Any](), readValue[Any]())
          } catch {
            case eof: EOFException =>
              finished = true
              null
          }
        }

        override def hasNext: Boolean = {
          if (!isEmptyStream && !finished) {
            if (!gotNext) {
              nextValue = getNext
              gotNext = true
            }
          }
          !isEmptyStream && !finished
        }

        override def next(): (Any, Any) = {
          if (!hasNext) {
            throw new NoSuchElementException("End of stream")
          }
          gotNext = false
          nextValue
        }
      }

      override def readKey[T: ClassTag](): T = null.asInstanceOf[T]

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
            this.close()
            throw new EOFException
          }
          maybeBatch
        }
        val numRows = batch.numRows()
        numBatchesTotal += 1
        numRowsTotal += numRows
        batch.asInstanceOf[T]
      }

      override def readObject[T: ClassTag](): T =
        throw new UnsupportedOperationException

      override def close(): Unit = {
        if (numBatchesTotal > 0) {
          readBatchNumRows.set(numRowsTotal.toDouble / numBatchesTotal)
        }
        numOutputRows += numRowsTotal
        if (dIn != null) {
          dIn.close()
        }
        if (!closeCalled.compareAndSet(false, true)) {
          return
        }
        if (shuffleReaderHandle != -1L) {
          jniWrapper.closeDeserializer(shuffleReaderHandle)
        }
      }
    }
  }

  override def serializeStream(out: OutputStream): SerializationStream =
    throw new UnsupportedOperationException

  override def serialize[T: ClassTag](t: T): ByteBuffer =
    throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException
}
