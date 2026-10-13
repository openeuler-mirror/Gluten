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

package org.apache.gluten.utils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import nova.hetu.omniruntime.`type`._
import nova.hetu.omniruntime.vector.{ByteVec, BooleanVec, Decimal128Vec, DoubleVec, FloatVec, IntVec, LongVec, ShortVec, VarcharVec, ArrayVec, Vec, VecBatch}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{NullType, ByteType, BooleanType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampType, ArrayType, BinaryType, Metadata}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.expression.OmniExpressionAdaptor.{sparkTypeToOmniType, sparkTypeToOmniTypeWithComplex}
import org.apache.gluten.utils.OmniAdaptorUtil.transColBatchToOmniVecs
import org.apache.gluten.vectorized.OmniColumnVector

class MergeIterator(iter: Iterator[ColumnarBatch], localSchema: StructType,
                    numMergedVecBatches: SQLMetric) extends Iterator[ColumnarBatch] {

  private val outputQueue = new mutable.Queue[VecBatch]
  private val bufferedVecBatch = new ListBuffer[VecBatch]()
  val columnarConf: GlutenConfig = GlutenConfig.get
  private val maxBatchSizeInBytes: Int = columnarConf.omniColumnarMaxBatchSizeInBytes
  private val maxRowCount: Int = columnarConf.omniColumnarMaxRowCount
  private val mergedBatchThreshold: Int = columnarConf.omniColumnarMergedBatchThreshold
  private var totalRows = 0
  private var currentBatchSizeInBytes = 0

  private def createOmniVectors(schema: StructType, rowSize: Int): Array[Vec] = {
    val vecs = new Array[Vec](schema.fields.length)
    // Resolve the per-task column-vector pool address once (0L outside a Spark task).
    val taskPoolAddress = OmniColumnVector.resolveColumnVectorPoolAddress()
    try {
      schema.fields.zipWithIndex.foreach { case (field, index) =>
        field.dataType match {
          case LongType | TimestampType =>
            vecs(index) = new LongVec(rowSize, taskPoolAddress)
          case DateType | IntegerType =>
            vecs(index) = new IntVec(rowSize, taskPoolAddress)
          case ShortType =>
            vecs(index) = new ShortVec(rowSize, taskPoolAddress)
          case DoubleType =>
            vecs(index) = new DoubleVec(rowSize, taskPoolAddress)
          case FloatType =>
            vecs(index) = new FloatVec(rowSize, taskPoolAddress)
          case BooleanType | NullType =>
            vecs(index) = new BooleanVec(rowSize, taskPoolAddress)
          case ByteType =>
            vecs(index) = new ByteVec(rowSize, taskPoolAddress)
          case StringType | BinaryType =>
            val vecType: DataType = sparkTypeToOmniType(field.dataType, field.metadata)
            vecs(index) = new VarcharVec(rowSize, taskPoolAddress)
          case ArrayType(elementType, _) =>
            vecs(index) = new ArrayVec(new ArrayDataType(sparkTypeToOmniTypeWithComplex(elementType, Metadata.empty)), 0)
          case dt: DecimalType =>
            if (DecimalType.is64BitDecimalType(dt)) {
              vecs(index) = new LongVec(rowSize, taskPoolAddress)
            } else {
              vecs(index) = new Decimal128Vec(rowSize, taskPoolAddress)
            }
          case _ =>
            throw new UnsupportedOperationException("Fail to create omni vector, unsupported fields")
        }
      }
    } catch {
      case e: Exception => {
        for (vec <- vecs) {
          if (vec != null) {
            vec.close()
          }
        }
        throw new RuntimeException("allocate memory failed!")
      }
    }
    vecs
  }

  private def buffer(vecBatch: VecBatch): Unit = {
    var totalSize = 0
    vecBatch.getVectors.zipWithIndex.foreach {
      case (vec, i) =>
        totalSize += vec.getCapacityInBytes
    }
    currentBatchSizeInBytes += totalSize
    totalRows += vecBatch.getRowCount

    bufferedVecBatch.append(vecBatch)
    if (isFull()) {
      flush()
    }
  }

  private def merge(resultBatch: VecBatch, bufferedBatch: ListBuffer[VecBatch]): Unit = {
    localSchema.fields.zipWithIndex.foreach { case (field, index) =>
      var offset = 0
      for (elem <- bufferedBatch) {
        val src: Vec = elem.getVector(index)
        val dest: Vec = resultBatch.getVector(index)
        dest.append(src, offset, elem.getRowCount)
        offset += elem.getRowCount
        src.close()
      }
    }
    // close bufferedBatch
    bufferedBatch.foreach(batch => batch.close())
  }

  private def flush(): Unit = {

    if (bufferedVecBatch.isEmpty) {
      return
    }
    val resultBatch: VecBatch = new VecBatch(createOmniVectors(localSchema, totalRows), totalRows)
    merge(resultBatch, bufferedVecBatch)
    outputQueue.enqueue(resultBatch)
    numMergedVecBatches += 1

    bufferedVecBatch.clear()
    currentBatchSizeInBytes = 0
    totalRows = 0

  }

  private def vecBatchToColumnarBatch(vecBatch: VecBatch): ColumnarBatch = {
    val vectors: Seq[OmniColumnVector] = OmniColumnVector.allocateColumns(
      vecBatch.getRowCount, localSchema, false)
    vectors.zipWithIndex.foreach { case (vector, i) =>
      vector.reset()
      vector.setVec(vecBatch.getVectors()(i))
    }
    vecBatch.close()
    new ColumnarBatch(vectors.toArray, vecBatch.getRowCount)
  }

  override def hasNext: Boolean = {
    while (outputQueue.isEmpty && iter.hasNext) {
      val batch: ColumnarBatch = iter.next()
      val input: Array[Vec] = transColBatchToOmniVecs(batch)
      val vecBatch = new VecBatch(input, batch.numRows())
      if (vecBatch.getRowCount > mergedBatchThreshold) {
        flush()
        outputQueue.enqueue(vecBatch)
      } else {
        buffer(vecBatch)
      }
    }

    if (outputQueue.isEmpty && bufferedVecBatch.isEmpty) {
      false
    } else {
      true
    }
  }

  override def next(): ColumnarBatch = {
    if (outputQueue.nonEmpty) {
      vecBatchToColumnarBatch(outputQueue.dequeue())
    } else if (bufferedVecBatch.nonEmpty) {
      flush()
      vecBatchToColumnarBatch(outputQueue.dequeue())
    } else {
      throw new RuntimeException("bufferedVecBatch and outputQueue are empty")
    }
  }


  def isFull(): Boolean = {
    totalRows > maxRowCount || currentBatchSizeInBytes >= maxBatchSizeInBytes
  }

  def close(): Unit = {
    for (elem <- bufferedVecBatch) {
      elem.releaseAllVectors()
      elem.close()
    }
    for (elem <- outputQueue) {
      elem.releaseAllVectors()
      elem.close()
    }
  }
}
