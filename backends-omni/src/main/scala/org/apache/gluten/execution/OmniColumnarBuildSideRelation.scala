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
package org.apache.gluten.execution

import nova.hetu.omniruntime.vector.VecBatch
import nova.hetu.omniruntime.vector.serialize.VecBatchSerializerFactory
import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.vectorized.VectorTransferUtils
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, IdentityBroadcastMode}
import org.apache.spark.sql.execution.joins.{BuildSideRelation, HashedRelationBroadcastMode}
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable.ArrayBuffer

/**
 * @param mode         the Spark broadcast mode
 * @param output       output attributes of the build side
 * @param batches      serialized columnar batches (driver-side broadcast payload)
 * @param broadcastCacheKey exchange descriptor shared by all compatible join
 *                          signatures; empty string disables caching
 */
case class OmniColumnarBuildSideRelation(
    mode: BroadcastMode,
    output: Seq[Attribute],
    batches: Array[Array[Byte]],
    broadcastCacheKey: String = "")
  extends BuildSideRelation {

  private def transformProjection: UnsafeProjection = {
    mode match {
      case HashedRelationBroadcastMode(k, _) => UnsafeProjection.create(k)
      case IdentityBroadcastMode => UnsafeProjection.create(output, output)
    }
  }

  override def deserialized: Iterator[ColumnarBatch] = {

    val deserializer = VecBatchSerializerFactory.create()
    val dataTypes = ConverterUtils.collectAttributeTypeNodes(output)

    new Iterator[ColumnarBatch] {
      var batchId = 0
      private var poolOwner: TaskContext = null
      private var resolvedPoolAddress = 0L
      private var poolResolved = false

      private def currentTaskPoolAddress(): Long = {
        val context = TaskContext.get()
        // The cache belongs to this iterator, not the shared broadcast relation.
        // Refresh on a context change and reject access after task completion.
        if (context != null && context.isCompleted()) {
          throw new IllegalStateException("Cannot deserialize a broadcast after task completion")
        }
        if (!poolResolved || (context ne poolOwner)) {
          val address = org.apache.gluten.vectorized.OmniColumnVector.resolveColumnVectorPoolAddress()
          poolOwner = context
          resolvedPoolAddress = address
          poolResolved = true
        }
        resolvedPoolAddress
      }

      override def hasNext: Boolean = {
        batchId < batches.length
      }

      override def next: ColumnarBatch = {
        // Reuse this iterator's task pool and preserve it in the batch's Java wrappers.
        val taskPoolAddress = currentTaskPoolAddress()
        val vecBatch = deserializer.deserialize(batches(batchId), taskPoolAddress)
        val vecs = vecBatch.getVectors
        val colBatch = VectorTransferUtils.vectorsToNativeColumnarBatch(
          vecs, dataTypes, vecBatch.getRowCount, taskPoolAddress)
        batchId += 1
        colBatch
      }
    }
  }

  override def asReadOnlyCopy(): OmniColumnarBuildSideRelation = this

  // Spark may reset a relation while another consumer of the same exchange is
  // running. The SQL execution owner retires all executor entries at query end.
  override def reset(): Unit = {}

  /**
   * Transform columnar broadcast value to Array[InternalRow] by key and distinct. NOTE: This method
   * was called in Spark Driver, should manage resources carefully.
   */
  override def transform(key: Expression): Array[InternalRow] = {
    if (batches.length == 0) {
      Iterator.empty.toArray
    } else {
      val deserializer = VecBatchSerializerFactory.create()

      val proj = UnsafeProjection.create(Seq(key))

      val dataTypes = ConverterUtils.collectAttributeTypeNodes(output)
      val retRows = new ArrayBuffer[InternalRow]()
      batches.foreach {
        input =>
          var vecBatch: VecBatch = null
          try {
            vecBatch = deserializer.deserialize(input)
            val columnarBatch = VectorTransferUtils.vectorsToNativeColumnarBatch(
              vecBatch.getVectors,
              dataTypes,
              vecBatch.getRowCount)
            columnarBatch
              .rowIterator()
              .asScala
              .map(transformProjection)
              .map(proj)
              .map(_.copy())
              .foreach(retRows.append(_))
          } finally {
            if (vecBatch != null) {
              try {
                vecBatch.releaseAllVectors()
              } finally {
                vecBatch.close()
              }
            }
          }
      }
      retRows.toArray
    }
  }
}
