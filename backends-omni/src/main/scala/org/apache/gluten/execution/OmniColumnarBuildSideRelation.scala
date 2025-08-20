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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, IdentityBroadcastMode}
import org.apache.spark.sql.execution.joins.{BuildSideRelation, HashedRelationBroadcastMode}
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable.ArrayBuffer

case class OmniColumnarBuildSideRelation(
    mode: BroadcastMode,
    output: Seq[Attribute],
    batches: Array[Array[Byte]])
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

      override def hasNext: Boolean = {
        batchId < batches.length
      }

      override def next: ColumnarBatch = {
        val vecBatch = deserializer.deserialize(batches(batchId))
        val vecs = vecBatch.getVectors
        val colBatch =
          VectorTransferUtils.vectorsToNativeColumnarBatch(vecs, dataTypes, vecBatch.getRowCount)
        batchId += 1
        colBatch
      }
    }
  }

  override def asReadOnlyCopy(): OmniColumnarBuildSideRelation = this

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
