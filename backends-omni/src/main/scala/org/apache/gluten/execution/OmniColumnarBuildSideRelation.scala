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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BoundReference, Expression, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable.ArrayBuffer

case class OmniColumnarBuildSideRelation(
    mode: BroadcastMode,
    output: Seq[Attribute],
    batches: Array[Array[Byte]])
  extends BuildSideRelation {

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
      val columnNames = key.flatMap {
        case expression: AttributeReference => Some(expression)
        case _ => None
      }
      if (columnNames.isEmpty) {
        throw new IllegalArgumentException(s"Key column not found in expression: $key")
      }
      if (columnNames.size != 1) {
        throw new IllegalArgumentException(s"Multiple key columns found in expression: $key")
      }
      val columnExpr = columnNames.head
      val oneColumnWithSameName = output.count(_.name == columnExpr.name) == 1
      val columnInOutput = output.zipWithIndex.filter {
        p: (Attribute, Int) =>
          if (oneColumnWithSameName) {
            // The comparison of exprId can be ignored when
            // only one attribute name match is found.
            p._1.name == columnExpr.name
          } else {
            // A case where output has multiple columns with same name
            p._1.name == columnExpr.name && p._1.exprId == columnExpr.exprId
          }
      }
      if (columnInOutput.isEmpty) {
        throw new IllegalStateException(
          s"Key $key not found from build side relation output: $output")
      }
      if (columnInOutput.size != 1) {
        throw new IllegalStateException(
          s"More than one key $key found from build side relation output: $output")
      }
      val replacement =
        BoundReference(columnInOutput.head._2, columnExpr.dataType, columnExpr.nullable)

      val projExpr = key.transformDown {
        case _: AttributeReference =>
          replacement
      }

      val proj = UnsafeProjection.create(projExpr)
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
