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
package org.apache.spark.sql.execution

import nova.hetu.omniruntime.vector.Vec
import org.apache.gluten.exception.{GlutenException, GlutenNotSupportException}
import org.apache.gluten.execution.ColumnarToRowExecBase
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.utils.SparkMemoryUtils
import org.apache.gluten.vectorized.OmniColumnVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable.ListBuffer

case class OmniColumnarToRowExec(child: SparkPlan) extends ColumnarToRowExecBase(child = child) {

  override protected def doValidateInternal(): ValidationResult = {
    val schema = child.schema
    for (field <- schema.fields) {
      field.dataType match {
        case _: BooleanType =>
        case _: ByteType =>
        case _: ShortType =>
        case _: IntegerType =>
        case _: LongType =>
        case _: FloatType =>
        case _: DoubleType =>
        case _: StringType =>
        case _: TimestampType =>
        case _: DateType =>
        case _: BinaryType =>
        case _: DecimalType =>
        case _: ArrayType =>
        case _: MapType =>
        case _: StructType =>
        case YearMonthIntervalType.DEFAULT =>
        case _: NullType =>
        case _ =>
          throw new GlutenNotSupportException(
            s"${field.dataType} is unsupported in " +
              s"OmniColumnarToRowExec.")
      }
    }
    ValidationResult.succeeded
  }

  override def doExecuteInternal(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val numInputBatches = longMetric("numInputBatches")
    val omniColumnarToRowTime = longMetric("omniColumnarToRowTime")
    // This avoids calling `output` in the RDD closure, so that we don't need to include the entire
    // plan (this) in the closure.
    val localOutput = this.output
    child.executeColumnar().mapPartitionsInternal { batches =>
      ColumnarBatchToInternalRow.convert(localOutput, batches, numOutputRows, numInputBatches, omniColumnarToRowTime, true)
    }
  }

  object ColumnarBatchToInternalRow {
    final val NANOSECONDS = java.util.concurrent.TimeUnit.NANOSECONDS

    def convert(output: Seq[Attribute], batches: Iterator[ColumnarBatch],
                numOutputRows: SQLMetric, numInputBatches: SQLMetric,
                rowToOmniColumnarTime: SQLMetric,
                mayPartialFetch: Boolean = true): Iterator[InternalRow] = {
      val startTime = System.nanoTime()
      val toUnsafe = UnsafeProjection.create(output, output)

      val batchIter = batches.flatMap { batch =>

        // toClosedVecs closed case: [Deprcated]
        // 1) all rows of batch fetched and closed
        // 2) only fetch Partial rows(eg: top-n, limit-n), closed at task CompletionListener callback
        val toClosedVecs = new ListBuffer[Vec]
        for (i <- 0 until batch.numCols()) {
          batch.column(i) match {
            case vector: OmniColumnVector =>
              toClosedVecs.append(vector.getVec)
            case _ =>
              throw new GlutenException("Not Support batch type.")
          }
        }

        numInputBatches += 1
        val iter = batch.rowIterator().asScala.map(toUnsafe)
        rowToOmniColumnarTime += NANOSECONDS.toMillis(System.nanoTime() - startTime)

        new Iterator[InternalRow] {
          val numOutputRowsMetric: SQLMetric = numOutputRows


          SparkMemoryUtils.addLeakSafeTaskCompletionListener { _ =>
            toClosedVecs.foreach { vec =>
              vec.close()
            }
          }

          override def hasNext: Boolean = {
            val has = iter.hasNext
            // fetch all rows
            if (!has) {
              toClosedVecs.foreach { vec =>
                vec.close()
                toClosedVecs.remove(toClosedVecs.indexOf(vec))
              }
            }
            has
          }

          override def next(): InternalRow = {
            numOutputRowsMetric += 1
            iter.next()
          }
        }
      }
      batchIter
    }
  }

  protected def withNewChildInternal(newChild: SparkPlan): OmniColumnarToRowExec =
    copy(child = newChild)
}
