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

import nova.hetu.omniruntime.vector.{StringViewVec, Vec}
import org.apache.gluten.exception.{GlutenException, GlutenNotSupportException}
import org.apache.gluten.execution.ColumnarToRowExecBase
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.utils.SparkMemoryUtils
import org.apache.gluten.vectorized.OmniColumnVector
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
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
    val stringColumnIndexes = child.schema.fields.zipWithIndex.collect {
      case (field, index) if field.dataType == StringType => index
    }
    val runtimeValidationEnabled = SQLConf.get.getConfString(
      "spark.omni.stringview.runtimeValidation.enabled", "false").toBoolean
    child.executeColumnar().mapPartitionsInternal { batches =>
      var validationLogged = false
      val validatedBatches = if (runtimeValidationEnabled) {
        batches.map { batch =>
          ColumnarBatchToInternalRow.validateStringViewInput(batch, stringColumnIndexes)
          if (!validationLogged) {
            System.err.println(
              "SV_E2E_C2R inputType=OMNI_STRING_VIEW output=InternalRow")
            validationLogged = true
          }
          batch
        }
      } else {
        batches
      }
      ColumnarBatchToInternalRow.convert(
        localOutput,
        validatedBatches,
        numOutputRows,
        numInputBatches,
        omniColumnarToRowTime,
        true)
    }
  }

  override def doExecuteBroadcast[T](): Broadcast[T] = {
    val numOutputRows = longMetric("numOutputRows")
    val numInputBatches = longMetric("numInputBatches")
    val convertTime = longMetric("omniColumnarToRowTime")

    val mode = BroadcastUtils.getBroadcastMode(outputPartitioning)
    val relation = child.executeBroadcast[T]()

    BroadcastUtils.omniToSparkUnsafe(
      sparkContext,
      mode,
      relation,
      ColumnarBatchToInternalRow.convert(output, _, numOutputRows, numInputBatches, convertTime))
  }

  protected def withNewChildInternal(newChild: SparkPlan): OmniColumnarToRowExec =
    copy(child = newChild)
}

object ColumnarBatchToInternalRow {
  final val NANOSECONDS = java.util.concurrent.TimeUnit.NANOSECONDS

  private[execution] def validateStringViewInput(
      batch: ColumnarBatch,
      stringColumnIndexes: Seq[Int]): Unit = {
    require(
      stringColumnIndexes.nonEmpty,
      "STRING_VIEW_RUNTIME_VALIDATION: C2R requires at least one Spark StringType column")
    stringColumnIndexes.foreach { index =>
      val vector = batch.column(index) match {
        case omniVector: OmniColumnVector => omniVector
        case other =>
          throw new IllegalArgumentException(
            s"STRING_VIEW_RUNTIME_VALIDATION: C2R expected OmniColumnVector at column $index, " +
              s"got ${other.getClass.getName}")
      }
      require(
        vector.getVec.isInstanceOf[StringViewVec],
        s"STRING_VIEW_RUNTIME_VALIDATION: C2R expected StringViewVec at column $index, " +
          s"got ${vector.getVec.getClass.getName}")
    }
  }

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
