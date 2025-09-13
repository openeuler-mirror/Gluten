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
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.utils.SparkMemoryUtils
import org.apache.gluten.vectorized.OmniColumnVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._


case class RowToOmniColumnarExec(child: SparkPlan) extends RowToColumnarExecBase(child = child) {
  override def doExecuteColumnarInternal(): RDD[ColumnarBatch] = {
    val enableOffHeapColumnVector = SQLConf.get.offHeapColumnVectorEnabled
    val numInputRows = longMetric("numInputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val rowToOmniColumnarTime = longMetric("rowToOmniColumnarTime")
    val numRows = GlutenConfig.get.maxBatchSize
    // This avoids calling `schema` in the RDD closure, so that we don't need to include the entire
    // plan (this) in the closure.
    val localSchema = schema
    child.execute().mapPartitions { rowIterator =>
      InternalRowToColumnarBatch.convert(enableOffHeapColumnVector, numInputRows, numOutputBatches, rowToOmniColumnarTime, numRows, localSchema, rowIterator)
    }
  }

  // For spark 3.2.
  protected def withNewChildInternal(newChild: SparkPlan): RowToOmniColumnarExec =
    copy(child = newChild)
}

object InternalRowToColumnarBatch {
  final val NANOSECONDS  = java.util.concurrent.TimeUnit.NANOSECONDS
  def convert(enableOffHeapColumnVector: Boolean,
              numInputRows: SQLMetric,
              numOutputBatches: SQLMetric,
              rowToOmniColumnarTime: SQLMetric,
              numRows: Int, localSchema: StructType,
              rowIterator: Iterator[InternalRow]): Iterator[ColumnarBatch] = {
    if (rowIterator.hasNext) {
      new Iterator[ColumnarBatch] {
        private val converters = new OmniRowToColumnConverter(localSchema)

        override def hasNext: Boolean = {
          rowIterator.hasNext
        }

        override def next(): ColumnarBatch = {
          val startTime = System.nanoTime()
          val vectors: Seq[WritableColumnVector] = OmniColumnVector.allocateColumns(numRows,
            localSchema, true)
          val cb: ColumnarBatch = new ColumnarBatch(vectors.toArray)
          cb.setNumRows(0)
          vectors.foreach(_.reset())
          var rowCount = 0
          while (rowCount < numRows && rowIterator.hasNext) {
            val row = rowIterator.next()
            converters.convert(row, vectors.toArray)
            rowCount += 1
          }
          if (!enableOffHeapColumnVector) {
            vectors.foreach { v =>
              v.asInstanceOf[OmniColumnVector].getVec.setSize(rowCount)
            }
          }
          cb.setNumRows(rowCount)
          numInputRows += rowCount
          numOutputBatches += 1
          rowToOmniColumnarTime += NANOSECONDS.toMillis(System.nanoTime() - startTime)
          cb
        }
      }
    } else {
      Iterator.empty
    }
  }
}

private[execution] class OmniRowToColumnConverter(schema: StructType) extends Serializable {
  private val converters = schema.fields.map {
    f => OmniRowToColumnConverter.getConverterForType(f.dataType, f.nullable)
  }

  final def convert(row: InternalRow, vectors: Array[WritableColumnVector]): Unit = {
    var idx = 0
    while (idx < row.numFields) {
      converters(idx).append(row, idx, vectors(idx))
      idx += 1
    }
  }
}

/**
 * Provides an optimized set of APIs to extract a column from a row and append it to a
 * [[WritableColumnVector]].
 */
private object OmniRowToColumnConverter {
  SparkMemoryUtils.init()

  private abstract class TypeConverter extends Serializable {
    def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit
  }

  private final case class BasicNullableTypeConverter(base: TypeConverter) extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      if (row.isNullAt(column)) {
        cv.appendNull
      } else {
        base.append(row, column, cv)
      }
    }
  }

  private def getConverterForType(dataType: DataType, nullable: Boolean): TypeConverter = {
    val core = dataType match {
      case BinaryType => BinaryConverter
      case BooleanType => BooleanConverter
      case ByteType => ByteConverter
      case ShortType => ShortConverter
      case IntegerType | DateType => IntConverter
      case LongType | TimestampType => LongConverter
      case DoubleType => DoubleConverter
      case StringType => StringConverter
      case CalendarIntervalType => CalendarConverter
      case dt: DecimalType => DecimalConverter(dt)
      case unknown => throw new UnsupportedOperationException(
        s"Type $unknown not supported")
    }

    if (nullable) {
      dataType match {
        case _ => new BasicNullableTypeConverter(core)
      }
    } else {
      core
    }
  }

  private object BinaryConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      val bytes = row.getBinary(column)
      cv.appendByteArray(bytes, 0, bytes.length)
    }
  }

  private object BooleanConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit =
      cv.appendBoolean(row.getBoolean(column))
  }

  private object ByteConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit =
      cv.appendByte(row.getByte(column))
  }

  private object ShortConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit =
      cv.appendShort(row.getShort(column))
  }

  private object IntConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit =
      cv.appendInt(row.getInt(column))
  }

  private object LongConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit =
      cv.appendLong(row.getLong(column))
  }

  private object DoubleConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit =
      cv.appendDouble(row.getDouble(column))
  }

  private object StringConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      val data = row.getUTF8String(column).getBytes
      cv.asInstanceOf[OmniColumnVector].appendString(data.length, data, 0)
    }
  }

  private object CalendarConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      val c = row.getInterval(column)
      cv.appendStruct(false)
      cv.getChild(0).appendInt(c.months)
      cv.getChild(1).appendInt(c.days)
      cv.getChild(2).appendLong(c.microseconds)
    }
  }

  private case class DecimalConverter(dt: DecimalType) extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      val d = row.getDecimal(column, dt.precision, dt.scale)
      if (DecimalType.is64BitDecimalType(dt)) {
        cv.appendLong(d.toUnscaledLong)
      } else {
        cv.asInstanceOf[OmniColumnVector].appendDecimal(d)
      }
    }
  }
}

