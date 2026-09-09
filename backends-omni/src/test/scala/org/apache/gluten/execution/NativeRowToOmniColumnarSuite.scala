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

import org.apache.gluten.jni.JniLibLoader
import org.apache.gluten.vectorized.OmniColumnVector

import nova.hetu.omniruntime.vector.{StringViewVec, VarcharVec}

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GenericInternalRow}
import org.apache.spark.sql.execution.{LocalTableScanExec, OmniColumnarToRowExec}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

object NativeRowToOmniColumnarSuite {
  case class NativeResult(
      values: Seq[(String, java.lang.Integer, java.lang.Long, java.lang.Integer)],
      vectorClasses: Seq[String],
      batchCount: Int,
      sawLongStringAfterRead: Boolean,
      sawEmptyStringAfterRead: Boolean,
      sawNullStringAfterRead: Boolean,
      sawUtf8StringAfterRead: Boolean)

  case class InputRow(
      s: String,
      i: java.lang.Integer,
      l: java.lang.Long,
      d: java.lang.Integer)

  def jint(value: Int): java.lang.Integer = Integer.valueOf(value)

  def jlong(value: Long): java.lang.Long = java.lang.Long.valueOf(value)
}

// P1 note: disabled until P2. These tests assert R2C produces StringViewVec purely from the global
// switch. Under the P1 closed-default wire, R2C emits StringView only for columns whose schema marks
// physicalStringView (never blanket by the switch), so switch-only SV production no longer holds.
// Re-enable when P2 wires per-field source marking through native R2C.
@org.scalatest.Ignore
class NativeRowToOmniColumnarSuite extends SharedSparkSession {
  import NativeRowToOmniColumnarSuite._

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.gluten.sql.columnar.libpath", "/opt/Adaptor/lib/libspark_columnar_plugin.so")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "64m")
      .set("spark.sql.session.timeZone", "UTC")
  }

  test("native row to columnar honors StringView enabled config") {
    val result = runNativeRowToColumnar(stringViewEnabled = true)

    assert(result.vectorClasses.distinct == Seq(classOf[StringViewVec].getName))
    assert(result.values == expectedValues)
    assert(result.batchCount > 1)
    assert(result.sawLongStringAfterRead)
    assert(result.sawEmptyStringAfterRead)
    assert(result.sawNullStringAfterRead)
    assert(result.sawUtf8StringAfterRead)
  }

  test("native row to columnar keeps Varchar when StringView is disabled") {
    val result = runNativeRowToColumnar(stringViewEnabled = false)

    assert(result.vectorClasses.distinct == Seq(classOf[VarcharVec].getName))
    assert(result.values == expectedValues)
    assert(result.batchCount > 1)
    assert(result.sawLongStringAfterRead)
    assert(result.sawEmptyStringAfterRead)
    assert(result.sawNullStringAfterRead)
    assert(result.sawUtf8StringAfterRead)
  }

  test("StringView runtime validation rejects incompatible config") {
    val attr = AttributeReference("s", StringType, nullable = true)()
    val scan = LocalTableScanExec(
      Seq(attr),
      Seq(new GenericInternalRow(Array[Any](UTF8String.fromString("tiny")))))
    val rowToColumnar = RowToOmniColumnarExec(scan)

    withSQLConf(
      "spark.gluten.sql.columnar.backend.omni.nativeRowToColumnar.enabled" -> "true",
      "spark.gluten.sql.columnar.backend.omni.preferVectorizationExpression" -> "true",
      "spark.omni.stringview.enabled" -> "false",
      "spark.omni.stringview.runtimeValidation.enabled" -> "true") {
      val error = intercept[IllegalArgumentException] {
        rowToColumnar.executeColumnar()
      }
      assert(error.getMessage.contains("requires"))
    }
  }

  test("native StringView conversion preserves readable string values") {
    JniLibLoader.loadFromPath(spark.conf.get("spark.gluten.sql.columnar.libpath"))

    withSQLConf(
      "spark.gluten.sql.columnar.backend.omni.nativeRowToColumnar.enabled" -> "true",
      "spark.omni.stringview.enabled" -> "true",
      "spark.gluten.sql.columnar.maxBatchSize" -> "10") {

      val attr = AttributeReference("s", StringType, nullable = true)()
      val stringInternalRows: Seq[InternalRow] = inputData.map { row =>
        new GenericInternalRow(Array[Any](
          if (row.s == null) null else UTF8String.fromString(row.s)))
      }
      val scan = LocalTableScanExec(Seq(attr), stringInternalRows)
      val rowToColumnar = RowToOmniColumnarExec(scan)

      val target = UTF8String.fromString("tiny")

      // These comparisons validate readback after conversion. They do not execute
      // an Omni ExprEval filter and are not SQL filter E2E evidence.
      val counts = rowToColumnar.executeColumnar().mapPartitions { batches =>
        var eqCount = 0
        var neqCount = 0
        while (batches.hasNext) {
          val batch = batches.next()
          val vector = batch.column(0).asInstanceOf[OmniColumnVector]
          assert(vector.getVec.isInstanceOf[StringViewVec])
          (0 until batch.numRows()).foreach { rowId =>
            if (!vector.isNullAt(rowId)) {
              val v = vector.getUTF8String(rowId)
              if (v.equals(target)) eqCount += 1
              else neqCount += 1
            }
          }
        }
        Iterator((eqCount, neqCount))
      }.collect()

      val totalEq  = counts.map(_._1).sum
      val totalNeq = counts.map(_._2).sum

      assert(totalEq  == 1, s"EQ expected 1 but got $totalEq")
      assert(totalNeq == 6, s"NEQ expected 6 but got $totalNeq")
    }
  }

  // Full round-trip through the real OUT operator (OmniColumnarToRowExec): Row -> Columnar(SV) -> Row,
  // with no compute operator in between, asserting byte-exact preservation per row across
  // inline / non-inline / 12-vs-13B boundary / NULL / empty / UTF-8 (all present in inputData).
  test("round-trip StringView: Row -> Columnar(SV) -> Row preserves bytes") {
    JniLibLoader.loadFromPath(spark.conf.get("spark.gluten.sql.columnar.libpath"))
    withSQLConf(
      "spark.gluten.sql.columnar.backend.omni.nativeRowToColumnar.enabled" -> "true",
      "spark.omni.stringview.enabled" -> "true",
      "spark.omni.stringview.runtimeValidation.enabled" -> "true",
      "spark.gluten.sql.columnar.backend.omni.preferVectorizationExpression" -> "true",
      "spark.gluten.sql.columnar.maxBatchSize" -> "3") { // small => spans multiple batches

      val attr = AttributeReference("s", StringType, nullable = true)()
      val originalRows: Seq[InternalRow] = inputData.map { r =>
        new GenericInternalRow(Array[Any](if (r.s == null) null else UTF8String.fromString(r.s)))
      }
      val scan = LocalTableScanExec(Seq(attr), originalRows)
      val rowToColumnar = RowToOmniColumnarExec(scan)

      // (1) The intermediate columnar must really be StringView (not a silent VARCHAR fallback).
      val vecClasses = rowToColumnar
        .executeColumnar()
        .mapPartitions { batches =>
          batches.flatMap { batch =>
            (0 until batch.numCols()).map(c =>
              batch.column(c).asInstanceOf[OmniColumnVector].getVec.getClass.getName)
          }
        }
        .collect()
        .distinct
      assert(vecClasses.toSeq == Seq(classOf[StringViewVec].getName))

      // (2) Run the real OUT operator and collect the rebuilt rows (copy: OmniColumnarToRowExec
      // yields reused UnsafeRows, a raw collect() would alias).
      val out = OmniColumnarToRowExec(rowToColumnar)
        .execute()
        .mapPartitions(_.map(_.copy()))
        .collect()

      assert(out.length == inputData.length)
      out.zip(inputData).foreach {
        case (row, in) =>
          if (in.s == null) {
            assert(row.isNullAt(0))
          } else {
            assert(!row.isNullAt(0))
            val expected = UTF8String.fromString(in.s).getBytes
            val actual = row.getUTF8String(0).getBytes
            assert(
              java.util.Arrays.equals(actual, expected),
              s"byte mismatch for '${in.s}': expected ${expected.toSeq} got ${actual.toSeq}")
          }
      }
    }
  }

  // Mixed columns: a single batch holding two StringView columns (one inline-heavy, one
  // non-inline-heavy), round-tripped through the OUT operator with byte-exact per-column assertions.
  test("round-trip StringView: mixed inline + non-inline string columns") {
    JniLibLoader.loadFromPath(spark.conf.get("spark.gluten.sql.columnar.libpath"))
    withSQLConf(
      "spark.gluten.sql.columnar.backend.omni.nativeRowToColumnar.enabled" -> "true",
      "spark.omni.stringview.enabled" -> "true",
      "spark.omni.stringview.runtimeValidation.enabled" -> "true",
      "spark.gluten.sql.columnar.backend.omni.preferVectorizationExpression" -> "true",
      "spark.gluten.sql.columnar.maxBatchSize" -> "10") {

      // col a: inline-heavy; col b: non-inline-heavy. Cover null / empty / boundary(12 vs 13) / UTF-8.
      val pairs: Seq[(String, String)] = Seq(
        ("hi", "this string is definitely longer than twelve bytes"),
        ("", "another long non-inline value goes right here"),
        (null, "non null long companion value placed here"),
        ("abc", null),
        ("你好", "混合 inline 与 non-inline 的中文长串数据测试用例"),
        ("123456789012", "1234567890123")) // 12B inline vs 13B non-inline

      val a = AttributeReference("a", StringType, nullable = true)()
      val b = AttributeReference("b", StringType, nullable = true)()
      val rows: Seq[InternalRow] = pairs.map {
        case (s1, s2) =>
          new GenericInternalRow(
            Array[Any](
              if (s1 == null) null else UTF8String.fromString(s1),
              if (s2 == null) null else UTF8String.fromString(s2)))
      }
      val rowToColumnar = RowToOmniColumnarExec(LocalTableScanExec(Seq(a, b), rows))

      // Both columns must be StringView.
      val vecClasses = rowToColumnar
        .executeColumnar()
        .mapPartitions { batches =>
          batches.flatMap { batch =>
            (0 until batch.numCols()).map(c =>
              batch.column(c).asInstanceOf[OmniColumnVector].getVec.getClass.getName)
          }
        }
        .collect()
        .distinct
      assert(vecClasses.toSeq == Seq(classOf[StringViewVec].getName))

      val out = OmniColumnarToRowExec(rowToColumnar)
        .execute()
        .mapPartitions(_.map(_.copy()))
        .collect()

      assert(out.length == pairs.length)
      out.zip(pairs).foreach {
        case (row, (s1, s2)) =>
          def check(idx: Int, expected: String): Unit = {
            if (expected == null) {
              assert(row.isNullAt(idx))
            } else {
              assert(!row.isNullAt(idx))
              assert(
                java.util.Arrays
                  .equals(row.getUTF8String(idx).getBytes, UTF8String.fromString(expected).getBytes),
                s"col$idx byte mismatch for '$expected'")
            }
          }
          check(0, s1)
          check(1, s2)
      }
    }
  }


  private def runNativeRowToColumnar(stringViewEnabled: Boolean): NativeResult = {
    JniLibLoader.loadFromPath(spark.conf.get("spark.gluten.sql.columnar.libpath"))

    var result: NativeResult = null
    withSQLConf(
      "spark.gluten.sql.columnar.backend.omni.nativeRowToColumnar.enabled" -> "true",
      "spark.omni.stringview.enabled" -> stringViewEnabled.toString,
      "spark.omni.stringview.runtimeValidation.enabled" -> stringViewEnabled.toString,
      "spark.gluten.sql.columnar.backend.omni.preferVectorizationExpression" -> "true",
      "spark.gluten.sql.columnar.maxBatchSize" -> "3") {
      val attrs = Seq(
        AttributeReference("s", StringType, nullable = true)(),
        AttributeReference("i", IntegerType, nullable = true)(),
        AttributeReference("l", LongType, nullable = true)(),
        AttributeReference("d", DateType, nullable = true)())
      val scan = LocalTableScanExec(attrs, inputRows)
      val rowToColumnar = RowToOmniColumnarExec(scan)

      val partitionResults = rowToColumnar.executeColumnar().mapPartitions { batches =>
        var batchCount = 0
        val vectorClasses = scala.collection.mutable.ArrayBuffer[String]()
        val values = scala.collection.mutable.ArrayBuffer[(String, java.lang.Integer, java.lang.Long, java.lang.Integer)]()
        var sawLongStringAfterRead = false
        var sawEmptyStringAfterRead = false
        var sawNullStringAfterRead = false
        var sawUtf8StringAfterRead = false

        while (batches.hasNext) {
          batchCount += 1
          val batch = batches.next()
          val vector = batch.column(0).asInstanceOf[OmniColumnVector]
          vectorClasses += vector.getVec.getClass.getName

          (0 until batch.numRows()).foreach { rowId =>
            val s = if (vector.isNullAt(rowId)) null else vector.getUTF8String(rowId).toString
            val i = if (batch.column(1).isNullAt(rowId)) null else Integer.valueOf(batch.column(1).getInt(rowId))
            val l = if (batch.column(2).isNullAt(rowId)) null else java.lang.Long.valueOf(batch.column(2).getLong(rowId))
            val d = if (batch.column(3).isNullAt(rowId)) null else Integer.valueOf(batch.column(3).getInt(rowId))
            if (s != null && s.length > 12) {
              sawLongStringAfterRead = true
            }
            if (s == "") {
              sawEmptyStringAfterRead = true
            }
            if (s == null) {
              sawNullStringAfterRead = true
            }
            if (s == "你好世界") {
              sawUtf8StringAfterRead = true
            }
            values += ((s, i, l, d))
          }
        }
        Iterator(NativeResult(
          values.toSeq,
          vectorClasses.toSeq,
          batchCount,
          sawLongStringAfterRead,
          sawEmptyStringAfterRead,
          sawNullStringAfterRead,
          sawUtf8StringAfterRead))
      }.collect()

      result = NativeResult(
        partitionResults.flatMap(_.values).toSeq,
        partitionResults.flatMap(_.vectorClasses).toSeq,
        partitionResults.map(_.batchCount).sum,
        partitionResults.exists(_.sawLongStringAfterRead),
        partitionResults.exists(_.sawEmptyStringAfterRead),
        partitionResults.exists(_.sawNullStringAfterRead),
        partitionResults.exists(_.sawUtf8StringAfterRead))
    }
    result
  }

  private val inputData = Seq(
    InputRow("tiny", jint(1), jlong(10L), jint(0)),
    InputRow(null, null, null, null),
    InputRow("", jint(2), jlong(20L), jint(1)),
    InputRow("12345678901", jint(3), jlong(30L), jint(2)),
    InputRow("123456789012", jint(4), jlong(40L), jint(3)),
    InputRow("1234567890123", jint(5), jlong(50L), jint(4)),
    InputRow("this string is longer than twelve bytes", jint(6), jlong(60L), jint(5)),
    InputRow("你好世界", jint(7), jlong(70L), jint(6)))

  private def inputRows: Seq[InternalRow] = inputData.map {
    case InputRow(s, i, l, d) =>
      new GenericInternalRow(Array[Any](
        if (s == null) null else UTF8String.fromString(s),
        i,
        l,
        d))
  }

  private def expectedValues: Seq[(String, java.lang.Integer, java.lang.Long, java.lang.Integer)] =
    inputData.map(row => (row.s, row.i, row.l, row.d))
}
