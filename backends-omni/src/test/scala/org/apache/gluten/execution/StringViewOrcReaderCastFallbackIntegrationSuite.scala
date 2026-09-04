/*
 * Copyright (C) 2026-2026. Huawei Technologies Co., Ltd. All rights reserved.
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

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.StringViewToOmniVarcharCast
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.functions.{sum, row_number}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.StringType

/*
E2E TEST OVERVIEW
Tests check result in two ways:
 - by asserting normalized plans with plain string plans,
   checking there's string_view_to_omni_varchar_cast in right place
 - by asserting output of executed query
 - by checking physical types of strings outputed by each operator (probes)
 - plan is checked in three stages:
    - "Before" (SV OFF)
    - "After" (SV ON)
    - "Idempotent" (SV ON)
      - checks whether casting vector twice doesn't change the outcome
*/

class StringViewOrcReaderCastFallbackIntegrationSuite extends SharedSparkSession {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.gluten.sql.columnar.libpath", "/opt/Adaptor/lib/libspark_columnar_plugin.so")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "256m")
      .set("spark.sql.session.timeZone", "UTC")
      // AQE off
      .set("spark.sql.adaptive.enabled", "false")
  }
  private def collectAll(plan: SparkPlan): Seq[SparkPlan] = plan +: plan.children.flatMap(collectAll)



  // Normalizes the plan, so it can be used for asserts with plain text
  private def normalizePlanString(plan: String): String = {
    plan
      // Remove # idenifiers
      .replaceAll("#\\d+", "")
      // Remove node idenifiers - ^(...), *(...)
      .replaceAll("[*^]\\(\\d+\\)", "")
      // Remove specific file names
      .replaceAll("file:[^\\s,\\]]+", "file:path")
      // Remove exchange specifics
       .replaceAll("\\+- Exchange[^\\n]+", "+- Exchange")
      // Removes double spaces
      .replaceAll(" +", " ")
      // Trim the lines
      .split("\n")
      .map(_.trim())
      .filter(_.nonEmpty)
      .mkString("\n")
  }

  private def assertPlan(stage: String, testId: Int, plan: SparkPlan, expectedPlan: String): Unit = {
    val planString = normalizePlanString(plan.treeString)
    val expectedPlanString = normalizePlanString(expectedPlan)

    println(s"===EXECUTED-PLAN-${stage}-${testId}===")
    println(planString)

    assert(planString == expectedPlanString, s"query's plan should be ${expectedPlanString}, instead got ${planString}")
  }

  private def assertQueryResult[T: Ordering](stage: String, testId: Int, rows: Seq[T], expected: Seq[T], orderMatters: Boolean): Unit = {
    println(s"===RESULT-ROWS-${stage}-${testId}=== ${rows.mkString("[", ", ", "]")}")
    if (orderMatters) {
      assert(rows == expected, s"query's result should be ${expected}, got ${rows}")
    }
    else {
      assert(rows.length == expected.length, s"query should output ${expected.length} rows, instead got ${rows.length}")
      assert(rows.sorted == expected.sorted, s"query's output should contain ${expected}, got ${rows}")
    }
  }


  private def probeOutputTypes(plan: SparkPlan): Seq[(String, Seq[String])] = {
    collectAll(plan).map { el =>
      val types = el.output.map { attr =>
        if (StringViewToOmniVarcharCast.isPhysicalStringView(attr)) "StringView"
        else if (attr.dataType == StringType) "OmniVarchar"
        else attr.dataType.typeName
      }
      (el.getClass.getSimpleName, types)
    }
  }

  // Checks physical types of operators' outputs to see whether StringViewToOmniVarcharCast works well
  // @expectedCastIndex is index of Projection with StringView -> Omni Varchar Cast in operators Sequence
  // Operators before should output StringViews, operators after should output Omni Varchars
  // !!! Doesn't work if cast is partial !!!
  private def probeStringViewToOmniVarcharCast(stage:String, testId: Int, plan: SparkPlan, expectedCastIndex: Int): Unit = {
    val typesSeq = probeOutputTypes(plan).reverse
    println(s"===OPERATORS-TYPES-${stage}-${testId}===${typesSeq}")

    for (operatorId <- 0 until typesSeq.length) {
      var hasStringView = false
      var hasVarchar = false

      typesSeq(operatorId)._2.foreach {el =>
        if (el == "StringView") {
          hasStringView = true
        }
        else if (el == "OmniVarchar") {
          hasVarchar = true;
        }
      }
      assert(!hasStringView || !hasVarchar, s"function doesn't work for partial cast")

      if (operatorId < expectedCastIndex) {
        assert(!hasVarchar, "found OmniVarchar before cast")
      }
      else {
        assert(!hasStringView, "found StringView after cast")
      }
    }
  }

  private val sqlConfigSVEnabled : Seq[(String, String)] = Seq(
    "spark.gluten.sql.columnar.backend.omni.nativeRowToColumnar.enabled" -> "true",
      "spark.gluten.sql.columnar.backend.omni.preferVectorizationExpression" -> "true",
      "spark.omni.stringview.enabled" -> "true",
      "spark.omni.stringview.fallback.enabled" -> "true",
      "spark.sql.optimizer.excludedRules" ->
        "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"
  )
  private val sqlConfigSVDisabled : Seq[(String, String)] = Seq(
    "spark.gluten.sql.columnar.backend.omni.nativeRowToColumnar.enabled" -> "true",
      "spark.gluten.sql.columnar.backend.omni.preferVectorizationExpression" -> "true",
      "spark.omni.stringview.enabled" -> "false",
      "spark.omni.stringview.fallback.enabled" -> "false",
      "spark.sql.optimizer.excludedRules" ->
        "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"
  )



  test("sort-fallback-1: ORC Reader reads strings as StringViews and materializes to Omni VARCHAR at sort") {
    withTempPath { dir =>
      import testImplicits._

      val testId = 1

      val path = dir.getAbsolutePath
      val data = Seq("Paris", "sth", "Very Long Instruction Word", "VLIW", "abcdefghijkl", "abcdefghijklm")
      data.toDF("s").repartition(1).write.orc(path)

      // Before
      withSQLConf(sqlConfigSVDisabled: _*) {
        val df = spark.read.orc(path)
          .sortWithinPartitions("s")

        val plan = df.queryExecution.executedPlan

        assertPlan("BEFORE", testId, plan, """
          OmniColumnarToRow
            +- SortExecTransformer [s ASC NULLS FIRST], false, 0
            +- FileScanTransformer orc-native [s] Batched: true, DataFilters: [], Format: ORC-NATIVE, Location: InMemoryFileIndex(1 paths)[file:path], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<s:string> NativeFilters: []
        """.stripMargin)

        probeStringViewToOmniVarcharCast("BEFORE", testId, plan, -1)
      }

      // After
      withSQLConf(sqlConfigSVEnabled: _*) {
        // Enables check if test run in shell script
        println("===DIFF");

        val df = spark.read.orc(path)
          .sortWithinPartitions("s")
        val plan = df.queryExecution.executedPlan

        // Adaptation of test from StringViewSortFallbackIntegrationSuite.scala
        // Checks if everything is set properly (sanity check)
        val nodes = collectAll(plan)

        val hasFallbackCast = nodes.exists(
          _.expressions.exists(e => e.exists(_.isInstanceOf[StringViewToOmniVarcharCast]))
        )
        val hasSortTransformer = nodes.exists(_.isInstanceOf[SortExecTransformer])
        val hasSparkSort = nodes.exists(_.getClass.getSimpleName == "SortExec")

        // Sanity check (from previous tests)
        assert(hasFallbackCast, "expected StringViewToOmniVarcharCast materialize")
        assert(hasSortTransformer, "expected SortExecTransformer")
        assert(!hasSparkSort, "should not fall back to vanilla Spark SortExec")

        // Plan plain test assert
        assertPlan("AFTER", testId, plan,"""
          OmniColumnarToRow
          +- SortExecTransformer [s ASC NULLS FIRST], false, 0
          +- ProjectExecTransformer [string_view_to_omni_varchar_cast(s) AS s]
          +- FileScanTransformer orc-native [s] Batched: true, DataFilters: [], Format: ORC-NATIVE, Location: InMemoryFileIndex(1 paths)[file:path], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<s:string> NativeFilters: []
        """.stripMargin)

        probeStringViewToOmniVarcharCast("AFTER", testId, plan, 1)

        // Query execution's result check
        val rows = df.collect().map(_.getString(0)).toSeq
        val expected = data.sorted.toSeq

        assertQueryResult("AFTER", testId, rows, expected, true);


        // Idempotent - check if casting data two times doesn't change the output
        val path2 = s"${path}/idempotent"
        val tempDF = spark.read.orc(path)
          .sortWithinPartitions("s")
        // Orc read + sort triggers SV->Varchar cast
        probeStringViewToOmniVarcharCast("IDEMPOTENT", testId, tempDF.queryExecution.executedPlan, 1)
        tempDF.repartition(1).write.orc(path2)

        // Execution of the same query, but on data that has been casted
        val df2 = spark.read.orc(path2)
          .sortWithinPartitions("s")
        val plan2 = df2.queryExecution.executedPlan
        assertPlan("IDEMPOTENT", testId, plan2, plan.treeString)

        val rows2 = df2.collect().map(_.getString(0)).toSeq
        assertQueryResult("IDEMPOTENT", testId, rows2, rows, true)
      }
    }
  }

  test("sort-fallback-2: ORC Reader reads columns as StringView, neq filter and projection supports StringView, materialization happens before sort") {
    withTempPath { dir =>
      import testImplicits._

      val testId = 2

      val path = dir.getAbsolutePath
      val data = Seq(
          ("Mark", "red"),
          ("Victor", "violet"),
          ("Very very long name", "Very very long color"),
          ("shortname", "veryverylongcoloragain"),
          ("veryverylongnameagain", "shortcol")
      )
      data.toDF("name", "color").repartition(1).write.orc(path)

      // Before
      withSQLConf(sqlConfigSVDisabled: _*) {
        val df = spark.read.orc(path)
          .filter($"name" =!= "Mark")
          .select("color")
          .sortWithinPartitions("color")
          .filter($"color" =!= "shortcol")
        val plan = df.queryExecution.executedPlan

        assertPlan("BEFORE", testId, plan,"""
          OmniColumnarToRow
          +- SortExecTransformer [color ASC NULLS FIRST], false, 0
          +- OmniFilterExecTransformer (((isnotnull(name) AND isnotnull(color)) AND NOT (name = Mark)) AND NOT (color = shortcol)), [color]
          +- FileScanTransformer orc-native [name,color] Batched: true, DataFilters: [], Format: ORC-NATIVE, Location: InMemoryFileIndex(1 paths)[file:path], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<name:string,color:string> NativeFilters: []
        """.stripMargin)

        probeStringViewToOmniVarcharCast("BEFORE", testId, plan, -1)
      }

      // After
      withSQLConf(sqlConfigSVEnabled: _*) {
        val df = spark.read.orc(path)
          .filter($"name" =!= "Mark")
          .select("color")
          .sortWithinPartitions("color")
          .filter($"color" =!= "shortcol")

        val plan = df.queryExecution.executedPlan

        assertPlan("AFTER", testId, plan,"""
          OmniColumnarToRow
          +- SortExecTransformer [color ASC NULLS FIRST], false, 0
          +- ProjectExecTransformer [string_view_to_omni_varchar_cast(color) AS color]
          +- FilterExecTransformer (((isnotnull(name) AND isnotnull(color)) AND NOT (name = string_view_literal(Mark))) AND NOT (color = string_view_literal(shortcol)))
          +- FileScanTransformer orc-native [name,color] Batched: true, DataFilters: [], Format: ORC-NATIVE, Location: InMemoryFileIndex(1 paths)[file:path], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<name:string,color:string> NativeFilters: []
        """.stripMargin)

        probeStringViewToOmniVarcharCast("AFTER", testId, plan, 2)

        // Execute the query
        val rows = df.collect().map(_.getString(0)).toSeq
        val expected = Seq("Very very long color", "veryverylongcoloragain", "violet")

        assertQueryResult("AFTER", testId, rows, expected, true);

        // Idempotent - check if casting data two times doesn't change the output
        val path2 = s"${path}/idempotent"
        val tempDF = spark.read.orc(path)
          .sortWithinPartitions("color")
        // Orc read + sort triggers SV->Varchar cast
        probeStringViewToOmniVarcharCast("IDEMPOTENT", testId, tempDF.queryExecution.executedPlan, 1)
        tempDF.repartition(1).write.orc(path2)

        // Execution of the same query, but on data that has been casted
        val df2 = spark.read.orc(path2)
          .filter($"name" =!= "Mark")
          .select("color")
          .sortWithinPartitions("color")
          .filter($"color" =!= "shortcol")
        val plan2 = df2.queryExecution.executedPlan
        assertPlan("IDEMPOTENT", testId, plan2, plan.treeString)

        val rows2 = df2.collect().map(_.getString(0)).toSeq
        assertQueryResult("IDEMPOTENT", testId, rows2, rows, true)
      }
    }
  }

  test("window-fallback-1: ORC Reader reads StringView, Filter eq supports StringView, materializes before window operator") {
    withTempPath { dir =>
      import testImplicits._

      val testId = 3

      val path = dir.getAbsolutePath
      val data = Seq(
        ("Victor", "Mathematics", 130),
        ("Max", "Mathematics", 50),
        ("Tom", "Geography", 40),
        ("Max", "Geography", 20),
        ("Michael", "Mathematics", 83),
        ("Michael", "History", 27),
        ("Victor", "History", 39),
        ("Adam", "Mathematics", 55),
        ("John", "Mathematics", 99),
        ("Dean", "Mathematics", 77)
      )
      data.toDF("name", "subject", "score").repartition(1).write.orc(path)

      // Before
      withSQLConf(sqlConfigSVDisabled: _*) {
        val df = spark.read.orc(path)
          .filter($"subject" === "Mathematics")
          .withColumn("rank", row_number().over(Window.orderBy("score")))
          .select("rank", "name")

        val plan = df.queryExecution.executedPlan

        assertPlan("BEFORE", testId, plan,"""
          OmniColumnarToRow
          +- ProjectExecTransformer [rank, name]
          +- WindowExecTransformer [row_number() windowspecdefinition(score ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank], [score ASC NULLS FIRST]
          +- InputIteratorTransformer[name, score]
          +- RowToOmniColumnar
          +- Exchange
          +- OmniColumnarToRow
          +- OmniFilterExecTransformer (isnotnull(subject) AND (subject = Mathematics)), [name, score]
          +- FileScanTransformer orc-native [name,subject,score] Batched: true, DataFilters: [], Format: ORC-NATIVE, Location: InMemoryFileIndex(1 paths)[file:path], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<name:string,subject:string,score:int> NativeFilters: []
        """.stripMargin)

        probeStringViewToOmniVarcharCast("BEFORE", testId, plan, -1)
      }

      // After
      withSQLConf(sqlConfigSVEnabled: _*) {
        val df = spark.read.orc(path)
          .filter($"subject" === "Mathematics")
          .withColumn("rank", row_number().over(Window.orderBy("score")))
          .select("rank", "name")

        val plan = df.queryExecution.executedPlan

        // After StringView falls back to Varchar, it does not return to StringView,
        // even if later operators support StringView
        assertPlan("AFTER", testId, plan,"""
          OmniColumnarToRow
          +- ProjectExecTransformer [rank, name]
          +- WindowExecTransformer [row_number() windowspecdefinition(score ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank], [score ASC NULLS FIRST]
          +- InputIteratorTransformer[name, score]
          +- RowToOmniColumnar
          +- Exchange
          +- OmniColumnarToRow
          +- ProjectExecTransformer [string_view_to_omni_varchar_cast(name) AS name, score]
          +- FilterExecTransformer (isnotnull(subject) AND (subject = string_view_literal(Mathematics)))
          +- FileScanTransformer orc-native [name,subject,score] Batched: true, DataFilters: [], Format: ORC-NATIVE, Location: InMemoryFileIndex(1 paths)[file:path], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<name:string,subject:string,score:int> NativeFilters: []
        """.stripMargin)

        probeStringViewToOmniVarcharCast("AFTER", testId, plan, 2)

        val rows = df.collect().map(el => (el.getInt(0), el.getString(1))).toSeq
        val expected = Seq((1, "Max"), (2, "Adam"), (3, "Dean"), (4, "Michael"), (5, "John"), (6, "Victor"))

        assertQueryResult("AFTER", testId, rows, expected, true);


        // Idempotent - check if casting data two times doesn't change the output
        val path2 = s"${path}/idempotent"
        val tempDF = spark.read.orc(path)
          .sortWithinPartitions("name")
        // Orc read + sort triggers SV->Varchar cast
        probeStringViewToOmniVarcharCast("IDEMPOTENT", testId, tempDF.queryExecution.executedPlan, 1)
        tempDF.repartition(1).write.orc(path2)

        // Execution of the same query, but on data that has been casted
        val df2 = spark.read.orc(path2)
          .filter($"subject" === "Mathematics")
          .withColumn("rank", row_number().over(Window.orderBy("score")))
          .select("rank", "name")
        val plan2 = df2.queryExecution.executedPlan
        assertPlan("IDEMPOTENT", testId, plan2, plan.treeString)

        val rows2 = df2.collect().map(el => (el.getInt(0), el.getString(1))).toSeq
        assertQueryResult("IDEMPOTENT", testId, rows2, rows, true)
      }
    }
  }

  test("window-fallback-2: ORC Reader reads StringView, Filter eq and projection supports StringView, materialization happens before window") {
    withTempPath { dir =>
      import testImplicits._

      val testId = 4

      val path = dir.getAbsolutePath
      val data = Seq(
        (1, "Victor", "Mathematics", 3),
        (1, "Max", "Mathematics", 5),
        (2, "Tom", "Geography", 4),
        (1, "Max", "Geography", 2),
        (1, "Michael", "Mathematics", 4),
        (1, "Michael", "History", 2),
        (1, "Victor", "History", 3),
        (2, "Adam", "Mathematics", 5)
      )
      data.toDF("school_id", "name", "subject", "grade").repartition(1).write.orc(path)

      // Before
      withSQLConf(sqlConfigSVDisabled: _*) {
        val df = spark.read.orc(path)
          .filter($"school_id" === 1)
          .select("subject", "grade")
          .withColumn("sum_grade", sum("grade").over(Window.partitionBy("subject")))
          .select("subject", "sum_grade")

        val plan = df.queryExecution.executedPlan

        assertPlan("BEFORE", testId, plan,"""
        OmniColumnarToRow
          +- ProjectExecTransformer [subject, sum_gradeL]
          +- WindowExecTransformer [sum(grade) windowspecdefinition(subject, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS sum_gradeL], [subject]
          +- InputIteratorTransformer[subject, grade]
          +- RowToOmniColumnar
          +- Exchange
          +- OmniColumnarToRow
          +- ProjectExecTransformer [subject, grade]
          +- FileScanTransformer orc-native [school_id,subject,grade] Batched: true, DataFilters: [isnotnull(school_id), (school_id = 1)], Format: ORC-NATIVE, Location: InMemoryFileIndex(1 paths)[file:path], PartitionFilters: [], PushedFilters: [IsNotNull(school_id), EqualTo(school_id,1)], ReadSchema: struct<school_id:int,subject:string,grade:int> NativeFilters: [isnotnull(school_id),(school_id = 1)]
        """.stripMargin)

        probeStringViewToOmniVarcharCast("BEFORE", testId, plan, -1)
      }

      // After
      withSQLConf(sqlConfigSVEnabled: _*) {
        val df = spark.read.orc(path)
          .filter($"school_id" === 1)
          .select("subject", "grade")
          .withColumn("sum_grade", sum("grade").over(Window.partitionBy("subject")))
          .select("subject", "sum_grade")

        val plan = df.queryExecution.executedPlan

        assertPlan("AFTER", testId, plan,"""
          OmniColumnarToRow
          +- ProjectExecTransformer [subject, sum_gradeL]
          +- WindowExecTransformer [sum(grade) windowspecdefinition(subject, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS sum_gradeL], [subject]
          +- InputIteratorTransformer[subject, grade]
          +- RowToOmniColumnar
          +- Exchange
          +- OmniColumnarToRow
          +- ProjectExecTransformer [string_view_to_omni_varchar_cast(subject) AS subject, grade]
          +- FileScanTransformer orc-native [school_id,subject,grade] Batched: true, DataFilters: [isnotnull(school_id), (school_id = 1)], Format: ORC-NATIVE, Location: InMemoryFileIndex(1 paths)[file:path], PartitionFilters: [], PushedFilters: [IsNotNull(school_id), EqualTo(school_id,1)], ReadSchema: struct<school_id:int,subject:string,grade:int> NativeFilters: [isnotnull(school_id),(school_id = 1)]
        """.stripMargin)

        probeStringViewToOmniVarcharCast("AFTER", testId, plan, 1)

        val rows = df.collect().map(el => (el.getString(0), el.getLong(1))).toSeq
        val expected = Seq(("Mathematics", 12), ("Mathematics", 12), ("Mathematics", 12), ("Geography", 2), ("History", 5), ("History", 5))
          .map(el => (el._1, el._2.toLong))

        assertQueryResult("AFTER", testId, rows, expected, false);


        // Idempotent - check if casting data two times doesn't change the output
        val path2 = s"${path}/idempotent"
        val tempDF = spark.read.orc(path)
          .sortWithinPartitions("subject")
        // Orc read + sort triggers SV->Varchar cast
        probeStringViewToOmniVarcharCast("IDEMPOTENT", testId, tempDF.queryExecution.executedPlan, 1)
        tempDF.repartition(1).write.orc(path2)

        // Execution of the same query, but on data that has been casted
        val df2 = spark.read.orc(path2)
          .filter($"school_id" === 1)
          .select("subject", "grade")
          .withColumn("sum_grade", sum("grade").over(Window.partitionBy("subject")))
          .select("subject", "sum_grade")
        val plan2 = df2.queryExecution.executedPlan
        assertPlan("IDEMPOTENT", testId, plan2, plan.treeString)

        val rows2 = df2.collect().map(el => (el.getString(0), el.getLong(1))).toSeq
        assertQueryResult("IDEMPOTENT", testId, rows2, rows, false)
      }
    }
  }

  test("isolation-1: StringView survives to C2R") {
    withTempPath { dir =>
      import testImplicits._

      val testId = 5

      val path = dir.getAbsolutePath
      val data = Seq("a", "ab", "123456789012", "1234567890123", "aaaaaaaaaaaaaaaaaaaaaaaa")
      data.toDF("s").repartition(1).write.orc(path)

      // Before
      withSQLConf(sqlConfigSVDisabled: _*) {
        val df = spark.read.orc(path)
          .filter($"s" =!= "sth")
          .filter($"s" =!= "ab")

        val plan = df.queryExecution.executedPlan

        assertPlan("BEFORE", testId, plan,"""
          OmniColumnarToRow
          +- FilterExecTransformer ((isnotnull(s) AND NOT (s = sth)) AND NOT (s = ab))
          +- FileScanTransformer orc-native [s] Batched: true, DataFilters: [], Format: ORC-NATIVE, Location: InMemoryFileIndex(1 paths)[file:path], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<s:string> NativeFilters: []
        """.stripMargin)

        probeStringViewToOmniVarcharCast("BEFORE", testId, plan, -1)
      }

      // After
      withSQLConf(sqlConfigSVEnabled: _*) {
        val df = spark.read.orc(path)
          .filter($"s" =!= "sth")
          .filter($"s" =!= "ab")

        val plan = df.queryExecution.executedPlan

        assertPlan("AFTER", testId, plan,"""
          OmniColumnarToRow
          +- FilterExecTransformer ((isnotnull(s) AND NOT (s = string_view_literal(sth))) AND NOT (s = string_view_literal(ab)))
          +- FileScanTransformer orc-native [s] Batched: true, DataFilters: [], Format: ORC-NATIVE, Location: InMemoryFileIndex(1 paths)[file:path], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<s:string> NativeFilters: []
        """.stripMargin)

        // Since SV survives to the end, cast happens in infinity
        val infinity = 1000000
        probeStringViewToOmniVarcharCast("AFTER", testId, plan, infinity)

        val rows = df.collect().map(el => el.getString(0)).toSeq
        val expected = Seq("a", "123456789012", "1234567890123", "aaaaaaaaaaaaaaaaaaaaaaaa")

        assertQueryResult("AFTER", testId, rows, expected, false);

        // Idempotent - check if casting data two times doesn't change the output
        val path2 = s"${path}/idempotent"
        val tempDF = spark.read.orc(path)
          .sortWithinPartitions("s")
        // Orc read + sort triggers SV->Varchar cast
        probeStringViewToOmniVarcharCast("IDEMPOTENT", testId, tempDF.queryExecution.executedPlan, 1)
        tempDF.repartition(1).write.orc(path2)

        // Execution of the same query, but on data that has been casted
        val df2 = spark.read.orc(path2)
          .filter($"s" =!= "sth")
          .filter($"s" =!= "ab")
        val plan2 = df2.queryExecution.executedPlan
        assertPlan("IDEMPOTENT", testId, plan2, plan.treeString)

        val rows2 = df2.collect().map(el => el.getString(0)).toSeq
        assertQueryResult("IDEMPOTENT", testId, rows2, rows, false)
      }
    }
  }

  test("isolation-2: StringView survives to C2R") {
    withTempPath { dir =>
      import testImplicits._

      val testId = 6

      val path = dir.getAbsolutePath
      val data = Seq(
        ("Michael", "Football"),
        ("Jacob", "Basketball"),
        ("Mark", "Football"),
        ("Quite long name", "Football"),
        ("abcdefghijkl", "Football")
      )
      data.toDF("name", "sport").repartition(1).write.orc(path)

      // Before
      withSQLConf(sqlConfigSVDisabled: _*) {
        val df = spark.read.orc(path)
          .filter($"sport" === "Football")
          .select("name")

        val plan = df.queryExecution.executedPlan

        assertPlan("BEFORE", testId, plan,"""
          OmniColumnarToRow
          +- OmniFilterExecTransformer (isnotnull(sport) AND (sport = Football)), [name]
          +- FileScanTransformer orc-native [name,sport] Batched: true, DataFilters: [], Format: ORC-NATIVE, Location: InMemoryFileIndex(1 paths)[file:path], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<name:string,sport:string> NativeFilters: []
        """.stripMargin)

        probeStringViewToOmniVarcharCast("BEFORE", testId, plan, -1)
      }

      // After
      withSQLConf(sqlConfigSVEnabled: _*) {
        val df = spark.read.orc(path)
          .filter($"sport" === "Football")
          .select("name")

        val plan = df.queryExecution.executedPlan

        assertPlan("AFTER", testId, plan,"""
          OmniColumnarToRow
          +- OmniFilterExecTransformer (isnotnull(sport) AND (sport = string_view_literal(Football))), [name]
          +- FileScanTransformer orc-native [name,sport] Batched: true, DataFilters: [], Format: ORC-NATIVE, Location: InMemoryFileIndex(1 paths)[file:path], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<name:string,sport:string> NativeFilters: []
        """.stripMargin)

        // Since SV survives to the end, cast happens in infinity
        val infinity = 1000000
        probeStringViewToOmniVarcharCast("AFTER", testId, plan, infinity)

        val rows = df.collect().map(el => el.getString(0)).toSeq
        val expected = Seq("Michael", "Mark", "Quite long name", "abcdefghijkl")

        assertQueryResult("AFTER", testId, rows, expected, false);


        // Idempotent - check if casting data two times doesn't change the output
        val path2 = s"${path}/idempotent"
        val tempDF = spark.read.orc(path)
          .sortWithinPartitions("name")
        // Orc read + sort triggers SV->Varchar cast
        probeStringViewToOmniVarcharCast("IDEMPOTENT", testId, tempDF.queryExecution.executedPlan, 1)
        tempDF.repartition(1).write.orc(path2)

        // Execution of the same query, but on data that has been casted
        val df2 = spark.read.orc(path2)
          .filter($"sport" === "Football")
          .select("name")
        val plan2 = df2.queryExecution.executedPlan
        assertPlan("IDEMPOTENT", testId, plan2, plan.treeString)

        val rows2 = df2.collect().map(el => el.getString(0)).toSeq
        assertQueryResult("IDEMPOTENT", testId, rows2, rows, false)
      }
    }
  }

  test("aggr-sql: SQL query, aggregation doesn't support SV") {
    withTempPath { dir =>
      import testImplicits._

      val testId = 7

      val path = dir.getAbsolutePath
      val data = Seq(
        ("Michael", "Football", 7),
        ("Jacob", "Basketball-long-long", 8),
        ("Mark", "Football", 10),
        ("Quite long name", "Football", 15),
        ("abcdefghijkl", "Basketball-long-long", 16)
      )
      data.toDF("name", "sport", "score").repartition(1).write.orc(path)

      // Before
      withSQLConf(sqlConfigSVDisabled: _*) {
        spark.read.orc(path).createOrReplaceTempView("tbl")

        val df = spark.sql("""
          SELECT sport, SUM(score) AS team_score
          FROM tbl
          WHERE name != 'Michael'
          GROUP BY sport
        """)

        val plan = df.queryExecution.executedPlan

        assertPlan("BEFORE", testId, plan,"""
          OmniColumnarToRow
          +- OmniHashAggregateTransformer(keys=[sport], functions=[sum(score)], isStreamingAgg=false, output=[sport, team_scoreL])
          +- InputIteratorTransformer[sport, sumL]
          +- RowToOmniColumnar
          +- Exchange
          +- OmniColumnarToRow
          +- OmniAdaptiveHashAggregateTransformer(keys=[sport], functions=[partial_sum(score)], isStreamingAgg=false, output=[sport, sumL])
          +- OmniFilterExecTransformer (isnotnull(name) AND NOT (name = Michael)), [sport, score]
          +- FileScanTransformer orc-native [name,sport,score] Batched: true, DataFilters: [], Format: ORC-NATIVE, Location: InMemoryFileIndex(1 paths)[file:path], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<name:string,sport:string,score:int> NativeFilters: []
        """.stripMargin)

        probeStringViewToOmniVarcharCast("BEFORE", testId, plan, -1)
      }

      // After
      withSQLConf(sqlConfigSVEnabled: _*) {
        spark.read.orc(path).createOrReplaceTempView("tbl")

        val df = spark.sql("""
          SELECT sport, SUM(score) AS team_score
          FROM tbl
          WHERE name != 'Michael'
          GROUP BY sport
        """)

        val plan = df.queryExecution.executedPlan

        assertPlan("AFTER", testId, plan,"""
          OmniColumnarToRow
          +- OmniHashAggregateTransformer(keys=[sport], functions=[sum(score)], isStreamingAgg=false, output=[sport, team_scoreL])
          +- InputIteratorTransformer[sport, sumL]
          +- RowToOmniColumnar
          +- Exchange
          +- OmniColumnarToRow
          +- OmniAdaptiveHashAggregateTransformer(keys=[sport], functions=[partial_sum(score)], isStreamingAgg=false, output=[sport, sumL])
          +- ProjectExecTransformer [string_view_to_omni_varchar_cast(sport) AS sport, score]
          +- FilterExecTransformer (isnotnull(name) AND NOT (name = string_view_literal(Michael)))
          +- FileScanTransformer orc-native [name,sport,score] Batched: true, DataFilters: [], Format: ORC-NATIVE, Location: InMemoryFileIndex(1 paths)[file:path], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<name:string,sport:string,score:int> NativeFilters: []
        """.stripMargin)

        probeStringViewToOmniVarcharCast("AFTER", testId, plan, 2)

        val rows = df.collect().map(el => (el.getString(0), el.getLong(1))).toSeq
        val expected = Seq(("Basketball-long-long", 24), ("Football", 25)).map(el => (el._1, el._2.toLong))

        assertQueryResult("AFTER", testId, rows, expected, false)

        // Idempotent - check if casting data two times doesn't change the output
        val path2 = s"${path}/idempotent"
        val tempDF = spark.read.orc(path)
          .sortWithinPartitions("name")
        // Orc read + sort triggers SV->Varchar cast
        probeStringViewToOmniVarcharCast("IDEMPOTENT", testId, tempDF.queryExecution.executedPlan, 1)
        tempDF.repartition(1).write.orc(path2)

        // Execution of the same query, but on data that has been casted
        spark.read.orc(path2).createOrReplaceTempView("tbl2")
        val df2 = spark.sql("""
          SELECT sport, SUM(score) AS team_score
          FROM tbl2
          WHERE name != 'Michael'
          GROUP BY sport
        """)
        val plan2 = df2.queryExecution.executedPlan
        assertPlan("IDEMPOTENT", testId, plan2, plan.treeString)

        val rows2 = df2.collect().map(el => (el.getString(0), el.getLong(1))).toSeq
        assertQueryResult("IDEMPOTENT", testId, rows2, rows, false)
      }
    }
  }
}
