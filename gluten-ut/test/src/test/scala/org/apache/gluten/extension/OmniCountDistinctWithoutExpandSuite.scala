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
package org.apache.gluten.extension

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.WholeStageTransformerSuite
import org.apache.gluten.expression.aggregate.OmniCountDistinct
import org.apache.gluten.utils.{BackendTestUtils, SystemParameters}

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.adaptive.DisableAdaptiveExecutionSuite

/**
 * Tests for [[OmniCountDistinctWithoutExpand]] rule: count(distinct x) is rewritten to the
 * OmniCountDistinct UDAF so the physical plan no longer contains Expand (which replicates each
 * row N times for N distinct aggregates and causes OOM with dozens of count(distinct)).
 *
 * Rule activation requires spark.gluten.sql.countDistinctWithoutExpand=true (default false).
 */
class OmniCountDistinctWithoutExpandSuite extends WholeStageTransformerSuite
  with DisableAdaptiveExecutionSuite {

  override protected val resourcePath: String = null
  override protected val fileFormat: String = null

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.shuffle.partitions", "1")
    if (BackendTestUtils.isCHBackendLoaded()) {
      conf
        .set("spark.gluten.sql.enable.native.validation", "false")
        .set(GlutenConfig.GLUTEN_LIB_PATH, SystemParameters.getClickHouseLibPath)
    } else if (BackendTestUtils.isOmniBackendLoaded()) {
      conf
        .set(GlutenConfig.GLUTEN_LIB_PATH, SystemParameters.getOmniLibPath)
    }
    conf
  }

  test("count(distinct) rewritten to OmniCountDistinct when enabled") {
    withSQLConf(GlutenConfig.ENABLE_COUNT_DISTINCT_WITHOUT_EXPAND.key -> "true") {
      val df = sql("SELECT count(DISTINCT id) FROM range(100)")
      // Logical level: rewritten to the OmniCountDistinct UDAF (no longer distinct).
      val analyzedString = df.queryExecution.analyzed.toString
      assert(analyzedString.contains("omni_count_distinct"))
      assert(!analyzedString.contains("distinct"))

      // Physical level: no Expand operator.
      val executedPlan = df.queryExecution.executedPlan.toString
      assert(!executedPlan.contains("Expand"))

      // Result correctness: range(100) has 100 distinct ids.
      checkAnswer(df, Seq(org.apache.spark.sql.Row(100L)))
    }
  }

  test("multiple count(distinct) columns dedup correctly") {
    withTempView("v") {
      spark
        .range(50)
        .selectExpr("id % 10 AS g", "id % 7 AS a", "id % 3 AS b")
        .createOrReplaceTempView("v")
      withSQLConf(GlutenConfig.ENABLE_COUNT_DISTINCT_WITHOUT_EXPAND.key -> "true") {
        val df = sql("SELECT g, count(DISTINCT a), count(DISTINCT b) FROM v GROUP BY g ORDER BY g")
        // Each group g covers 10 consecutive ids: id%7 has 7 distinct residues,
        // id%3 has 3 distinct residues.
        val expected = (0 until 10).map(g => org.apache.spark.sql.Row(g, 7L, 3L))
        checkAnswer(df, expected)
        // Plan level: rewritten UDAF present, no Expand.
        val analyzedString = df.queryExecution.analyzed.toString
        assert(analyzedString.contains("omni_count_distinct"))
        assert(!df.queryExecution.executedPlan.toString.contains("Expand"))
      }
    }
  }

  test("rule is a no-op when disabled") {
    withTempView("v") {
      spark.range(10).selectExpr("id", "id % 3 AS g").createOrReplaceTempView("v")
      withSQLConf(GlutenConfig.ENABLE_COUNT_DISTINCT_WITHOUT_EXPAND.key -> "false") {
        val df = sql("SELECT count(DISTINCT id) FROM v")
        // Not rewritten: stays as plain count with isDistinct, planned via Expand.
        val analyzedString = df.queryExecution.analyzed.toString
        assert(!analyzedString.contains("omni_count_distinct"))
        checkAnswer(df, Seq(org.apache.spark.sql.Row(10L)))
      }
    }
  }

  test("unsupported types keep the Expand path") {
    // Array input is not in the supported type list of the rule; count(distinct array_col)
    // must keep the vanilla Expand path.
    withTempView("v") {
      spark
        .range(20)
        .selectExpr("id", "array(id % 2, id % 3) AS arr")
        .createOrReplaceTempView("v")
      withSQLConf(GlutenConfig.ENABLE_COUNT_DISTINCT_WITHOUT_EXPAND.key -> "true") {
        val df = sql("SELECT count(DISTINCT arr) FROM v")
        val analyzedString = df.queryExecution.analyzed.toString
        assert(!analyzedString.contains("omni_count_distinct"))
        // Vanilla Spark plans distinct aggregates via Expand.
        assert(df.queryExecution.executedPlan.toString.contains("Expand"))
        checkAnswer(df, Seq(org.apache.spark.sql.Row(6L)))
      }
    }
  }

  test("OmniCountDistinct expression properties") {
    import org.apache.spark.sql.catalyst.expressions.Literal
    import org.apache.spark.sql.types.LongType
    val lit = Literal(1)
    val cd = OmniCountDistinct(lit)
    assert(cd.dataType == LongType)
    assert(!cd.nullable)
    assert(cd.prettyName == "omni_count_distinct")
    assert(cd.aggBufferAttributes.length == 1)
    assert(cd.aggBufferAttributes.head.dataType.typeName.contains("array"))
    assert(cd.children.head == lit)
  }
}
