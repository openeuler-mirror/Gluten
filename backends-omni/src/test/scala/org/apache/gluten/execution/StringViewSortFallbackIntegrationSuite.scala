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
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.{Alias, StringViewToOmniVarcharCast}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Integration: with StringView on, a Sort keyed on a StringView column must NOT fall back to a
 * vanilla Spark SortExec. StringViewFallbackRule (pre-transform) materializes the SV key to Omni
 * VARCHAR (StringViewToOmniVarcharCast + physical-omni-varchar metadata), so the Sort validates
 * against Omni VARCHAR and stays offloaded as SortExecTransformer.
 *
 * Uses a LocalRelation row source (not ORC) so the SV columns come from native R2C, since the ORC
 * reader does not produce StringView yet.
 */
// Re-enabled in P2B: StringViewFallbackRule.markSources marks LocalTableScanExec string outputs as
// physical StringView (prefer-SV), so SV lights up end-to-end from the in-memory row source through
// R2C. Sort (not SV-capable) gets a boundary cast to VARCHAR; the SV-on path must stay byte-identical
// to the SV-off VARCHAR baseline.
class StringViewSortFallbackIntegrationSuite extends SharedSparkSession {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.gluten.sql.columnar.libpath", "/opt/Adaptor/lib/libspark_columnar_plugin.so")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "256m")
      .set("spark.sql.session.timeZone", "UTC")
      // AQE off so the columnar transform applies to the whole plan up-front and executedPlan is
      // inspectable without triggering native execution.
      .set("spark.sql.adaptive.enabled", "false")
  }

  private def collectAll(plan: SparkPlan): Seq[SparkPlan] = plan +: plan.children.flatMap(collectAll)

  // Dumps the substrait Plan (protobuf-as-JSON) for every WholeStageTransformer in the plan — the
  // wire format Gluten hands to native OmniRuntime. This is where the SV->Omni VARCHAR cast shows up
  // as `string` with `typeVariationReference: 21` (vs 0 for a still-StringView column). Complements
  // the Spark executedPlan treeString, which is the higher-level operator view.
  private def printSubstraitPlans(plan: SparkPlan, label: String): Unit = {
    val wsts = collectAll(plan).collect { case w: WholeStageTransformer => w }
    // scalastyle:off println
    wsts.zipWithIndex.foreach {
      case (w, i) =>
        println(s"===SUBSTRAIT-PLAN[$label#$i]-BEGIN===")
        println(w.substraitPlanJson)
        println(s"===SUBSTRAIT-PLAN[$label#$i]-END===")
    }
    // scalastyle:on println
  }

  test("StringView sort key materializes to VARCHAR and the sort offloads (no fallback)") {
    withSQLConf(
      "spark.gluten.sql.columnar.backend.omni.nativeRowToColumnar.enabled" -> "true",
      "spark.gluten.sql.columnar.backend.omni.preferVectorizationExpression" -> "true",
      "spark.omni.stringview.enabled" -> "true",
      "spark.omni.stringview.fallback.enabled" -> "true",
      "spark.sql.optimizer.excludedRules" ->
        "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"
    ) {
      import testImplicits._
      // sortWithinPartitions => a local Sort with NO range-partition Exchange, isolating the
      // Sort-key fallback path (a global orderBy would add a shuffle, which is a separate concern).
      val df = Seq("charlie", "alice", "bob", "dave").toDF("s").sortWithinPartitions("s")
      val plan = df.queryExecution.executedPlan
      val nodes = collectAll(plan)

      val hasFallbackCast = nodes.exists(
        _.expressions.exists(e => e.exists(_.isInstanceOf[StringViewToOmniVarcharCast])))
      val hasSortTransformer = nodes.exists(_.isInstanceOf[SortExecTransformer])
      val hasSparkSort = nodes.exists(_.getClass.getSimpleName == "SortExec")

      // scalastyle:off println
      println("===EXECUTED-PLAN-BEGIN===")
      println(plan.treeString)
      println("===EXECUTED-PLAN-END===")
      // scalastyle:on println
      printSubstraitPlans(plan, "simple")

      assert(
        hasFallbackCast,
        s"expected a StringViewToOmniVarcharCast materialize (so the key was StringView).\n$plan")
      assert(hasSortTransformer, s"Sort should offload to SortExecTransformer.\n$plan")
      assert(!hasSparkSort, s"Sort must NOT fall back to vanilla Spark SortExec.\n$plan")

      // Full e2e: execute the query. This drives the native SV->Omni VARCHAR cast kernel + native Sort.
      val rows = df.collect().map(_.getString(0)).toSeq
      // scalastyle:off println
      println(s"===RESULT-ROWS=== ${rows.mkString("[", ", ", "]")}")
      // scalastyle:on println
      // No baseline diff requested — just confirm it ran end-to-end.
      assert(rows.length == 4, s"expected 4 rows, got $rows")
    }
  }

  // Returns the StringView->Omni VARCHAR cast column names a node materializes (empty if none).
  private def castColumns(plan: SparkPlan): Seq[String] =
    plan.expressions.collect {
      case a: Alias if a.child.isInstanceOf[StringViewToOmniVarcharCast] => a.name
    }

  private def className(plan: SparkPlan): String = plan.getClass.getSimpleName

  private def assertNoFallbackCastProject(nodes: Seq[SparkPlan], plan: SparkPlan): Unit = {
    assert(
      !nodes.exists(node => className(node) == "ProjectExec" && castColumns(node).nonEmpty),
      s"StringView fallback casts must offload to ProjectExecTransformer, not remain Spark ProjectExec.\n$plan")
  }

  private def assertCastProjectsOffloaded(castNodes: Seq[SparkPlan], plan: SparkPlan): Unit = {
    assert(
      castNodes.forall(_.isInstanceOf[ProjectExecTransformer]),
      s"all StringView fallback cast projects must offload to ProjectExecTransformer; got " +
        s"${castNodes.map(className).mkString(", ")}.\n$plan")
  }


  test("mixed plan: Filter consumes StringView, its SV column is projected away, only the Sort key is cast") {
    withSQLConf(
      "spark.gluten.sql.columnar.backend.omni.nativeRowToColumnar.enabled" -> "true",
      "spark.gluten.sql.columnar.backend.omni.preferVectorizationExpression" -> "true",
      "spark.omni.stringview.enabled" -> "true",
      "spark.omni.stringview.fallback.enabled" -> "true",
      "spark.sql.optimizer.excludedRules" ->
        "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"
    ) {
      import testImplicits._
      // s1 is consumed by a native Filter AS StringView, then PROJECTED AWAY before the Sort, so no
      // uncast StringView column ever reaches OmniColumnarToRow (this is what sidesteps known-issue #38).
      // s2 is the Sort key: StringView-unsupported => materialized to Omni VARCHAR right before the Sort.
      // Net: the SV-supporting operator (Filter) keeps StringView, the unsupported operator (Sort) gets
      // a cast-to-Omni-VARCHAR at its boundary -- proven end to end, no crash.
      val input = Seq(("alice", "x3"), ("bob", "x1"), ("carol", "x2"), ("dave", "x4"))
      val df = input
        .toDF("s1", "s2")
        .filter($"s1" =!= "zzz") // s1 consumed as StringView by the native Filter (keeps all rows)
        .select("s2") // s1 dropped here; only the Sort key s2 continues upward
        .sortWithinPartitions("s2")
      val plan = df.queryExecution.executedPlan
      val nodes = collectAll(plan)

      // scalastyle:off println
      println("===MIXED-PLAN-BEGIN===")
      println(plan.treeString)
      println("===MIXED-PLAN-END===")
      // scalastyle:on println
      printSubstraitPlans(plan, "project-away")

      val filterNode = nodes.find(_.getClass.getSimpleName == "FilterExecTransformer")
      assert(
        filterNode.isDefined,
        s"Filter on a StringView column must offload to FilterExecTransformer (consume SV natively).\n$plan")

      // Exactly one materialize cast, it touches only the Sort key s2, and it sits ABOVE the Filter.
      val castNodes = nodes.filter(castColumns(_).nonEmpty)
      assert(castNodes.size == 1, s"expected exactly one materialize cast at the Sort boundary.\n$plan")
      assert(
        castColumns(castNodes.head) == Seq("s2"),
        s"only the Sort key s2 should be materialized to Omni VARCHAR; got ${castColumns(castNodes.head)}.\n$plan")
      assert(
        collectAll(castNodes.head).contains(filterNode.get),
        s"the materialize cast must sit ABOVE the Filter (between Filter and Sort).\n$plan")

      assert(nodes.exists(_.isInstanceOf[SortExecTransformer]), s"Sort should offload.\n$plan")
      assert(
        !nodes.exists(_.getClass.getSimpleName == "SortExec"),
        s"Sort must NOT fall back to vanilla Spark SortExec.\n$plan")

      // Full e2e: execute. Drives native SV Filter + native SV->Omni VARCHAR cast + native Sort; C2R
      // then reads only Omni VARCHAR (s2), so no #38 crash. Confirm every row survives.
      val rows = df.collect().map(_.getString(0)).toSet
      // scalastyle:off println
      println(s"===MIXED-RESULT=== ${rows.toSeq.sorted.mkString("[", ", ", "]")}")
      // scalastyle:on println
      assert(rows == input.map(_._2).toSet, s"row data corrupted across the SV->VARCHAR boundary: got $rows")
    }
  }

  test("mixed plan: upstream Filter consumes StringView, all Sort-input SV columns cast at the Sort boundary") {
    withSQLConf(
      "spark.gluten.sql.columnar.backend.omni.nativeRowToColumnar.enabled" -> "true",
      "spark.gluten.sql.columnar.backend.omni.preferVectorizationExpression" -> "true",
      "spark.omni.stringview.enabled" -> "true",
      "spark.omni.stringview.fallback.enabled" -> "true",
      "spark.sql.optimizer.excludedRules" ->
        "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"
    ) {
      import testImplicits._
      // Both s1 and s2 reach the Filter as StringView (the Filter consumes SV natively). s2 is the
      // Sort key; s1 is a payload column that passes THROUGH the Sort. Native SortRel validation
      // rejects OMNI_STRING_VIEW in ANY input column, so BOTH must be materialized to Omni VARCHAR at
      // the Sort boundary (not just the key s2, and not deferred to the C2R boundary) — otherwise the
      // Sort drops to vanilla Spark. One boundary Project above the Filter casts both; by the time the
      // row reaches C2R everything is already VARCHAR, so there is no separate C2R-boundary cast.
      val input = Seq(("alice", "x3"), ("bob", "x1"), ("alice", "x2"), ("carol", "x4"))
      val df = input
        .toDF("s1", "s2")
        .filter($"s1" =!= "zzz") // keeps all rows; references s1 as a StringView column
        .sortWithinPartitions("s2")
      val plan = df.queryExecution.executedPlan
      val nodes = collectAll(plan)

      // scalastyle:off println
      println("===EXECUTED-PLAN-BEGIN===")
      println(plan.treeString)
      println("===EXECUTED-PLAN-END===")
      // scalastyle:on println
      printSubstraitPlans(plan, "carry-through")

      val filterNode = nodes
        .find(_.getClass.getSimpleName == "FilterExecTransformer")
        .getOrElse(fail(
          s"Filter on a StringView column must offload to FilterExecTransformer (consume SV natively), " +
            s"not fall back.\n$plan"))

      // The SV columns reach the Filter as StringView: NO cast at or below the Filter.
      assert(
        !collectAll(filterNode).exists(castColumns(_).nonEmpty),
        s"no StringViewToOmniVarcharCast may appear at or below the Filter — the cast must not be pushed " +
          s"down to the source; the Filter has to see StringView.\n$plan")

      // Exactly ONE boundary cast, covering BOTH StringView input columns of the Sort (key + payload).
      val castNodes = nodes.filter(castColumns(_).nonEmpty)
      assert(
        castNodes.size == 1,
        s"expected exactly one Sort-boundary cast covering all StringView Sort inputs.\n$plan")
      assert(
        castColumns(castNodes.head).toSet == Set("s1", "s2"),
        s"the Sort-boundary cast must materialize BOTH SV inputs s1 and s2; got ${castColumns(castNodes.head)}.\n$plan")
      assertCastProjectsOffloaded(castNodes, plan)
      assertNoFallbackCastProject(nodes, plan)

      // The cast sits ABOVE the Filter and BELOW the Sort (Filter < cast < Sort).
      val castNode = castNodes.head
      assert(
        collectAll(castNode).contains(filterNode),
        s"the boundary cast must sit ABOVE the Filter, not at the source.\n$plan")
      val sortNode = nodes
        .find(_.isInstanceOf[SortExecTransformer])
        .getOrElse(fail(s"Sort should offload to SortExecTransformer.\n$plan"))
      assert(
        collectAll(sortNode).contains(castNode),
        s"the boundary cast must sit BELOW the Sort (between Filter and Sort).\n$plan")
      assert(
        !nodes.exists(_.getClass.getSimpleName == "SortExec"),
        s"Sort must NOT fall back to vanilla Spark SortExec.\n$plan")

      // Full e2e: execute. Drives native SV Filter + native SV->Omni VARCHAR casts + native Sort; C2R
      // then reads only Omni VARCHAR, so no #38 crash. Confirm every row survives.
      val rows = df.collect().map(r => (r.getString(0), r.getString(1))).toSet
      // scalastyle:off println
      println(s"===RESULT-ROWS=== ${rows.toSeq.sortBy(_._2).mkString("[", ", ", "]")}")
      // scalastyle:on println
      assert(rows == input.toSet, s"row data corrupted across the SV->VARCHAR boundary: got $rows")
    }
  }

  test("isolation: StringView column survives Filter and is consumed by C2R directly (no cast)") {
    withSQLConf(
      "spark.gluten.sql.columnar.backend.omni.nativeRowToColumnar.enabled" -> "true",
      "spark.gluten.sql.columnar.backend.omni.preferVectorizationExpression" -> "true",
      "spark.omni.stringview.enabled" -> "true",
      "spark.omni.stringview.fallback.enabled" -> "true",
      "spark.sql.optimizer.excludedRules" ->
        "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"
    ) {
      import testImplicits._
      val input = Seq("alice", "bob", "carol", "dave")
      val df = input.toDF("s").filter($"s" =!= "zzz") // keeps all rows; s stays StringView, no Sort
      val plan = df.queryExecution.executedPlan
      // scalastyle:off println
      println("===ISO-PLAN-BEGIN===")
      println(plan.treeString)
      println("===ISO-PLAN-END===")
      // scalastyle:on println
      printSubstraitPlans(plan, "isolation")
      val nodes = collectAll(plan)
      assert(
        nodes.exists(n => className(n).contains("FilterExecTransformer")),
        s"Filter should offload and consume StringView.\n$plan")
      // The rule no longer force-casts root output: s stays StringView through the Filter and
      // OmniColumnarToRow consumes it directly, so no StringViewToOmniVarcharCast appears at all.
      assert(
        !nodes.exists(castColumns(_).nonEmpty),
        s"no StringViewToOmniVarcharCast expected; s stays SV and C2R reads it directly.\n$plan")
      val rows = df.collect().map(_.getString(0)).toSet
      // scalastyle:off println
      println(s"===ISO-RESULT=== ${rows.toSeq.sorted.mkString("[", ", ", "]")}")
      // scalastyle:on println
      assert(rows == input.toSet, s"StringView column corrupted reading back through C2R: got $rows")
    }
  }

  test("EQ on StringView via SQL + AQE runs natively (regression: transition-node stranded cast)") {
    withSQLConf(
      "spark.gluten.sql.columnar.backend.omni.nativeRowToColumnar.enabled" -> "true",
      "spark.gluten.sql.columnar.backend.omni.preferVectorizationExpression" -> "true",
      "spark.omni.stringview.enabled" -> "true",
      "spark.omni.stringview.runtimeValidation.enabled" -> "true",
      // AQE ON + spark.sql reproduces the real-submit plan shape (query-output wrapper +
      // InputIteratorTransformer boundaries) that exposed the transition-node over-downgrade bug: the
      // rule downgraded a StringView column before a transition node, inserting a cast Project that
      // stranded at a stage boundary -> "column support mismatch". Regression guard.
      "spark.sql.adaptive.enabled" -> "true",
      "spark.sql.optimizer.excludedRules" ->
        "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"
    ) {
      import testImplicits._
      Seq(("tiny", 1), ("other", 2), ("world_is_longer_value", 3)).toDF("s", "id")
        .createOrReplaceTempView("t")
      // collect() executing without a "column support mismatch" crash IS the regression assertion.
      val rows = spark.sql("SELECT s, id FROM t WHERE s = 'tiny'").collect().map(_.getString(0)).toSeq
      assert(rows == Seq("tiny"), s"got $rows")
    }
  }

  // ---- Differential correctness oracle -------------------------------------------------------
  // The `.toSet`/round-trip assertions above prove no row is lost/corrupted, but NOT that the
  // result matches what NOT using StringView would produce (order, and every byte). These build
  // the SAME DataFrame twice under identical native config, differing ONLY in StringView:
  //   - baseline: spark.omni.stringview.enabled=false  -> plain Omni VARCHAR path (no cast-fallback)
  //   - cast path: stringview.enabled=true + fallback.enabled=true -> SV + boundary cast
  // and assert the collected rows are byte-identical AND in the same order. The VARCHAR path is the
  // oracle: cast-fallback must not change results vs not using StringView. Same session/partitioning
  // => collect() order is deterministic and comparable across the two runs.
  private val nativeConf: Seq[(String, String)] = Seq(
    "spark.gluten.sql.columnar.backend.omni.nativeRowToColumnar.enabled" -> "true",
    "spark.gluten.sql.columnar.backend.omni.preferVectorizationExpression" -> "true",
    "spark.sql.optimizer.excludedRules" ->
      "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"
  )

  private def assertCastFallbackMatchesBaseline(label: String)(build: => DataFrame): Unit = {
    // withSQLConf here takes a `=> Unit` body and returns Unit, so capture the result via a var.
    // dumpLabel != None => also print the substrait plan for that run (used for the cast path).
    def rowsUnder(dumpLabel: Option[String], extra: (String, String)*): Seq[Row] = {
      var out: Seq[Row] = Seq.empty
      withSQLConf((nativeConf ++ extra): _*) {
        val df = build
        dumpLabel.foreach(l => printSubstraitPlans(df.queryExecution.executedPlan, l))
        out = df.collect().toSeq
      }
      out
    }

    val baseline = rowsUnder(None, "spark.omni.stringview.enabled" -> "false")
    val castPath = rowsUnder(
      Some(s"$label-castPath"),
      "spark.omni.stringview.enabled" -> "true",
      "spark.omni.stringview.fallback.enabled" -> "true")

    // scalastyle:off println
    println(s"===DIFF[$label] baseline=${baseline.mkString("[", ", ", "]")}")
    println(s"===DIFF[$label] castPath=${castPath.mkString("[", ", ", "]")}")
    // scalastyle:on println
    assert(
      castPath == baseline,
      s"[$label] cast-fallback result diverged from the StringView-OFF VARCHAR baseline.\n" +
        s"baseline=${baseline.mkString("[", ", ", "]")}\n" +
        s"castPath=${castPath.mkString("[", ", ", "]")}")
  }

  // Boundary payloads that exercise the SV->VARCHAR cast kernel's risky paths — especially
  // NON-INLINE strings (>12 bytes, the german-string layout whose value buffer was misread in #38),
  // plus empty / multibyte-UTF-8 / duplicate / NULL.
  private val NON_INLINE = "a_very_long_non_inline_string_beyond_twelve_bytes"
  private val UTF8_NON_INLINE = "日本語_非内联_超过十二字节的字符串"

  test("differential: carry-through (SV payload thru Sort) with boundary data matches VARCHAR baseline") {
    import testImplicits._
    // s1 is the filter/payload column (all non-null, none == "zzz" so the filter keeps every row);
    // s2 is the Sort key. Covers non-inline (both cols), empty, multibyte UTF-8, duplicate, and a
    // NULL sort key — all carried through the SV Filter, the Sort-boundary cast, and C2R.
    val input = Seq[(String, String)](
      ("alice", "x3"),
      (NON_INLINE, "short"),
      ("emptyKeyRow", ""),
      ("δοκιμή_unicode", UTF8_NON_INLINE),
      ("dupRow", "dupVal"),
      ("dupRow", "dupVal"),
      ("nullKeyRow", null)
    )
    assertCastFallbackMatchesBaseline("carry-through-boundary") {
      input.toDF("s1", "s2").filter($"s1" =!= "zzz").sortWithinPartitions("s2")
    }
  }

  test("differential: StringView survives Filter to C2R with boundary data matches VARCHAR baseline") {
    import testImplicits._
    // isNull-tolerant predicate keeps the NULL row so it too crosses the SV->VARCHAR C2R boundary.
    val input = Seq[String](
      "alice", NON_INLINE, "", UTF8_NON_INLINE, "dup", "dup", null)
    assertCastFallbackMatchesBaseline("isolation-boundary") {
      input.toDF("s").filter($"s".isNull || $"s" =!= "zzz")
    }
  }
}
