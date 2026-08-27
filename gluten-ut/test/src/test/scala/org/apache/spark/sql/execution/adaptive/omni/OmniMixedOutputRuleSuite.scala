package org.apache.spark.sql.execution.adaptive.omni

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{HashAggregateExecTransformer, OmniAdaptiveHashAggregateExecTransformer, OmniHashAggregateExecTransformer}
import org.apache.gluten.utils.SystemParameters

import org.apache.spark.sql.{GlutenSQLTestsTrait, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate.Final
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.internal.SQLConf

/**
 * Verifies the mixed-output enablement rule ([[org.apache.gluten.extension.OmniMixedStoragePrepRule]]):
 * - a partial HashAgg below a shuffle below a merging (Final) HashAgg emits mixed output;
 * - the same chain feeding a ShuffledHashJoin keeps mixed output enabled and must not crash: the
 *   final HashAgg consumes the mixed input and emits columnar output to the join, so the chain is
 *   safe.
 *
 * Note: the rule's "tainted key" path (a shuffle whose partition key is shared with a join-fed
 * exchange, which disables mixed output) requires Spark exchange reuse that simple range queries
 * cannot force deterministically, so it is intentionally not covered by a unit test here.
 */
class OmniMixedOutputRuleSuite extends GlutenSQLTestsTrait {
  import testImplicits._

  override def sparkConf = {
    super.sparkConf
      .set(GlutenConfig.GLUTEN_LIB_PATH, SystemParameters.getOmniLibPath)
      .set(GlutenConfig.ENABLE_MIXED_STORAGE.key, "true")
      .set(GlutenConfig.MIXED_STORAGE_MIN_KEYS.key, "1")
      .set(GlutenConfig.COLUMNAR_OMNI_PREFER_SHUFFLED_HASH_JOIN.key, "false")
  }

  private def omniAggs(plan: SparkPlan): Seq[HashAggregateExecTransformer] =
    plan.collect { case a: OmniHashAggregateExecTransformer => a } ++
      plan.collect { case a: OmniAdaptiveHashAggregateExecTransformer => a }

  // Intermediate (partial/partialmerge) aggs produce partial states.
  private def partialAggs(plan: SparkPlan): Seq[HashAggregateExecTransformer] =
    omniAggs(plan).filterNot(_.aggregateExpressions.exists(_.mode == Final))

  test("partial -> shuffle -> final: mixed output enabled on the partial agg") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      val df = spark.range(0, 1000, 1, 4)
        .selectExpr("id % 50 as k", "id as v")
        .groupBy("k")
        .agg("v" -> "sum")

      // Materialize to force plan execution.
      checkAnswer(df.selectExpr("count(*)"), Row(50))

      val plan = df.queryExecution.executedPlan
      val partial = partialAggs(plan)
      assert(partial.nonEmpty, "expected at least one partial HashAgg in the plan")
      partial.foreach { p =>
        assert(p.mixedOutputEnabled, s"partial HashAgg should emit mixed output: $p")
      }
    }
  }

  test("partial -> shuffle -> final -> shuffled hash join: mixed output stays enabled, no crash") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_OMNI_PREFER_SHUFFLED_HASH_JOIN.key -> "true") {
      // Two aggregations joined on their group key. The final HashAggs consume the mixed input
      // and emit columnar output to the join, so mixed output on the partials is safe.
      val left = spark.range(0, 500, 1, 2)
        .selectExpr("id as k", "id as v1")
        .groupBy("k")
        .agg("v1" -> "sum")
        .as("l")
      val right = spark.range(0, 500, 1, 2)
        .selectExpr("id as k", "id as v2")
        .groupBy("k")
        .agg("v2" -> "sum")
        .as("r")
      // SHUFFLE_HASH hint 强制 ShuffledHashJoinExec，避免 AQE 下小表退化为 broadcast/SMJ。
      val df = left.hint("SHUFFLE_HASH", "l", "r").join(right, "k")

      checkAnswer(df.selectExpr("count(*)"), Row(500))
      val plan = df.queryExecution.executedPlan
      // 场景必须真实发生，否则下面的断言会空真通过。
      assert(
        plan.exists(_.isInstanceOf[ShuffledHashJoinExec]),
        s"expected ShuffledHashJoinExec in the plan, got:\n$plan")
      val partial = partialAggs(plan)
      assert(partial.nonEmpty, "expected at least one partial HashAgg in the plan")
      partial.foreach { p =>
        assert(p.mixedOutputEnabled, s"partial HashAgg should emit mixed output: $p")
      }
    }
  }
}
