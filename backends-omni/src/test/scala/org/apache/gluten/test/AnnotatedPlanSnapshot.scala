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

package org.apache.gluten.test

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, StringViewToOmniVarcharCast}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StringType

import org.scalatest.Assertions

/**
 * Golden "annotated plan" snapshots for the StringView cast-fallback tests. Renders a SparkPlan as an
 * indented tree where every string column is tagged with its physical Omni type (SV vs VARCHAR) and
 * every fallback cast is marked. That physical type is exactly the signal `SparkPlan.treeString` does
 * NOT show, so a single snapshot makes "which column stays StringView / where the SV->VARCHAR cast
 * lands" visible at a glance -- far more readable than a scatter of `nodes.exists(...)` assertions.
 *
 * There is no `insta` in Scala. Two knobs replace its auto-update:
 *   - run with `-Dsv.snapshot.update=true` to PRINT the actual normalized snapshot for every
 *     assertion, then paste it into the test's expected block;
 *   - seed a not-yet-captured expected with [[SNAPSHOT_PENDING]]: the assertion prints the actual and
 *     passes (bootstrap), so a test that can only run in the native env can be locked there later.
 */
trait AnnotatedPlanSnapshot extends Assertions {

  /** Sentinel expected value: print the actual snapshot and pass, instead of asserting. */
  protected val SNAPSHOT_PENDING: String = "<pending: capture with -Dsv.snapshot.update=true>"

  /** StringView->Omni VARCHAR cast column names a node materializes (empty if none). */
  protected def castColumns(plan: SparkPlan): Seq[String] =
    plan.expressions.collect {
      case a: Alias if a.child.isInstanceOf[StringViewToOmniVarcharCast] => a.name
    }

  protected def className(plan: SparkPlan): String = plan.getClass.getSimpleName

  private def physType(a: Attribute): String =
    if (a.dataType != StringType) a.dataType.simpleString
    else if (StringViewToOmniVarcharCast.isPhysicalStringView(a)) "SV"
    else "VARCHAR"

  /** Indented plan tree; each node shows its string columns' physical type + any fallback cast. */
  protected def renderAnnotatedPlan(plan: SparkPlan): String = {
    val sb = new StringBuilder
    def go(p: SparkPlan, depth: Int): Unit = {
      val strCols = p.output.filter(_.dataType == StringType).map(a => s"${a.name}:${physType(a)}")
      val casts = castColumns(p)
      val ann = (if (strCols.nonEmpty) s"  str[${strCols.mkString(", ")}]" else "") +
        (if (casts.nonEmpty) s"  cast->VARCHAR[${casts.mkString(", ")}]" else "")
      sb.append("  " * depth).append(className(p)).append(ann).append('\n')
      p.children.foreach(go(_, depth + 1))
    }
    go(plan, 0)
    sb.toString
  }

  /** Strip volatile bits (exprId, object hash, plan/stage/codegen ids) so snapshots stay stable. */
  protected def normalizePlan(s: String): String =
    s.replaceAll("#\\d+L?", "#N")
      .replaceAll("@[0-9a-f]+", "@H")
      .replaceAll("(plan_id|stage_id|codegenStageId)=\\w+", "$1=N")
      .split("\n")
      .map(_.replaceAll("[ \\t]+$", ""))
      .mkString("\n")
      .trim

  /** Assert the rendered annotated plan matches `expected` (both normalized). */
  protected def assertPlan(plan: SparkPlan, expected: String): Unit = {
    val actual = normalizePlan(renderAnnotatedPlan(plan))
    val pending = expected.trim == SNAPSHOT_PENDING
    if (pending || System.getProperty("sv.snapshot.update") == "true") {
      // scalastyle:off println
      println(s"===ANNOTATED-PLAN-SNAPSHOT-BEGIN===\n$actual\n===ANNOTATED-PLAN-SNAPSHOT-END===")
      // scalastyle:on println
    }
    if (!pending) {
      assert(
        actual == normalizePlan(expected),
        s"annotated-plan snapshot mismatch.\n--- expected ---\n$expected\n--- actual ---\n$actual")
    }
  }
}
