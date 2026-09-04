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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.expression.{ConverterUtils, ExpressionConverter}
import org.apache.gluten.substrait.`type`.OmniStringViewTypeNode
import org.apache.gluten.test.AnnotatedPlanSnapshot

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Ascending,
  AttributeReference,
  Concat,
  Literal,
  Lower,
  SortOrder,
  StartsWith,
  StringTrim,
  StringViewLiteral,
  StringViewToOmniVarcharCast,
  Substring
}
import org.apache.spark.sql.execution.{FilterExec, LocalTableScanExec, ProjectExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, Metadata, StringType}

class StringViewFallbackRuleSuite extends SparkFunSuite with AnnotatedPlanSnapshot {

  // The rule runs on hand-built physical plans (no execution), so these snapshots are fully
  // deterministic. Each tree shows every string column's physical type (SV vs VARCHAR) and where a
  // fallback cast lands -- the whole behavior of the rule in one readable block.

  test("insert fallback project before window string key") {
    withFallbackSmokeEnabled {
      val (s, id, child) = scanLeaf()
      val window = WindowExec(Nil, Seq(s), Seq(SortOrder(id, Ascending)), child)

      val rewritten = StringViewFallbackRule().apply(window)

      // Window rejects StringView -> its SV input `s` is materialized to VARCHAR by a boundary
      // ProjectExec inserted directly above the scan.
      assertPlan(
        rewritten,
        """|WindowExec  str[s:VARCHAR]
           |  ProjectExec  str[s:VARCHAR]  cast->VARCHAR[s]
           |    LocalTableScanExec  str[s:SV]
           |""".stripMargin)
    }
  }

  test("insert fallback project before sort string key") {
    withFallbackSmokeEnabled {
      val (s, _, child) = scanLeaf()
      val sort = SortExec(Seq(SortOrder(s, Ascending)), global = true, child)

      val rewritten = StringViewFallbackRule().apply(sort)

      assertPlan(
        rewritten,
        """|SortExec  str[s:VARCHAR]
           |  ProjectExec  str[s:VARCHAR]  cast->VARCHAR[s]
           |    LocalTableScanExec  str[s:SV]
           |""".stripMargin)
    }
  }

  test("fallback rule is idempotent") {
    withFallbackSmokeEnabled {
      val (s, _, child) = scanLeaf()
      val sort = SortExec(Seq(SortOrder(s, Ascending)), global = true, child)

      val once = StringViewFallbackRule().apply(sort)
      val twice = StringViewFallbackRule().apply(once)

      // Applying the rule a second time is a no-op: `twice` is byte-identical to `once` (one cast,
      // not two). The shared expected snapshot proves both.
      val expected =
        """|SortExec  str[s:VARCHAR]
           |  ProjectExec  str[s:VARCHAR]  cast->VARCHAR[s]
           |    LocalTableScanExec  str[s:SV]
           |""".stripMargin
      assertPlan(once, expected)
      assertPlan(twice, expected)
    }
  }

  test("no root cast: still-StringView root output is left for C2R") {
    withFallbackSmokeEnabled {
      val (_, _, child) = scanLeaf()

      // Root StringView column is consumed by OmniColumnarToRow directly; the rule must not insert a
      // boundary cast at the plan root -- `s` stays SV.
      val rewritten = StringViewFallbackRule().apply(child)

      assertPlan(
        rewritten,
        """|LocalTableScanExec  str[s:SV]
           |""".stripMargin)
    }
  }

  test("Filter over an SV-capable predicate keeps StringView (no cast)") {
    withFallbackSmokeEnabled {
      val (s, _, child) = scanLeaf()
      val filter = FilterExec(StartsWith(s, Literal("x")), child)

      val rewritten = StringViewFallbackRule().apply(filter)

      // StartsWith has a native StringView overload -> `s` stays SV, no boundary cast anywhere.
      assertPlan(
        rewritten,
        """|FilterExec  str[s:SV]
           |  LocalTableScanExec  str[s:SV]
           |""".stripMargin)
    }
  }

  test("Project passthrough keeps StringView (no cast)") {
    withFallbackSmokeEnabled {
      val (s, id, child) = scanLeaf()
      val project = ProjectExec(Seq(s, id), child)

      val rewritten = StringViewFallbackRule().apply(project)

      assertPlan(
        rewritten,
        """|ProjectExec  str[s:SV]
           |  LocalTableScanExec  str[s:SV]
           |""".stripMargin)
    }
  }

  test("Project with substr keeps StringView on output (SV-out)") {
    withFallbackSmokeEnabled {
      val (s, _, child) = scanLeaf()
      val project = ProjectExec(Seq(Alias(Substring(s, Literal(1), Literal(3)), "sub")()), child)

      val rewritten = StringViewFallbackRule().apply(project)

      assertPlan(
        rewritten,
        """|ProjectExec  str[sub:SV]
           |  LocalTableScanExec  str[s:SV]
           |""".stripMargin)
    }
  }

  test("Project with trim keeps StringView on output (SV-out)") {
    withFallbackSmokeEnabled {
      val (s, _, child) = scanLeaf()
      val project = ProjectExec(Seq(Alias(StringTrim(s), "trimmed")()), child)

      val rewritten = StringViewFallbackRule().apply(project)

      assertPlan(
        rewritten,
        """|ProjectExec  str[trimmed:SV]
           |  LocalTableScanExec  str[s:SV]
           |""".stripMargin)
    }
  }

  test("Project substr output downgrades before Sort") {
    withFallbackSmokeEnabled {
      val (s, _, child) = scanLeaf()
      val project = ProjectExec(Seq(Alias(Substring(s, Literal(1), Literal(3)), "sub")()), child)
      val sort = SortExec(Seq(SortOrder(project.output.head, Ascending)), global = true, project)

      val rewritten = StringViewFallbackRule().apply(sort)

      assertPlan(
        rewritten,
        """|SortExec  str[sub:VARCHAR]
           |  ProjectExec  str[sub:VARCHAR]  cast->VARCHAR[sub]
           |    ProjectExec  str[sub:SV]
           |      LocalTableScanExec  str[s:SV]
           |""".stripMargin)
    }
  }

  test("Project with a non-SV expression downgrades only the fed column") {
    withFallbackSmokeEnabled {
      val (s, _, child) = scanLeaf()
      // lower has no StringView overload -> the column feeding it must be materialized to VARCHAR,
      // via a boundary ProjectExec below the lower Project.
      val project = ProjectExec(Seq(Alias(Lower(s), "lc")()), child)

      val rewritten = StringViewFallbackRule().apply(project)

      assertPlan(
        rewritten,
        """|ProjectExec  str[lc:VARCHAR]
           |  ProjectExec  str[s:VARCHAR]  cast->VARCHAR[s]
           |    LocalTableScanExec  str[s:SV]
           |""".stripMargin)
    }
  }

  test("root output does not double cast an already-materialized sort key") {
    withFallbackSmokeEnabled {
      val (s, _, child) = scanLeaf()
      val sort = SortExec(Seq(SortOrder(s, Ascending)), global = true, child)

      val rewritten = StringViewFallbackRule().apply(sort)

      // Exactly one cast Project (the Sort-boundary one); the root pass does not add a second.
      assertPlan(
        rewritten,
        """|SortExec  str[s:VARCHAR]
           |  ProjectExec  str[s:VARCHAR]  cast->VARCHAR[s]
           |    LocalTableScanExec  str[s:SV]
           |""".stripMargin)
    }
  }

  test("Project with concat keeps StringView on output (SV-out)") {
    withFallbackSmokeEnabled {
      val (s, _, child) = scanLeaf()
      val project = ProjectExec(Seq(Alias(Concat(Seq(s, Literal("-"))), "joined")()), child)

      val rewritten = StringViewFallbackRule().apply(project)

      assertPlan(
        rewritten,
        """|ProjectExec  str[joined:SV]
           |  LocalTableScanExec  str[s:SV]
           |""".stripMargin)
    }
  }

  test("Project concat output downgrades before Sort") {
    withFallbackSmokeEnabled {
      val (s, _, child) = scanLeaf()
      val project = ProjectExec(Seq(Alias(Concat(Seq(s, Literal("-"))), "joined")()), child)
      val sort = SortExec(Seq(SortOrder(project.output.head, Ascending)), global = true, project)

      val rewritten = StringViewFallbackRule().apply(sort)

      assertPlan(
        rewritten,
        """|SortExec  str[joined:VARCHAR]
           |  ProjectExec  str[joined:VARCHAR]  cast->VARCHAR[joined]
           |    ProjectExec  str[joined:SV]
           |      LocalTableScanExec  str[s:SV]
           |""".stripMargin)
    }
  }

  test("fallback cast downgrades to standard varchar (variation 0)") {
    val s = AttributeReference("s", StringType)()

    val transformer = ExpressionConverter.replaceWithExpressionTransformer(
      StringViewToOmniVarcharCast(s),
      Seq(s))
    val proto = transformer
      .doTransform(new java.util.HashMap[String, java.lang.Long]())
      .toProtobuf

    assert(proto.hasCast)
    assert(proto.getCast.getType.hasString)
    // Downgrade target is plain VARCHAR, i.e. variation 0 — never the StringView variation.
    assert(proto.getCast.getType.getString.getTypeVariationReference == 0)
  }

  test("string_view_literal emits substrait literal with type_variation_reference 21") {
    val transformer = ExpressionConverter.replaceWithExpressionTransformer(
      StringViewLiteral(Literal("zzz")),
      Seq.empty)
    val proto = transformer
      .doTransform(new java.util.HashMap[String, java.lang.Long]())
      .toProtobuf

    assert(proto.hasLiteral, s"expected a Literal node, got: $proto")
    assert(proto.getLiteral.hasString, s"expected a string literal, got: ${proto.getLiteral}")
    assert(proto.getLiteral.getString == "zzz")
    assert(
      proto.getLiteral.getTypeVariationReference ==
        OmniStringViewTypeNode.OMNI_STRING_VIEW_TYPE_VARIATION_REFERENCE,
      s"expected StringView literal variation 21, got ${proto.getLiteral.getTypeVariationReference}")
  }

  test("getTypeNode emits stringview variation 21 for physical-stringview attribute, 0 otherwise") {
    val plain = AttributeReference("s", StringType)()
    val stringView = AttributeReference(
      "s",
      StringType,
      nullable = true,
      metadata = StringViewToOmniVarcharCast.stringViewMetadata(Metadata.empty))()

    // Closed default: an unmarked StringType column is standard VARCHAR (variation 0).
    assert(ConverterUtils.getTypeNode(plain).toProtobuf.getString.getTypeVariationReference == 0)
    assert(
      ConverterUtils.getTypeNode(stringView).toProtobuf.getString.getTypeVariationReference ==
        OmniStringViewTypeNode.OMNI_STRING_VIEW_TYPE_VARIATION_REFERENCE)
    assert(
      ConverterUtils
        .collectAttributeTypeNodes(Seq(stringView))
        .get(0)
        .toProtobuf
        .getString
        .getTypeVariationReference ==
        OmniStringViewTypeNode.OMNI_STRING_VIEW_TYPE_VARIATION_REFERENCE)
  }

  private def scanLeaf(): (AttributeReference, AttributeReference, SparkPlan) = {
    // Mark the string column as physical StringView so the marker-driven rule treats it as SV and
    // inserts the boundary cast. Unmarked StringType is VARCHAR (closed default) and is never cast.
    val s = AttributeReference(
      "s",
      StringType,
      nullable = true,
      metadata = StringViewToOmniVarcharCast.stringViewMetadata(Metadata.empty))()
    val id = AttributeReference("id", IntegerType)()
    (s, id, LocalTableScanExec(Seq(s, id), Nil))
  }

  private def withFallbackSmokeEnabled[T](f: => T): T = {
    val conf = new SQLConf
    conf.setConfString(GlutenConfig.ENABLE_OMNI_STRING_VIEW.key, "true")
    SQLConf.withExistingConf(conf)(f)
  }
}
