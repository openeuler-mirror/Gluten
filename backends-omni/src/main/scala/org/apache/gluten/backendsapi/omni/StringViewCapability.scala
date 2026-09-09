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

package org.apache.gluten.backendsapi.omni

import org.apache.spark.sql.catalyst.expressions.{
  Ascii,
  Attribute,
  Concat,
  Contains,
  EndsWith,
  EqualNullSafe,
  EqualTo,
  Expression,
  IsNotNull,
  IsNull,
  Length,
  Like,
  Literal,
  Not,
  StartsWith,
  StringInstr,
  StringLocate,
  StringTrim,
  StringTrimLeft,
  StringTrimRight,
  Substring
}
import org.apache.spark.sql.execution.{ExpandExec, FilterExec, ProjectExec, SortExec, SparkPlan, UnionExec}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.execution.joins.BaseJoinExec
import org.apache.spark.sql.execution.window.WindowExec

/**
 * Closed-default StringView capability whitelist for the Omni backend. Nothing is StringView-capable
 * unless listed here; every unlisted operator or expression forces its string operands to VARCHAR.
 *
 * The two whitelists MIRROR native truth and must stay in sync (see StringViewCapabilityCrossCheck):
 *   - operator hard-reject set: `SubstraitToOmniPlanValidator.cpp` rejects OMNI_STRING_VIEW (type id
 *     26) in the input-type whitelist of Sort/Window/WindowGroupLimit/TopN(Fetch)/Set/Expand rels;
 *     Filter/Project validate by compiling expressions instead. First version is deliberately more
 *     conservative than native: only Filter/Project are SV-capable; Aggregate/Join/Union/Exchange/
 *     Write/Python/unknown all require VARCHAR.
 *   - expression whitelist: functions with an OMNI_STRING_VIEW overload registered in
 *     `omni/core/src/vectorization/registration/RegisterString.cpp` (+ Comparisons.cpp).
 */
object StringViewCapability {

  /**
   * True if `plan`'s native path can consume StringView inputs (still subject to a per-expression
   * check for Filter/Project). Every other operator requires all string inputs materialized to
   * VARCHAR before it.
   */
  def operatorSupportsStringView(plan: SparkPlan): Boolean = plan match {
    case _: FilterExec | _: ProjectExec => true
    case _ => false
  }

  /**
   * True if `plan` is a real data operator whose native rel rejects StringView inputs, so every
   * still-StringView input column must be materialized to VARCHAR before it. EXPLICIT list (mirrors
   * the native validator's input-type-whitelisted rels + the conservative first-version additions).
   *
   * Operators that are NEITHER SV-capable NOR SV-rejecting — transitions/boundaries (RowToColumnar,
   * ColumnarToRow, InputIterator, WholeStageTransformer), the query-output wrapper (e.g.
   * DummyRowOutput), scans, and anything unrecognized — are TRANSPARENT: they pass StringView columns
   * through unchanged and MUST NOT trigger a downgrade. Downgrading before a transition inserts a
   * stray cast Project that gets stranded outside a whole-stage and crashes ("column support
   * mismatch"). Note these match vanilla physical operators; post-offload *ExecTransformer nodes are
   * transparent here because their SV inputs were already cast during the pre-transform pass.
   */
  def operatorRejectsStringView(plan: SparkPlan): Boolean = plan match {
    case _: SortExec | _: WindowExec | _: ExpandExec | _: UnionExec => true
    case _: BaseAggregateExec | _: BaseJoinExec | _: Exchange => true
    case _ => false
  }

  /**
   * True if `expr`'s native implementation accepts StringView string operands. A node that returns
   * false forces its (transitive) string column operands to be downgraded to VARCHAR. Passthrough
   * (Attribute) and string Literal are always compatible: SV read-only functions register both
   * {SV,SV} and {SV,VARCHAR} overloads, so a VARCHAR literal beside an SV column resolves.
   */
  def expressionSupportsStringView(expr: Expression): Boolean = expr match {
    case _: Attribute | _: Literal => true
    // Boolean combinators have no string operands, so they never force a downgrade; keeping them
    // here lets a StringView column flow through IsNull/StartsWith/… under an And/Or/Not.
    case _: Not => true
    case _: IsNull | _: IsNotNull => true
    // EQ/NEQ (=, <=>, =!=) run on StringView. Native has {SV,SV} comparisons; StringViewFallbackRule
    // threads the comparison literal as a StringView literal (variation 21) so both operands are SV.
    case _: EqualTo | _: EqualNullSafe => true
    case _: Length | _: Ascii => true
    // Read-only string funcs register both {SV,SV} and {SV,VARCHAR} overloads, so a VARCHAR literal
    // beside an SV column resolves natively.
    case _: StartsWith | _: EndsWith | _: Contains => true
    case _: Like => true
    case _: StringInstr | _: StringLocate => true
    case _: Concat => true
    case _: Substring => true
    case _: StringTrim | _: StringTrimLeft | _: StringTrimRight => true
    case _ => false
  }

  /**
   * True if `expr`'s native implementation returns a physical StringView column (not VARCHAR). Used by
   * [[org.apache.spark.sql.catalyst.optimizer.StringViewFallbackRule]] to propagate SV metadata on
   * Project outputs and to select the OMNI_STRING_VIEW return-type overload at compile time.
   */
  def expressionProducesStringView(expr: Expression): Boolean = expr match {
    case _: Concat => true
    case _: Substring => true
    // Native SV-out trim overloads are 1-arg only (whitespace trim); custom trim chars still VARCHAR-out.
    case trim: StringTrim if trim.trimStr.isEmpty => true
    case trim: StringTrimLeft if trim.trimStr.isEmpty => true
    case trim: StringTrimRight if trim.trimStr.isEmpty => true
    case _ => false
  }
}
