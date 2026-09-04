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

import org.apache.gluten.backendsapi.omni.StringViewCapability
import org.apache.gluten.config.GlutenConfig

import org.apache.gluten.datasources.orc.OmniOrcFileFormat

import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Attribute,
  AttributeReference,
  EqualNullSafe,
  EqualTo,
  Expression,
  ExprId,
  Literal,
  NamedExpression,
  StringViewLiteral,
  StringViewToOmniVarcharCast
}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.{FileSourceScanExec, LocalTableScanExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.types.StringType

import scala.collection.mutable

// Pre-transform rule (runs on the Spark physical plan BEFORE operators are validated/offloaded).
// Prefer-StringView: data sources mark string columns physical StringView; this rule walks the plan
// BOTTOM-UP and, guided by StringViewCapability, downgrades a column to Omni VARCHAR at the first
// boundary where an operator or expression cannot consume StringView. A downgrade inserts a boundary
// ProjectExec wrapping the column in StringViewToOmniVarcharCast (marked physical VARCHAR); the
// VARCHAR physical type then propagates upward (a column never goes back to StringView).
//
//   - Non-SV-capable operator (Sort/Window/Aggregate/Join/Union/Exchange/…): every still-StringView
//     input column is cast (native validation rejects StringView in ANY input of these rels).
//   - Filter/Project (SV-capable): only columns feeding a non-SV-capable expression are cast; a
//     StartsWith/EQ/… over the column, or a bare passthrough, keeps it StringView.
//
// No unconditional root cast: a still-StringView root column is consumed by OmniColumnarToRow, which
// supports StringView directly.
case class StringViewFallbackRule() extends Rule[SparkPlan] {
  import StringViewFallbackRule._

  private val physicalTypes = mutable.HashMap.empty[ExprId, StringPhysicalType]

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!GlutenConfig.get.enableOmniStringView) {
      return plan
    }
    physicalTypes.clear()
    // Prefer-SV: mark leaf row-source string outputs as physical StringView so they flow as SV until
    // the walk below downgrades them at the first non-SV boundary. Marking the source is enough —
    // operators expose `output = child.output` and transformers read `child.output` live, so the
    // marker (and any downgrade cast above it) propagates up the tree without re-marking each column.
    val preferred = markSources(plan)
    preferred.transformUp {
      case p =>
        // Children are already processed (transformUp is bottom-up), so their output physical types
        // are in the map. Decide which of this node's input columns must be VARCHAR.
        val svCapable = StringViewCapability.operatorSupportsStringView(p)
        val toDowngrade =
          if (svCapable) {
            // Filter/Project: downgrade only columns feeding a non-SV-capable expression.
            p.expressions.flatMap(columnsToDowngrade).toSet.filter(isStringView)
          } else if (StringViewCapability.operatorRejectsStringView(p)) {
            // Sort/Window/Aggregate/Join/Union/Exchange: native rejects SV in any input.
            p.children
              .flatMap(_.output)
              .filter(a => a.dataType == StringType && isStringView(a.exprId))
              .map(_.exprId)
              .toSet
          } else {
            // Transitions/output-wrapper/scan/unknown are transparent — pass StringView through.
            // Downgrading here would insert a stray cast Project that strands at a stage boundary.
            Set.empty[ExprId]
          }
        // On an SV-capable operator, a comparison literal beside a column that stays StringView here
        // must itself be emitted as StringView (native has only {SV,SV} comparisons). Do this before
        // inserting the boundary cast so the marker survives on the operator's own expressions.
        val marked = if (svCapable) markStringViewComparisonLiterals(p, toDowngrade) else p
        val rewritten =
          if (toDowngrade.isEmpty) marked
          else marked.withNewChildren(marked.children.map(c => castColumns(c, toDowngrade)))
        updateOutput(rewritten)
        stampStringViewProducerMetadata(rewritten)
    }
  }

  // Rebuild leaf row-source operators so their StringType outputs carry the physical-StringView
  // marker. Only sources that actually feed a native StringView-producing path are marked; the ORC/
  // Parquet native readers do not emit StringView yet, so file scans are intentionally NOT marked
  // here (they stay VARCHAR until the native reader supports SV).
  private def markSources(plan: SparkPlan): SparkPlan = plan.transformUp {
    case l: LocalTableScanExec if l.output.exists(a => a.dataType == StringType) =>
      l.copy(output = l.output.map(markStringView))
    case f: FileSourceScanExec if f.output.exists(a => a.dataType == StringType) &&
      (f.relation.fileFormat.isInstanceOf[OrcFileFormat] ||
        f.relation.fileFormat.isInstanceOf[OmniOrcFileFormat]) =>
      f.copy(output = f.output.map(markStringView))
  }

  private def markStringView(a: Attribute): Attribute = a match {
    case ref: AttributeReference if ref.dataType == StringType =>
      ref.withMetadata(StringViewToOmniVarcharCast.stringViewMetadata(ref.metadata))
    case other => other
  }

  private def isStringView(exprId: ExprId): Boolean =
    physicalTypes.get(exprId).contains(StringViewPhysical)

  // For EQ/NEQ (EqualTo / EqualNullSafe; `=!=` desugars to Not(EqualTo)) where one operand is a
  // string column that stays StringView at this operator (tracked SV and not being downgraded here),
  // wrap the other operand's string literal in StringViewLiteral so it is emitted as a StringView
  // literal — native only registers {SV,SV} comparisons, so a VARCHAR literal would not resolve.
  private def markStringViewComparisonLiterals(plan: SparkPlan, downgraded: Set[ExprId]): SparkPlan = {
    def staysStringView(e: Expression): Boolean = e match {
      case a: Attribute =>
        a.dataType == StringType && isStringView(a.exprId) && !downgraded.contains(a.exprId)
      case _ => false
    }
    def markLiteral(e: Expression): Expression = e match {
      case lit: Literal if lit.dataType == StringType && lit.value != null => StringViewLiteral(lit)
      case other => other
    }
    plan.transformExpressions {
      case eq @ EqualTo(l, r) if staysStringView(l) => eq.copy(right = markLiteral(r))
      case eq @ EqualTo(l, r) if staysStringView(r) => eq.copy(left = markLiteral(l))
      case eq @ EqualNullSafe(l, r) if staysStringView(l) => eq.copy(right = markLiteral(r))
      case eq @ EqualNullSafe(l, r) if staysStringView(r) => eq.copy(left = markLiteral(l))
    }
  }

  // Columns (by ExprId) inside `e` that must be materialized to VARCHAR because they feed an
  // expression node without a native StringView implementation. A StringViewToOmniVarcharCast is a
  // barrier (its subtree is already downgraded) — this is what keeps the rule idempotent. Alias is
  // transparent. An SV-capable node recurses into its children; any other node forces every string
  // column below it (down to the next cast barrier) to VARCHAR.
  private def columnsToDowngrade(e: Expression): Set[ExprId] = e match {
    case _: StringViewToOmniVarcharCast => Set.empty // barrier: subtree already downgraded
    case _: Alias => e.children.flatMap(columnsToDowngrade).toSet // transparent rename
    case _ =>
      // A node forces VARCHAR only on its own STRING-typed operands, and only if it lacks a native
      // StringView implementation. Boolean combinators (And/Or/Not) have no string operands, so they
      // impose nothing; string columns feeding them via IsNull/EqualTo/… stay SV. Recurse everywhere
      // to catch non-SV nodes deeper in the tree.
      val here =
        if (!StringViewCapability.expressionSupportsStringView(e)) {
          e.children.filter(_.dataType == StringType).flatMap(stringColumnsUnder).toSet
        } else {
          Set.empty[ExprId]
        }
      here ++ e.children.flatMap(columnsToDowngrade)
  }

  private def stringColumnsUnder(e: Expression): Set[ExprId] = e match {
    case _: StringViewToOmniVarcharCast => Set.empty
    case a: Attribute if a.dataType == StringType => Set(a.exprId)
    case _ => e.children.flatMap(stringColumnsUnder).toSet
  }

  private def castColumns(child: SparkPlan, downgrade: Set[ExprId]): SparkPlan = {
    val castExprIds = child.output.filter(a => downgrade.contains(a.exprId)).map(_.exprId).toSet
    if (castExprIds.isEmpty) {
      child
    } else {
      val projectList = child.output.map {
        case attr if castExprIds.contains(attr.exprId) =>
          Alias(StringViewToOmniVarcharCast(attr), attr.name)(
            attr.exprId,
            attr.qualifier,
            Some(StringViewToOmniVarcharCast.varcharMetadata(attr.metadata)))
        case attr => attr
      }
      val project = ProjectExec(projectList, child)
      updateOutput(project)
      project
    }
  }

  private def updateOutput(plan: SparkPlan): Unit = {
    plan match {
      case project: ProjectExec =>
        updateProjectOutput(project.projectList, project.child.output)

      case _ =>
        plan.output.foreach {
          case attr if attr.dataType == StringType =>
            // Seed a first-seen leaf/source column from its physical-type marker (closed default:
            // unmarked -> VARCHAR). Already-tracked columns keep their propagated type.
            val seeded = physicalTypes.getOrElse(
              attr.exprId,
              if (StringViewToOmniVarcharCast.isPhysicalStringView(attr)) StringViewPhysical
              else OmniVarcharPhysical)
            physicalTypes.update(attr.exprId, seeded)
          case _ =>
        }
    }
  }

  private def updateProjectOutput(projectList: Seq[NamedExpression], childOutput: Seq[Attribute]): Unit = {
    val childTypes =
      childOutput.map(attr => attr.exprId -> physicalTypes.getOrElse(attr.exprId, OmniVarcharPhysical)).toMap
    projectList.foreach {
      case attr: Attribute if attr.dataType == StringType =>
        physicalTypes.update(attr.exprId, childTypes.getOrElse(attr.exprId, OmniVarcharPhysical))

      case alias @ Alias(child: Attribute, _) if alias.dataType == StringType =>
        physicalTypes.update(alias.exprId, childTypes.getOrElse(child.exprId, OmniVarcharPhysical))

      case alias @ Alias(StringViewToOmniVarcharCast(_: Attribute), _) =>
        physicalTypes.update(alias.exprId, OmniVarcharPhysical)

      case alias @ Alias(e, _) if alias.dataType == StringType =>
        physicalTypes.update(
          alias.exprId,
          if (StringViewCapability.expressionProducesStringView(e)) StringViewPhysical
          else OmniVarcharPhysical)

      case named if named.dataType == StringType =>
        physicalTypes.update(named.exprId, OmniVarcharPhysical)

      case _ =>
    }
  }

  // Stamp physical-StringView metadata on Project outputs whose expression produces StringView natively
  // (e.g. concat SV-out), so downstream operators and Substrait serialization see variation 21.
  private def stampStringViewProducerMetadata(plan: SparkPlan): SparkPlan = plan match {
    case project: ProjectExec =>
      val newList = project.projectList.map {
        case alias @ Alias(e, _)
            if alias.dataType == StringType &&
              StringViewCapability.expressionProducesStringView(e) &&
              physicalTypes.get(alias.exprId).contains(StringViewPhysical) &&
              !StringViewToOmniVarcharCast.isPhysicalStringView(alias.metadata) =>
          alias.copy(alias.child, alias.name)(
            alias.exprId,
            alias.qualifier,
            Some(StringViewToOmniVarcharCast.stringViewMetadata(alias.metadata)),
            alias.nonInheritableMetadataKeys)
        case other => other
      }
      if (newList eq project.projectList) project else project.copy(projectList = newList)
    case other => other
  }
}

object StringViewFallbackRule {
  private sealed trait StringPhysicalType
  private case object StringViewPhysical extends StringPhysicalType
  private case object OmniVarcharPhysical extends StringPhysicalType
}
