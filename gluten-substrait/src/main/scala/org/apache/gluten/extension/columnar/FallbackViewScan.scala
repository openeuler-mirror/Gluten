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
package org.apache.gluten.extension.columnar

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTableType, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, View}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.hive.HiveTableScanExecTransformer

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable
import scala.util.control.NonFatal

object FallbackViewScan {
  private val VIEW_SCAN_FALLBACK_TAG =
    TreeNodeTag[Boolean]("org.apache.gluten.ViewScanFallback")

  private val FALLBACK_REASON =
    "Scan from view is configured to fallback to vanilla Spark"

  private val MAX_RECORDED_VIEW_EXPR_IDS = 100000

  private val recordedViewOutputExprIds =
    new ConcurrentHashMap[ExprId, java.lang.Boolean]()

  private def tagLogicalSubtree(plan: LogicalPlan): Unit = {
    plan.foreach(_.setTagValue(VIEW_SCAN_FALLBACK_TAG, true))
  }

  private def recordViewOutputExprIds(plan: LogicalPlan): Unit = {
    if (recordedViewOutputExprIds.size() > MAX_RECORDED_VIEW_EXPR_IDS) {
      recordedViewOutputExprIds.clear()
    }

    plan.foreach {
      logicalPlan =>
        logicalPlan.output.foreach {
          attr =>
            recordedViewOutputExprIds.put(attr.exprId, java.lang.Boolean.TRUE)
        }
    }
  }

  private def outputOverlapsRecordedView(plan: SparkPlan): Boolean = {
    plan.output.exists(attr => recordedViewOutputExprIds.containsKey(attr.exprId))
  }

  private def hasViewScanFallbackTag(plan: LogicalPlan): Boolean = {
    plan.getTagValue(VIEW_SCAN_FALLBACK_TAG).getOrElse(false)
  }

  private def hasTaggedLogicalSubtree(plan: LogicalPlan): Boolean = {
    var hasTag = false
    plan.foreach {
      logicalPlan =>
        if (hasViewScanFallbackTag(logicalPlan)) {
          hasTag = true
        }
    }
    hasTag
  }

  private def isScan(plan: SparkPlan): Boolean = plan match {
    case _: BatchScanExec => true
    case _: FileSourceScanExec => true
    case p if HiveTableScanExecTransformer.isHiveTableScan(p) => true
    case _ => false
  }

  private def collectTaggedOutputExprIds(plan: SparkPlan): Set[ExprId] = {
    val exprIds = mutable.HashSet.empty[ExprId]
    plan.foreach {
      _.logicalLink.foreach {
        _.foreach {
          logicalPlan =>
            if (hasViewScanFallbackTag(logicalPlan)) {
              logicalPlan.output.foreach(attr => exprIds += attr.exprId)
            }
        }
      }
    }
    exprIds.toSet
  }

  private def outputOverlaps(plan: SparkPlan, exprIds: Set[ExprId]): Boolean = {
    plan.output.exists(attr => exprIds.contains(attr.exprId))
  }

  private def lookupTableType(
      session: SparkSession,
      tableTypeCache: mutable.Map[TableIdentifier, Boolean],
      identifier: TableIdentifier): Boolean = {
    tableTypeCache.getOrElseUpdate(
      identifier,
      try {
        val metadata = session.sessionState.catalog.getTableMetadata(identifier)
        metadata.tableType == CatalogTableType.VIEW
      } catch {
        case NonFatal(_) => false
      })
  }

  private def isViewIdentifier(
      session: SparkSession,
      tableTypeCache: mutable.Map[TableIdentifier, Boolean],
      tableIdentifier: Option[TableIdentifier]): Boolean = {
    tableIdentifier.exists(identifier => lookupTableType(session, tableTypeCache, identifier))
  }

  private def invokeNoArg(target: Any, methodName: String): Option[Any] = {
    try {
      val method = target.getClass.getMethods
        .find(method => method.getName == methodName && method.getParameterCount == 0)
        .orElse {
          Iterator
            .iterate[Class[_]](target.getClass)(_.getSuperclass)
            .takeWhile(_ != null)
            .flatMap {
              clazz =>
                clazz.getDeclaredMethods
                  .find(method => method.getName == methodName && method.getParameterCount == 0)
            }
            .toSeq
            .headOption
        }
        .get
      method.setAccessible(true)
      Some(method.invoke(target))
    } catch {
      case NonFatal(_) => None
    }
  }

  private def tableTypeIsView(tableType: Any): Boolean = {
    tableType == CatalogTableType.VIEW
  }

  private def hiveScanTableType(plan: SparkPlan): Option[Any] = {
    invokeNoArg(plan, "relation")
      .flatMap(relation => invokeNoArg(relation, "tableMeta"))
      .flatMap(tableMeta => invokeNoArg(tableMeta, "tableType"))
  }

  private def hiveScanTableTypeIsView(plan: SparkPlan): Boolean = {
    hiveScanTableType(plan)
      .exists(tableType => tableTypeIsView(tableType))
  }

  private def logicalPlanHasViewCatalogTable(plan: LogicalPlan): Boolean = {
    var found = false
    plan.foreach {
      case relation: LogicalRelation =>
        if (relation.catalogTable.exists(_.tableType == CatalogTableType.VIEW)) {
          found = true
        }
      case relation: HiveTableRelation =>
        if (relation.tableMeta.tableType == CatalogTableType.VIEW) {
          found = true
        }
      case _ =>
    }
    found
  }

  private def isViewScan(
      plan: SparkPlan,
      session: SparkSession,
      tableTypeCache: mutable.Map[TableIdentifier, Boolean]): Boolean = plan match {
    case scan: FileSourceScanExec =>
      isViewIdentifier(session, tableTypeCache, scan.tableIdentifier) ||
        scan.logicalLink.exists(logicalPlan => logicalPlanHasViewCatalogTable(logicalPlan))
    case hiveScan if HiveTableScanExecTransformer.isHiveTableScan(hiveScan) =>
      hiveScanTableTypeIsView(hiveScan) ||
        hiveScan.logicalLink.exists(logicalPlan => logicalPlanHasViewCatalogTable(logicalPlan))
    case _ => false
  }

  case class MarkViewChildren() extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan.foreach {
        case view: View =>
          tagLogicalSubtree(view.child)
          recordViewOutputExprIds(view.child)
        case _ =>
      }
      plan
    }
  }

  case class FallbackScans(glutenConf: GlutenConfig, session: SparkSession) extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = {
      if (!glutenConf.viewScanFallbackEnabled) {
        return plan
      }

      val viewOutputExprIds = collectTaggedOutputExprIds(plan)
      val tableTypeCache = mutable.HashMap.empty[TableIdentifier, Boolean]
      plan.transformUp {
        case scan if isScan(scan) =>
          val viewScan = isViewScan(scan, session, tableTypeCache)
          val logicalLinkTagged =
            scan.logicalLink.exists(logicalPlan => hasTaggedLogicalSubtree(logicalPlan))
          val outputOverlapped = outputOverlaps(scan, viewOutputExprIds)
          val recordedOutputOverlapped = outputOverlapsRecordedView(scan)
          val shouldFallback =
            viewScan || logicalLinkTagged || outputOverlapped || recordedOutputOverlapped
          if (shouldFallback) {
            FallbackTags.add(scan, FALLBACK_REASON)
          }
          scan
      }
    }
  }
}
