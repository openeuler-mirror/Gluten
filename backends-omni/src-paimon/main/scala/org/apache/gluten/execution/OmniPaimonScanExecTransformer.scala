/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.execution

import com.huawei.boostkit.spark.jni.{OrcPushFilterBuilder, ParquetPushFilterBuilder}
import org.apache.gluten.config.GlutenConfig.COLUMNAR_OMNI_ENABLE_VEC_PREDICATE_FILTER
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, DynamicPruningExpression, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.util.RebaseDateTime
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.FilterConverter
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}

/**
 * Omni Paimon scan: extends [[PaimonScanTransformer]] to inject ORC/Parquet push-filter JSON
 * so the native reader can do stripe/row-group level skipping.
 */
class OmniPaimonScanExecTransformer(
    override val output: Seq[AttributeReference],
    @transient override val scan: Scan,
    override val runtimeFilters: Seq[Expression],
    @transient override val table: Table,
    override val keyGroupedPartitioning: Option[Seq[Expression]] = None,
    override val commonPartitionValues: Option[Seq[(InternalRow, Int)]] = None)
  extends PaimonScanTransformer(
    output = output,
    scan = scan,
    runtimeFilters = runtimeFilters,
    table = table,
    keyGroupedPartitioning = keyGroupedPartitioning,
    commonPartitionValues = commonPartitionValues) {

  override def copy(
      output: Seq[AttributeReference] = this.output,
      scan: Scan = this.scan,
      runtimeFilters: Seq[Expression] = this.runtimeFilters,
      table: Table = this.table,
      keyGroupedPartitioning: Option[Seq[Expression]] = this.keyGroupedPartitioning,
      commonPartitionValues: Option[Seq[(InternalRow, Int)]] = this.commonPartitionValues)
    : OmniPaimonScanExecTransformer = {
    new OmniPaimonScanExecTransformer(
      output, scan, runtimeFilters, table, keyGroupedPartitioning, commonPartitionValues)
  }

  override protected def buildPushFilterJson: String = {
    val sourceFilter = FilterConverter.toSourceFilters(filterExprs()).orNull
    fileFormat match {
      case ReadFileFormat.OrcReadFormat =>
        new OrcPushFilterBuilder(getDataSchema, getDataSchema).buildPushFilterJson(
          sourceFilter,
          session.sessionState.conf.getConf(COLUMNAR_OMNI_ENABLE_VEC_PREDICATE_FILTER),
          session.sessionState.conf.orcFilterPushDown)
      case ReadFileFormat.ParquetReadFormat =>
        def toPolicy(modeStr: String): LegacyBehaviorPolicy.Value = modeStr match {
          case "LEGACY" => LegacyBehaviorPolicy.LEGACY
          case "CORRECTED" => LegacyBehaviorPolicy.CORRECTED
          case "EXCEPTION" => LegacyBehaviorPolicy.EXCEPTION
          case _ => LegacyBehaviorPolicy.LEGACY
        }
        val dtSpec = new RebaseDateTime.RebaseSpec(
          toPolicy(session.sessionState.conf.getConf(SQLConf.PARQUET_REBASE_MODE_IN_READ)), scala.None)
        val i96Spec = new RebaseDateTime.RebaseSpec(
          toPolicy(session.sessionState.conf.getConf(SQLConf.PARQUET_INT96_REBASE_MODE_IN_READ)),
          scala.None)
        new ParquetPushFilterBuilder(getDataSchema, getDataSchema, dtSpec, i96Spec)
          .buildPushFilterJson(
            sourceFilter,
            session.sessionState.conf.getConf(COLUMNAR_OMNI_ENABLE_VEC_PREDICATE_FILTER),
            session.sessionState.conf.parquetFilterPushDown)
      case _ => "{}"
    }
  }

  override def doCanonicalize(): OmniPaimonScanExecTransformer = {
    copy(
      output = output.map(QueryPlan.normalizeExpressions(_, output)),
      runtimeFilters = QueryPlan.normalizePredicates(
        runtimeFilters.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral)),
        output))
  }
}

object OmniPaimonScanExecTransformer {
  def apply(batchScan: BatchScanExec): OmniPaimonScanExecTransformer = {
    new OmniPaimonScanExecTransformer(
      batchScan.output,
      batchScan.scan,
      batchScan.runtimeFilters,
      table = SparkShimLoader.getSparkShims.getBatchScanExecTable(batchScan),
      keyGroupedPartitioning = SparkShimLoader.getSparkShims.getKeyGroupedPartitioning(batchScan),
      commonPartitionValues = SparkShimLoader.getSparkShims.getCommonPartitionValues(batchScan))
  }

  def supportsBatchScan(batchScan: BatchScanExec): Boolean =
    PaimonScanTransformer.supportsBatchScan(batchScan)

  def hasUnsupportedPaimonMetadataColumns(output: Seq[Attribute]): Boolean =
    PaimonScanTransformer.hasUnsupportedPaimonMetadataColumns(output)

  def offload(batchScan: BatchScanExec): SparkPlan = {
    val nativeOutput = PaimonScanTransformer.nativeOutputFor(batchScan)
    val scan = apply(batchScan, nativeOutput)
    if (nativeOutput.map(_.exprId) == batchScan.output.map(_.exprId)) {
      scan
    } else {
      new ProjectExec(PaimonScanTransformer.projectListFor(batchScan.output, nativeOutput), scan)
    }
  }

  private def apply(
      batchScan: BatchScanExec,
      output: Seq[AttributeReference]): OmniPaimonScanExecTransformer = {
    new OmniPaimonScanExecTransformer(
      output,
      batchScan.scan,
      batchScan.runtimeFilters,
      table = SparkShimLoader.getSparkShims.getBatchScanExecTable(batchScan),
      keyGroupedPartitioning = SparkShimLoader.getSparkShims.getKeyGroupedPartitioning(batchScan),
      commonPartitionValues = SparkShimLoader.getSparkShims.getCommonPartitionValues(batchScan))
  }
}
