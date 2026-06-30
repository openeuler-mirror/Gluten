/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.execution

import com.google.protobuf.StringValue
import com.huawei.boostkit.spark.jni.ParquetPushFilterBuilder
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig.COLUMNAR_OMNI_ENABLE_VEC_PREDICATE_FILTER
import org.apache.gluten.expression.{ConverterUtils, ExpressionConverter}
import org.apache.gluten.extension.PushDownFilterToOmniScan
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.`type`.ColumnTypeNode
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.substrait.rel.RelBuilder

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.BitSet

import io.substrait.proto.NamedStruct

import scala.collection.JavaConverters._

/**
 * Physical scan node for Delta data files executed by Omni's native Parquet reader.
 *
 * The transformer keeps Delta-specific scan validation, rejects unsupported deletion-vector
 * columns, and builds the Substrait file-scan relation used by the Omni backend.
 */
case class OmniDeltaScanExecTransformer(
    @transient override val relation: HadoopFsRelation,
    override val output: Seq[Attribute],
    override val requiredSchema: StructType,
    override val partitionFilters: Seq[Expression],
    override val optionalBucketSet: Option[BitSet],
    override val optionalNumCoalescedBuckets: Option[Int],
    override val dataFilters: Seq[Expression],
    override val tableIdentifier: Option[TableIdentifier],
    override val disableBucketedScan: Boolean = false)
  extends FileSourceScanExecTransformerBase(
    relation,
    output,
    requiredSchema,
    partitionFilters,
    optionalBucketSet,
    optionalNumCoalescedBuckets,
    dataFilters,
    tableIdentifier,
    disableBucketedScan)
  with Logging {

  override lazy val fileFormat: ReadFileFormat = ReadFileFormat.ParquetReadFormat

  override protected def doValidateInternal(): ValidationResult = {
    if (
      requiredSchema.fields.exists(_.name == "__delta_internal_is_row_deleted") ||
        requiredSchema.fields.exists(_.name == "__delta_internal_row_index")) {
      return ValidationResult.failed("Deletion vector is not supported in native.")
    }
    super.doValidateInternal()
  }

  override val nodeName: String =
    s"OmniDeltaScanExecTransformer ${tableIdentifier.map(_.unquotedString).getOrElse("")}"

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val output = outputAttributes()
    val typeNodes = ConverterUtils.collectAttributeTypeNodes(output)
    val nameList = ConverterUtils.collectAttributeNamesWithoutExprId(output)
    val columnTypeNodes = output.map {
      attr =>
        if (getPartitionSchema.exists(_.name.equals(attr.name))) {
          new ColumnTypeNode(NamedStruct.ColumnType.PARTITION_COL)
        } else if (SparkShimLoader.getSparkShims.isRowIndexMetadataColumn(attr.name)) {
          new ColumnTypeNode(NamedStruct.ColumnType.ROWINDEX_COL)
        } else if (isMetadataColumn(attr)) {
          new ColumnTypeNode(NamedStruct.ColumnType.METADATA_COL)
        } else {
          new ColumnTypeNode(NamedStruct.ColumnType.NORMAL_COL)
        }
    }.asJava

    val transformer = filterExprs()
      .map(ExpressionConverter.replaceAttributeReference)
      .reduceLeftOption(And)
      .map(ExpressionConverter.replaceWithExpressionTransformer(_, output))
    val filterNodes = transformer.map(_.doTransform(context.registeredFunction))
    val exprNode = filterNodes.orNull

    val optimization =
      BackendsApiManager.getTransformerApiInstance.packPBMessage(
        StringValue.newBuilder.setValue("isMergeTree=0\n").build)
    val filter = pushedDownFilters.reduceOption(org.apache.spark.sql.sources.And(_, _))

    val datetimeRebaseModeStr =
      session.sessionState.conf.getConf(SQLConf.PARQUET_REBASE_MODE_IN_READ)
    val int96RebaseModeStr =
      session.sessionState.conf.getConf(SQLConf.PARQUET_INT96_REBASE_MODE_IN_READ)
    val json = new ParquetPushFilterBuilder(
      relation.dataSchema,
      requiredSchema,
      datetimeRebaseModeStr,
      int96RebaseModeStr).buildPushFilterJson(
      filter.orNull,
      session.sessionState.conf.getConf(COLUMNAR_OMNI_ENABLE_VEC_PREDICATE_FILTER),
      session.sessionState.conf.parquetFilterPushDown)

    val extraProto = BackendsApiManager.getTransformerApiInstance.packPBMessage(
      StringValue.newBuilder.setValue(json).build)
    val extensionNode = ExtensionBuilder.makeAdvancedExtension(optimization, extraProto)

    val readNode = RelBuilder.makeReadRel(
      typeNodes,
      nameList,
      columnTypeNodes,
      exprNode,
      extensionNode,
      context,
      context.nextOperatorId(this.nodeName))
    TransformContext(output, readNode)
  }

  override def doCanonicalize(): OmniDeltaScanExecTransformer = {
    OmniDeltaScanExecTransformer(
      relation,
      output.map(QueryPlan.normalizeExpressions(_, output)),
      requiredSchema,
      QueryPlan.normalizePredicates(
        filterUnusedDynamicPruningExpressions(partitionFilters),
        output),
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      QueryPlan.normalizePredicates(dataFilters, output),
      None,
      disableBucketedScan)
  }
}

/** Factory and Delta scan detector used by the Delta pre-offload rule. */
object OmniDeltaScanExecTransformer {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  def isDeltaTableScan(scanExec: FileSourceScanExec): Boolean = {
    val formatClassName = scanExec.relation.fileFormat.getClass.getName
    // Delta also scans _delta_log JSON files through DeltaLogFileIndex. Only data-file scans
    // use DeltaParquetFileFormat and are safe to hand to Omni's Parquet reader.
    formatClassName == "org.apache.spark.sql.delta.DeltaParquetFileFormat"
  }

  def apply(scanExec: FileSourceScanExec): OmniDeltaScanExecTransformer = {
    OmniDeltaScanExecTransformer(
      scanExec.relation,
      scanExec.output,
      scanExec.requiredSchema,
      scanExec.partitionFilters,
      scanExec.optionalBucketSet,
      scanExec.optionalNumCoalescedBuckets,
      PushDownFilterToOmniScan.getPushedFilter(scanExec.dataFilters),
      scanExec.tableIdentifier,
      scanExec.disableBucketedScan)
  }
}
