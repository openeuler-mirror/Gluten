/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.execution

import com.google.protobuf.StringValue
import com.huawei.boostkit.spark.jni.{OrcPushFilterBuilder, ParquetPushFilterBuilder}
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
 * Omni Hudi scan: [[HudiScanTransformer]] file-format/validation helpers + ORC/Parquet push-filter JSON.
 */
case class OmniHudiScanExecTransformer(
    @transient override val relation: HadoopFsRelation,
    override val output: Seq[Attribute],
    override val requiredSchema: StructType,
    override val partitionFilters: Seq[Expression],
    override val optionalBucketSet: Option[BitSet],
    override val optionalNumCoalescedBuckets: Option[Int],
    override val dataFilters: Seq[Expression],
    override val tableIdentifier: Option[TableIdentifier],
    override val disableBucketedScan: Boolean)
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

  override lazy val fileFormat: ReadFileFormat =
    OmniHudiScanExecTransformer.substraitFileFormat(relation)

  override protected def doValidateInternal(): ValidationResult = {
    OmniHudiScanExecTransformer.validateHudiMetaColumns(requiredSchema) match {
      case Some(failed) => failed
      case None => super.doValidateInternal()
    }
  }

  override val nodeName: String = {
    s"OmniHudiScanExecTransformer $relation " +
      s"${tableIdentifier.map(_.unquotedString).getOrElse("")}"
  }

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

    val optimizationContent =
      s"isMergeTree=${if (this.fileFormat == ReadFileFormat.MergeTreeReadFormat) "1" else "0"}\n"
    val optimization =
      BackendsApiManager.getTransformerApiInstance.packPBMessage(
        StringValue.newBuilder.setValue(optimizationContent).build)
    val filter = pushedDownFilters.reduceOption(org.apache.spark.sql.sources.And(_, _))
    val json = this.fileFormat match {
      case ReadFileFormat.OrcReadFormat =>
        new OrcPushFilterBuilder(relation.dataSchema, requiredSchema).buildPushFilterJson(
          filter.orNull,
          session.sessionState.conf.getConf(COLUMNAR_OMNI_ENABLE_VEC_PREDICATE_FILTER),
          session.sessionState.conf.orcFilterPushDown)
      case ReadFileFormat.ParquetReadFormat =>
        val datetimeRebaseModeStr =
          session.sessionState.conf.getConf(SQLConf.PARQUET_REBASE_MODE_IN_READ)
        val int96RebaseModeStr =
          session.sessionState.conf.getConf(SQLConf.PARQUET_INT96_REBASE_MODE_IN_READ)
        new ParquetPushFilterBuilder(
          relation.dataSchema,
          requiredSchema,
          datetimeRebaseModeStr,
          int96RebaseModeStr)
          .buildPushFilterJson(
            filter.orNull,
            session.sessionState.conf.getConf(COLUMNAR_OMNI_ENABLE_VEC_PREDICATE_FILTER),
            session.sessionState.conf.parquetFilterPushDown)
      case _ => "{}"
    }
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

  override def doCanonicalize(): OmniHudiScanExecTransformer = {
    new OmniHudiScanExecTransformer(
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

object OmniHudiScanExecTransformer {

  def isHudiFileFormat(formatClassName: String): Boolean = {
    val lowerName = formatClassName.toLowerCase(java.util.Locale.ROOT)
    lowerName.contains("hudi") || lowerName.contains("hoodie")
  }

  def isHudiTableScan(scanExec: FileSourceScanExec): Boolean = {
    if (isHudiFileFormat(scanExec.relation.fileFormat.getClass.getName)) {
      return true
    }
    val locationClass =
      scanExec.relation.location.getClass.getName.toLowerCase(java.util.Locale.ROOT)
    if (locationClass.contains("hoodie")) {
      return true
    }
    scanExec.relation.options.keys.exists(_.toLowerCase(java.util.Locale.ROOT).startsWith("hoodie."))
  }

  def substraitFileFormat(relation: HadoopFsRelation): ReadFileFormat = {
    val options = relation.options.map {
      case (key, value) => key.toLowerCase(java.util.Locale.ROOT) -> value
    }
    val storage = inferStorageFormat(relation.fileFormat.getClass.getName, options)
    if (storage.equalsIgnoreCase("orc")) ReadFileFormat.OrcReadFormat
    else ReadFileFormat.ParquetReadFormat
  }

  def inferStorageFormat(
      formatClassName: String,
      options: Map[String, String]): String = {
    val className = formatClassName.toLowerCase(java.util.Locale.ROOT)
    val configuredFormat = Seq(
      options.get("hoodie.table.base.file.format"),
      options.get("hoodie.datasource.write.file.format")).flatten
      .headOption
      .map(_.toLowerCase(java.util.Locale.ROOT))

    configuredFormat match {
      case Some(format) if isSupportedStorageFormat(format) => format
      case _ if className.contains("orc") => "orc"
      case _ => "parquet"
    }
  }

  def isSupportedStorageFormat(format: String): Boolean = {
    format.equalsIgnoreCase("parquet") || format.equalsIgnoreCase("orc")
  }

  def validateHudiMetaColumns(requiredSchema: StructType): Option[ValidationResult] = {
    if (requiredSchema.fields.exists(_.name.startsWith("_hoodie"))) {
      Some(ValidationResult.failed(s"Hudi meta field not supported."))
    } else {
      None
    }
  }

  def apply(scanExec: FileSourceScanExec): OmniHudiScanExecTransformer = {
    new OmniHudiScanExecTransformer(
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
