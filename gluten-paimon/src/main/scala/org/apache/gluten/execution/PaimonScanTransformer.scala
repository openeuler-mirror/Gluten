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
package org.apache.gluten.execution

import com.google.protobuf.StringValue
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.expression.{ConverterUtils, ExpressionConverter}
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.`type`.ColumnTypeNode
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.substrait.rel.RelBuilder
import org.apache.gluten.substrait.rel.SplitInfo

import org.apache.paimon.spark.source.GlutenPaimonSourceUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  And,
  Attribute,
  AttributeReference,
  CreateNamedStruct,
  DynamicPruningExpression,
  Expression,
  Literal
}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read.{InputPartition, Scan}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.types.StructType
import io.substrait.proto.NamedStruct

import scala.collection.JavaConverters._

class PaimonScanTransformer(
    override val output: Seq[AttributeReference],
    @transient override val scan: Scan,
    override val runtimeFilters: Seq[Expression],
    @transient override val table: Table,
    override val keyGroupedPartitioning: Option[Seq[Expression]] = None,
    override val commonPartitionValues: Option[Seq[(InternalRow, Int)]] = None)
  extends BatchScanExecTransformerBase(
    output = output,
    scan = scan,
    runtimeFilters = runtimeFilters,
    table = table,
    keyGroupedPartitioning = keyGroupedPartitioning,
    commonPartitionValues = commonPartitionValues) {

  override def productArity: Int = 6
  override def productElement(n: Int): Any = n match {
    case 0 => output
    case 1 => scan
    case 2 => runtimeFilters
    case 3 => table
    case 4 => keyGroupedPartitioning
    case 5 => commonPartitionValues
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }
  override def canEqual(that: Any): Boolean = that.isInstanceOf[PaimonScanTransformer]

  def copy(
      output: Seq[AttributeReference] = this.output,
      scan: Scan = this.scan,
      runtimeFilters: Seq[Expression] = this.runtimeFilters,
      table: Table = this.table,
      keyGroupedPartitioning: Option[Seq[Expression]] = this.keyGroupedPartitioning,
      commonPartitionValues: Option[Seq[(InternalRow, Int)]] = this.commonPartitionValues)
    : PaimonScanTransformer = {
    new PaimonScanTransformer(
      output, scan, runtimeFilters, table, keyGroupedPartitioning, commonPartitionValues)
  }

  override protected[this] def supportsBatchScan(scan: Scan): Boolean = {
    PaimonScanTransformer.supportsBatchScan(scan, outputAttributes())
  }

  override lazy val getPartitionSchema: StructType =
    GlutenPaimonSourceUtil.getReadPartitionSchema(scan)

  override def getDataSchema: StructType =
    GlutenPaimonSourceUtil.getReadDataSchema(scan)

  override def getRootPathsInternal: Seq[String] = Seq.empty

  override lazy val fileFormat: ReadFileFormat = GlutenPaimonSourceUtil.getFileFormat(scan)

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val scanOutput = outputAttributes()
    val typeNodes = ConverterUtils.collectAttributeTypeNodes(scanOutput)
    val nameList = ConverterUtils.collectAttributeNamesWithoutExprId(scanOutput)
    val partitionNames = getPartitionSchema.fieldNames.toSet
    val columnTypeNodes = scanOutput.map {
      attr =>
        if (partitionNames.contains(attr.name) ||
          PaimonScanTransformer.isPaimonFileMetadataColumn(attr.name)) {
          new ColumnTypeNode(NamedStruct.ColumnType.PARTITION_COL)
        } else if (SparkShimLoader.getSparkShims.isRowIndexMetadataColumn(attr.name) ||
          PaimonScanTransformer.isPaimonRowIndexMetadataColumn(attr.name)) {
          new ColumnTypeNode(NamedStruct.ColumnType.ROWINDEX_COL)
        } else {
          new ColumnTypeNode(NamedStruct.ColumnType.NORMAL_COL)
        }
    }.asJava

    val transformer = filterExprs()
      .map(ExpressionConverter.replaceAttributeReference)
      .reduceLeftOption(And)
      .map(ExpressionConverter.replaceWithExpressionTransformer(_, scanOutput))
    val filterNodes = transformer.map(_.doTransform(context.registeredFunction))
    val exprNode = filterNodes.orNull

    val optimizationContent =
      s"isMergeTree=${if (fileFormat == ReadFileFormat.MergeTreeReadFormat) "1" else "0"}\n"
    val optimization =
      BackendsApiManager.getTransformerApiInstance.packPBMessage(
        StringValue.newBuilder.setValue(optimizationContent).build)
    val fileReadJson = buildFileReadJson(scanOutput, partitionNames)
    val mergedJson = mergePushFilterJson(fileReadJson)
    val extraProto = BackendsApiManager.getTransformerApiInstance.packPBMessage(
      StringValue.newBuilder.setValue(mergedJson).build)
    val extensionNode = ExtensionBuilder.makeAdvancedExtension(optimization, extraProto)

    val readNode = RelBuilder.makeReadRel(
      typeNodes,
      nameList,
      columnTypeNodes,
      exprNode,
      extensionNode,
      context,
      context.nextOperatorId(nodeName))
    TransformContext(scanOutput, readNode)
  }

  private def buildFileReadJson(output: Seq[Attribute], partitionNames: Set[String]): String = {
    val includedColumns = output
      .filterNot(attr => partitionNames.contains(attr.name))
      .filterNot(attr => SparkShimLoader.getSparkShims.isRowIndexMetadataColumn(attr.name))
      .filterNot(attr => attr.name.startsWith("__paimon_"))
      .map(_.name)
      .mkString(",")
    fileFormat match {
      case ReadFileFormat.OrcReadFormat | ReadFileFormat.ParquetReadFormat =>
        val base = s"""{"includedColumns":"${escapeJson(includedColumns)}","allColumns":"${escapeJson(includedColumns)}"}"""
        if (ReadFileFormat.OrcReadFormat == fileFormat) {
          val tzId = org.apache.spark.sql.internal.SQLConf.get.sessionLocalTimeZone
          val rawOffsetMillis = java.util.TimeZone.getTimeZone(tzId).getRawOffset
          s"""${base.dropRight(1)},"timezone raw offset millis":$rawOffsetMillis}"""
        } else {
          base
        }
      case _ => "{}"
    }
  }

  private def escapeJson(value: String): String = {
    value.flatMap {
      case '"' => "\\\""
      case '\\' => "\\\\"
      case c => c.toString
    }
  }

  /** Hook for backends to inject ORC/Parquet push-filter JSON; default is empty. */
  protected def buildPushFilterJson: String = ""

  protected def mergePushFilterJson(fileReadJson: String): String = {
    val pushFilter = buildPushFilterJson
    if (pushFilter.isEmpty || pushFilter == "{}") {
      fileReadJson
    } else if (fileFormat == ReadFileFormat.OrcReadFormat) {
      val tzId = org.apache.spark.sql.internal.SQLConf.get.sessionLocalTimeZone
      val rawOffsetMillis = java.util.TimeZone.getTimeZone(tzId).getRawOffset
      s"""${pushFilter.dropRight(1)},"timezone raw offset millis":$rawOffsetMillis}"""
    } else {
      pushFilter
    }
  }

  override def getSplitInfosFromPartitions(partitions: Seq[InputPartition]): Seq[SplitInfo] = {
    val groupedPartitions = SparkShimLoader.getSparkShims.orderPartitions(
      scan,
      keyGroupedPartitioning,
      filteredPartitions,
      outputPartitioning)
    groupedPartitions.zipWithIndex.map {
      case (partitionGroup, index) =>
        GlutenPaimonSourceUtil.genSplitInfo(Seq(partitionGroup), index, getPartitionSchema, fileFormat)
    }
  }

  override def doCanonicalize(): PaimonScanTransformer = {
    this.copy(
      output = output.map(QueryPlan.normalizeExpressions(_, output)),
      runtimeFilters = QueryPlan.normalizePredicates(
        runtimeFilters.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral)),
        output))
  }

  override def nodeName: String = "OmniPaimonScanExecTransformer"
}

object PaimonScanTransformer {
  def apply(batchScan: BatchScanExec): PaimonScanTransformer = {
    apply(batchScan, batchScan.output)
  }

  private def apply(
      batchScan: BatchScanExec,
      output: Seq[AttributeReference]): PaimonScanTransformer = {
    new PaimonScanTransformer(
      output,
      batchScan.scan,
      batchScan.runtimeFilters,
      table = SparkShimLoader.getSparkShims.getBatchScanExecTable(batchScan),
      keyGroupedPartitioning = SparkShimLoader.getSparkShims.getKeyGroupedPartitioning(batchScan),
      commonPartitionValues = SparkShimLoader.getSparkShims.getCommonPartitionValues(batchScan))
  }

  def offload(batchScan: BatchScanExec): SparkPlan = {
    val nativeOutput = nativeOutputFor(batchScan)
    val scan = apply(batchScan, nativeOutput)
    if (nativeOutput.map(_.exprId) == batchScan.output.map(_.exprId)) {
      scan
    } else {
      ProjectExec(projectListFor(batchScan.output, nativeOutput), scan)
    }
  }

  def nativeOutputFor(batchScan: BatchScanExec): Seq[AttributeReference] = {
    val partitionNames = GlutenPaimonSourceUtil.getReadPartitionSchema(batchScan.scan).fieldNames.toSet
    val outputWithoutPartitionStruct = batchScan.output.filterNot(isPaimonPartitionColumn)
    val helperPartitionAttrs = paimonPartitionStruct(batchScan.output)
      .map(partitionStructHelperAttrs(_, outputWithoutPartitionStruct))
      .getOrElse(Seq.empty)
    val scanOutput = outputWithoutPartitionStruct ++ helperPartitionAttrs
    val (partitionLike, dataLike) = scanOutput.partition {
      attr =>
        partitionNames.contains(attr.name) || isPaimonFileMetadataColumn(attr.name)
    }
    val (rowIndexLike, regularLike) = dataLike.partition {
      attr =>
        SparkShimLoader.getSparkShims.isRowIndexMetadataColumn(attr.name) ||
          isPaimonRowIndexMetadataColumn(attr.name)
    }
    regularLike ++ rowIndexLike ++ partitionLike
  }

  def projectListFor(
      output: Seq[Attribute],
      nativeOutput: Seq[Attribute]): Seq[org.apache.spark.sql.catalyst.expressions.NamedExpression] = {
    output.map {
      case attr if isPaimonPartitionColumn(attr) =>
        Alias(partitionStructExpression(attr, nativeOutput), attr.name)(exprId = attr.exprId)
      case attr => attr
    }
  }

  private def paimonPartitionStruct(output: Seq[Attribute]): Option[StructType] = {
    output.collectFirst {
      case attr if isPaimonPartitionColumn(attr) => attr.dataType.asInstanceOf[StructType]
    }
  }

  private def partitionStructHelperAttrs(
      partitionStruct: StructType,
      output: Seq[Attribute]): Seq[AttributeReference] = {
    partitionStruct.fields.flatMap { field =>
      output.find(_.name == field.name) match {
        case Some(_: AttributeReference) => None
        case Some(_) => None
        case None => Some(AttributeReference(field.name, field.dataType, nullable = true)())
      }
    }
  }

  private def partitionStructExpression(
      attr: Attribute,
      nativeOutput: Seq[Attribute]): Expression = {
    val partitionStruct = attr.dataType.asInstanceOf[StructType]
    if (partitionStruct.isEmpty) {
      Literal.create(InternalRow.empty, attr.dataType)
    } else {
      val children = partitionStruct.fields.flatMap { field =>
        val value = nativeOutput.find(_.name == field.name).getOrElse {
          throw new IllegalStateException(
            "Cannot build __paimon_partition because partition field is missing: " + field.name)
        }
        Seq(Literal(field.name), value)
      }
      CreateNamedStruct(children)
    }
  }

  def supportsBatchScan(batchScan: BatchScanExec): Boolean = {
    supportsBatchScan(batchScan.scan, batchScan.output)
  }

  def supportsBatchScan(scan: Scan): Boolean = GlutenPaimonSourceUtil.supportsScan(scan)

  def supportsBatchScan(scan: Scan, output: Seq[Attribute]): Boolean = {
    GlutenPaimonSourceUtil.supportsScan(scan) && !hasUnsupportedPaimonMetadataColumns(output)
  }

  def isPaimonSupportedMetadataColumn(name: String): Boolean = {
    isPaimonFileMetadataColumn(name) || isPaimonRowIndexMetadataColumn(name)
  }

  def isPaimonFileMetadataColumn(name: String): Boolean = {
    name == "__paimon_bucket" || name == "__paimon_file_path"
  }

  def isPaimonRowIndexMetadataColumn(name: String): Boolean = {
    name == "__paimon_row_index"
  }

  def hasUnsupportedPaimonMetadataColumns(output: Seq[Attribute]): Boolean = {
    output.exists {
      attr =>
        attr.name.startsWith("__paimon_") &&
          !isPaimonSupportedMetadataColumn(attr.name) &&
          !isPaimonPartitionColumn(attr)
    }
  }

  def isPaimonPartitionColumn(attr: Attribute): Boolean = {
    attr.name == "__paimon_partition" && attr.dataType.isInstanceOf[StructType]
  }
}
