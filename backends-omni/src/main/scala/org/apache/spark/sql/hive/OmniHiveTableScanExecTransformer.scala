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
package org.apache.spark.sql.hive

import com.google.protobuf.StringValue
import com.huawei.boostkit.spark.jni.{OrcPushFilterBuilder, ParquetPushFilterBuilder}
import io.substrait.proto.NamedStruct
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig.COLUMNAR_OMNI_ENABLE_VEC_PREDICATE_FILTER
import org.apache.gluten.config.GlutenConfig.COLUMNAR_OMNI_ENABLE_SCAN_FILTER_WHILE_DECODE
import org.apache.gluten.execution.{BasicScanExecTransformer, TransformContext}
import org.apache.gluten.expression.{ConverterUtils, ExpressionConverter}
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.`type`.ColumnTypeNode
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.substrait.rel.RelBuilder
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, AttributeSeq, Expression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.util.{MetadataColumnHelper, RebaseDateTime}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hive.OmniHiveTableScanExecTransformer._
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.hive.execution.{AbstractHiveTableScanExec, HiveTableScanExec}
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.Utils

import java.net.URI
import scala.collection.JavaConverters._


case class OmniHiveTableScanExecTransformer(
     requestedAttributes: Seq[Attribute],
     relation: HiveTableRelation,
     partitionPruningPred: Seq[Expression],
     prunedOutput: Seq[Attribute] = Seq.empty[Attribute])(@transient session: SparkSession)
  extends AbstractHiveTableScanExec(requestedAttributes, relation, partitionPruningPred)(session)
    with BasicScanExecTransformer {

  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance.genHiveTableScanTransformerMetrics(sparkContext)

  @transient private lazy val hiveQlTable = HiveClientImpl.toHiveTable(relation.tableMeta)

  @transient private lazy val tableDesc = new TableDesc(
    hiveQlTable.getInputFormatClass,
    hiveQlTable.getOutputFormatClass,
    hiveQlTable.getMetadata)

  override def filterExprs(): Seq[Expression] = Seq.empty

  override def getMetadataColumns(): Seq[AttributeReference] = Seq.empty

  override def outputAttributes(): Seq[Attribute] = {
    if (prunedOutput.nonEmpty) {
      prunedOutput
    } else {
      output
    }
  }

  override def getPartitions: Seq[InputPartition] = partitions

  override def getPartitionSchema: StructType = relation.tableMeta.partitionSchema

  override def getDataSchema: StructType = relation.tableMeta.dataSchema

  // TODO: get root paths from hive table.
  override def getRootPathsInternal: Seq[String] = Seq.empty

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genHiveTableScanTransformerMetricsUpdater(metrics)

  @transient private lazy val hivePartitionConverter =
    new HivePartitionConverter(session.sessionState.newHadoopConf(), session)

  @transient private lazy val partitions: Seq[InputPartition] =
    if (!relation.isPartitioned) {
      val tableLocation: URI = relation.tableMeta.storage.locationUri.getOrElse {
        throw new UnsupportedOperationException("Table path not set.")
      }
      hivePartitionConverter.createFilePartition(tableLocation)
    } else {
      hivePartitionConverter.createFilePartition(
        prunedPartitions,
        relation.partitionCols.map(_.dataType))
    }

  @transient override lazy val fileFormat: ReadFileFormat = {
    relation.tableMeta.storage.inputFormat match {
      case Some(inputFormat)
        if TEXT_INPUT_FORMAT_CLASS.isAssignableFrom(Utils.classForName(inputFormat)) =>
        relation.tableMeta.storage.serde match {
          case Some("org.openx.data.jsonserde.JsonSerDe") | Some(
          "org.apache.hive.hcatalog.data.JsonSerDe") =>
            ReadFileFormat.JsonReadFormat
          case _ => ReadFileFormat.TextReadFormat
        }
      case Some(inputFormat)
        if ORC_INPUT_FORMAT_CLASS.isAssignableFrom(Utils.classForName(inputFormat)) =>
        ReadFileFormat.OrcReadFormat
      case Some(inputFormat)
        if PARQUET_INPUT_FORMAT_CLASS.isAssignableFrom(Utils.classForName(inputFormat)) =>
        ReadFileFormat.ParquetReadFormat
      case _ => ReadFileFormat.UnknownFormat
    }
  }

  private def createDefaultTextOption(): Map[String, String] = {
    var options: Map[String, String] = Map()
    relation.tableMeta.storage.serde match {
      case Some("org.apache.hadoop.hive.serde2.OpenCSVSerde") =>
        options += ("field_delimiter" -> ",")
        options += ("quote" -> "\"")
        options += ("escape" -> "\\")
      case _ =>
        options += ("field_delimiter" -> DEFAULT_FIELD_DELIMITER.toString)
        options += ("nullValue" -> NULL_VALUE.toString)
        options += ("escape" -> "\\")
    }

    options
  }

  override def getProperties: Map[String, String] = {
    var properties: Map[String, String] = Map()
    tableDesc.getProperties
      .entrySet()
      .forEach(e => properties += (e.getKey.toString -> e.getValue.toString))

    var options: Map[String, String] = createDefaultTextOption()
    // property key string read from org.apache.hadoop.hive.serde.serdeConstants
    properties.foreach {
      case ("separatorChar", v) =>
        // If separatorChar, we should use default separatorChar
        // for org.apache.hadoop.hive.serde2.OpenCSVSerde
        // It fixed issue: https://github.com/oap-project/gluten/issues/3108
        val nv = if (v.isEmpty) "," else v
        options += ("field_delimiter" -> nv)
      case ("field.delim", v) =>
        // If field.delim is empty, we should use default field delimiter
        // for org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
        // It fixed issue: https://github.com/oap-project/gluten/issues/3108
        val nv = if (v.isEmpty) DEFAULT_FIELD_DELIMITER.toString else v
        options += ("field_delimiter" -> nv)
      case ("quoteChar", v) => options += ("quote" -> v)
      case ("quote.delim", v) => options += ("quote" -> v)
      case ("skip.header.line.count", v) => options += ("header" -> v)
      case ("escapeChar", v) => options += ("escape" -> v)
      case ("escape.delim", v) => options += ("escape" -> v)
      case ("serialization.null.format", v) => options += ("nullValue" -> v)
      case (_, _) =>
    }
    options
  }

  override def nodeName: String = s"NativeScan hive ${relation.tableMeta.qualifiedName}"

  override def doCanonicalize(): OmniHiveTableScanExecTransformer = {
    val input: AttributeSeq = relation.output
    OmniHiveTableScanExecTransformer(
      requestedAttributes.map(QueryPlan.normalizeExpressions(_, input)),
      relation.canonicalized.asInstanceOf[HiveTableRelation],
      QueryPlan.normalizePredicates(partitionPruningPred, input)
    )(sparkSession)
  }

  def attributesToStructType(attributes: Seq[Attribute]): StructType = {
    val fields: Array[StructField] = attributes.map(attr => {
      StructField(
        name = attr.name,
        dataType = attr.dataType,
        nullable = attr.nullable,
        metadata = attr.metadata
      )
    }).toArray
    StructType(fields)
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
        } else if (attr.isMetadataCol) {
          new ColumnTypeNode(NamedStruct.ColumnType.METADATA_COL)
        } else {
          new ColumnTypeNode(NamedStruct.ColumnType.NORMAL_COL)
        }
    }.asJava
    // Will put all filter expressions into an AND expression
    val transformer = filterExprs()
      .map(ExpressionConverter.replaceAttributeReference)
      .reduceLeftOption(And)
      .map(ExpressionConverter.replaceWithExpressionTransformer(_, output))
    val filterNodes = transformer.map(_.doTransform(context.registeredFunction))
    val exprNode = filterNodes.orNull

    // used by CH backend
    val optimizationContent =
      s"isMergeTree=${if (this.fileFormat == ReadFileFormat.MergeTreeReadFormat) "1" else "0"}\n"

    val optimization =
      BackendsApiManager.getTransformerApiInstance.packPBMessage(
        StringValue.newBuilder.setValue(optimizationContent).build)

    val json = this.fileFormat match {
      // ORC PushFilterJsonBuilder
      case ReadFileFormat.OrcReadFormat =>
        val orcBuilder = new OrcPushFilterBuilder(relation.tableMeta.dataSchema, attributesToStructType(requestedAttributes))
        orcBuilder.buildPushFilterJson(null,
          session.sessionState.conf.getConf(COLUMNAR_OMNI_ENABLE_VEC_PREDICATE_FILTER),
          session.sessionState.conf.orcFilterPushDown,
          session.sessionState.conf.getConf(COLUMNAR_OMNI_ENABLE_SCAN_FILTER_WHILE_DECODE)
        )

      // Parquet PushFilterJsonBuilder
      case ReadFileFormat.ParquetReadFormat =>
        val datetimeRebaseModeStr = session.sessionState.conf.getConf(SQLConf.PARQUET_REBASE_MODE_IN_READ)
        val int96RebaseModeStr = session.sessionState.conf.getConf(SQLConf.PARQUET_INT96_REBASE_MODE_IN_READ)

        def toLegacyBehaviorPolicy(modeStr: String): LegacyBehaviorPolicy.Value = {
          modeStr match {
            case "LEGACY" => LegacyBehaviorPolicy.LEGACY
            case "CORRECTED" => LegacyBehaviorPolicy.CORRECTED
            case "EXCEPTION" => LegacyBehaviorPolicy.EXCEPTION
            case _ => LegacyBehaviorPolicy.LEGACY
          }
        }

        val datetimeRebaseMode = toLegacyBehaviorPolicy(datetimeRebaseModeStr)
        val int96RebaseMode = toLegacyBehaviorPolicy(int96RebaseModeStr)

        val datetimeRebaseSpec = new RebaseDateTime.RebaseSpec(datetimeRebaseMode, scala.None)
        val int96RebaseSpec = new RebaseDateTime.RebaseSpec(int96RebaseMode, scala.None)

        val parquetBuilder = new ParquetPushFilterBuilder(relation.tableMeta.dataSchema, attributesToStructType(requestedAttributes),
          datetimeRebaseSpec, int96RebaseSpec)
        parquetBuilder.buildPushFilterJson(null,
          session.sessionState.conf.getConf(COLUMNAR_OMNI_ENABLE_VEC_PREDICATE_FILTER),
          session.sessionState.conf.parquetFilterPushDown
        )

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
}

object OmniHiveTableScanExecTransformer {

  private val NULL_VALUE: Char = 0x00
  private val DEFAULT_FIELD_DELIMITER: Char = 0x01
  val TEXT_INPUT_FORMAT_CLASS: Class[TextInputFormat] =
    Utils.classForName("org.apache.hadoop.mapred.TextInputFormat")
  val ORC_INPUT_FORMAT_CLASS: Class[OrcInputFormat] =
    Utils.classForName("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat")
  val PARQUET_INPUT_FORMAT_CLASS: Class[MapredParquetInputFormat] =
    Utils.classForName("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
  def isHiveTableScan(plan: SparkPlan): Boolean = {
    plan.isInstanceOf[HiveTableScanExec]
  }

  def apply(plan: SparkPlan): OmniHiveTableScanExecTransformer = {
    plan match {
      case hiveTableScan: HiveTableScanExec =>
        new OmniHiveTableScanExecTransformer(
          // need sort by dataSchema seq asc
          hiveTableScan.requestedAttributes.sortBy(_.exprId.id),
          hiveTableScan.relation,
          hiveTableScan.partitionPruningPred)(hiveTableScan.session)
      case _ =>
        throw new UnsupportedOperationException(
          s"Can't transform OmniHiveTableScanExecTransformer from ${plan.getClass.getSimpleName}")
    }
  }
}
