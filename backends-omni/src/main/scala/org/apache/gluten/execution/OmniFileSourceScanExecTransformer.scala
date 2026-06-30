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
import com.huawei.boostkit.spark.jni.{OrcPushFilterBuilder, ParquetPushFilterBuilder}
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig.COLUMNAR_OMNI_ENABLE_VEC_PREDICATE_FILTER
import org.apache.gluten.expression.{ConverterUtils, ExpressionConverter}
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.`type`.ColumnTypeNode
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.substrait.rel.RelBuilder
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.BitSet
import io.substrait.proto.NamedStruct
import org.apache.spark.sql.internal.SQLConf
import scala.Option

import scala.collection.JavaConverters._

case class OmniFileSourceScanExecTransformer(
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
      disableBucketedScan) {

  import org.apache.spark.sql.catalyst.util._

  override def doCanonicalize(): OmniFileSourceScanExecTransformer = {
    OmniFileSourceScanExecTransformer(
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
      disableBucketedScan
    )
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
    val filter = SparkShimLoader
      .getSparkShims
      .getPushedDownFilters(relation, dataFilters)
      .reduceOption(org.apache.spark.sql.sources.And(_, _))

    val json = this.fileFormat match {
      // ORC PushFilterJsonBuilder
      case ReadFileFormat.OrcReadFormat =>
        val orcBuilder = new OrcPushFilterBuilder(relation.dataSchema, requiredSchema)
        orcBuilder.buildPushFilterJson(filter.orNull,
          session.sessionState.conf.getConf(COLUMNAR_OMNI_ENABLE_VEC_PREDICATE_FILTER),
          session.sessionState.conf.orcFilterPushDown
        )

      // Parquet PushFilterJsonBuilder
      case ReadFileFormat.ParquetReadFormat =>
        val datetimeRebaseModeStr = session.sessionState.conf.getConf(SQLConf.PARQUET_REBASE_MODE_IN_READ)
        val int96RebaseModeStr = session.sessionState.conf.getConf(SQLConf.PARQUET_INT96_REBASE_MODE_IN_READ)

        val parquetBuilder = new ParquetPushFilterBuilder(relation.dataSchema, requiredSchema,
          datetimeRebaseModeStr, int96RebaseModeStr)
        parquetBuilder.buildPushFilterJson(filter.orNull,
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
