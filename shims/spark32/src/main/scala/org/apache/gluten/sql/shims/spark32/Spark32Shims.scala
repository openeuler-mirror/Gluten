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
package org.apache.gluten.sql.shims.spark32

import org.apache.gluten.config.GlutenConfig.ENABLE_FILES_SPLIT_SINGLE_FILE
import org.apache.gluten.execution.datasource.GlutenFormatFactory
import org.apache.gluten.expression.{ExpressionNames, Sig}
import org.apache.gluten.sql.shims.{ShimDescriptor, SparkShims}

import org.apache.spark.{ShuffleUtils, SparkContext, TaskContext, TaskContextUtils}
import org.apache.spark.scheduler.TaskInfo
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BinaryExpression, Expression, InputFileBlockLength, InputFileBlockStart, InputFileName, NamedExpression, ProjectionOverSchema}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.{JoinType, QueryPlan}
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationRef, Join, JoinHint, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, HashClusteredDistribution}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.execution.{FileSourceScanExec, PartitionedFileUtil, SparkPlan}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.FileFormatWriter.Empty2Null
import org.apache.spark.sql.execution.datasources.parquet.ParquetFilters
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.text.TextScan
import org.apache.spark.sql.execution.datasources.v2.utils.CatalogUtil
import org.apache.spark.sql.execution.exchange.BroadcastExchangeLike
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectBase, InsertIntoHiveDirCommand, InsertIntoHiveTable}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.{DecimalType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.storage.{BlockId, BlockManagerId}

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.parquet.schema.MessageType

import java.util.{HashMap => JHashMap, Locale, Map => JMap, Properties}

class Spark32Shims extends SparkShims {
  override def getShimDescriptor: ShimDescriptor = SparkShimProvider.DESCRIPTOR

  override def getDistribution(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression]): Seq[Distribution] = {
    HashClusteredDistribution(leftKeys) :: HashClusteredDistribution(rightKeys) :: Nil
  }

  override def scalarExpressionMappings: Seq[Sig] = Seq(Sig[Empty2Null](ExpressionNames.EMPTY2NULL))

  override def aggregateExpressionMappings: Seq[Sig] = Seq.empty

  override def convertPartitionTransforms(
      partitions: Seq[Transform]): (Seq[String], Option[BucketSpec]) = {
    CatalogUtil.convertPartitionTransforms(partitions)
  }

  override def generateFileScanRDD(
      sparkSession: SparkSession,
      readFunction: PartitionedFile => Iterator[InternalRow],
      filePartitions: Seq[FilePartition],
      fileSourceScanExec: FileSourceScanExec): FileScanRDD = {
    new FileScanRDD(sparkSession, readFunction, filePartitions)
  }

  override def getTextScan(
      sparkSession: SparkSession,
      fileIndex: PartitioningAwareFileIndex,
      dataSchema: StructType,
      readDataSchema: StructType,
      readPartitionSchema: StructType,
      options: CaseInsensitiveStringMap,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): TextScan = {
    TextScan(
      sparkSession,
      fileIndex,
      readDataSchema,
      readPartitionSchema,
      options,
      partitionFilters,
      dataFilters)
  }

  override def filesGroupedToBuckets(
      selectedPartitions: Array[PartitionDirectory]): Map[Int, Array[PartitionedFile]] = {
    selectedPartitions
      .flatMap {
        p => p.files.map(f => PartitionedFileUtil.getPartitionedFile(f, f.getPath, p.values))
      }
      .groupBy {
        f =>
          BucketingUtils
            .getBucketId(f.filePath)
            .getOrElse(throw new IllegalStateException(s"Invalid bucket file ${f.filePath}"))
      }
  }

  override def getBatchScanExecTable(batchScan: BatchScanExec): Table = null

  override def generatePartitionedFile(
      partitionValues: InternalRow,
      filePath: String,
      start: Long,
      length: Long,
      @transient locations: Array[String] = Array.empty): PartitionedFile =
    PartitionedFile(partitionValues, filePath, start, length, locations)

  override def bloomFilterExpressionMappings(): Seq[Sig] = List.empty

  override def newBloomFilterAggregate[T](
      child: Expression,
      estimatedNumItemsExpression: Expression,
      numBitsExpression: Expression,
      mutableAggBufferOffset: Int,
      inputAggBufferOffset: Int): TypedImperativeAggregate[T] =
    throw new UnsupportedOperationException()

  override def newMightContain(
      bloomFilterExpression: Expression,
      valueExpression: Expression): BinaryExpression =
    throw new UnsupportedOperationException()

  override def replaceBloomFilterAggregate[T](
      expr: Expression,
      bloomFilterAggReplacer: (
          Expression,
          Expression,
          Expression,
          Int,
          Int) => TypedImperativeAggregate[T]): Expression = expr

  override def replaceMightContain[T](
      expr: Expression,
      mightContainReplacer: (Expression, Expression) => BinaryExpression): Expression = expr

  override def getExtendedColumnarPostRules(): List[SparkSession => Rule[SparkPlan]] = {
    List(session => GlutenFormatFactory.getExtendedColumnarPostRule(session))
  }

  override def createTestTaskContext(properties: Properties): TaskContext = {
    TaskContextUtils.createTestTaskContext(properties)
  }

  def setJobDescriptionOrTagForBroadcastExchange(
      sc: SparkContext,
      broadcastExchange: BroadcastExchangeLike): Unit = {
    // Setup a job group here so later it may get cancelled by groupId if necessary.
    sc.setJobGroup(
      broadcastExchange.runId.toString,
      s"broadcast exchange (runId ${broadcastExchange.runId})",
      interruptOnCancel = true)
  }

  def cancelJobGroupForBroadcastExchange(
      sc: SparkContext,
      broadcastExchange: BroadcastExchangeLike): Unit = {
    sc.cancelJobGroup(broadcastExchange.runId.toString)
  }

  override def getShuffleReaderParam[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int): Tuple2[Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])], Boolean] = {
    ShuffleUtils.getReaderParam(handle, startMapIndex, endMapIndex, startPartition, endPartition)
  }

  override def getPartitionId(taskInfo: TaskInfo): Int = {
    throw new IllegalStateException("This is not supported.")
  }

  override def supportDuplicateReadingTracking: Boolean = false

  def getFileStatus(partition: PartitionDirectory): Seq[FileStatus] = partition.files

  def isFileSplittable(
      relation: HadoopFsRelation,
      filePath: Path,
      sparkSchema: StructType): Boolean =
    relation.sparkSession.sessionState.conf.getConf(ENABLE_FILES_SPLIT_SINGLE_FILE)

  def isRowIndexMetadataColumn(name: String): Boolean = false

  def findRowIndexColumnIndexInSchema(sparkSchema: StructType): Int = -1

  def splitFiles(
      sparkSession: SparkSession,
      file: FileStatus,
      filePath: Path,
      isSplitable: Boolean,
      maxSplitBytes: Long,
      partitionValues: InternalRow): Seq[PartitionedFile] = {
    PartitionedFileUtil.splitFiles(
      sparkSession,
      file,
      filePath,
      isSplitable,
      maxSplitBytes,
      partitionValues)
  }

  def structFromAttributes(attrs: Seq[Attribute]): StructType = {
    StructType(attrs.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
  }

  def attributesFromStruct(structType: StructType): Seq[Attribute] = {
    structType.fields.map {
      field => AttributeReference(field.name, field.dataType, field.nullable, field.metadata)()
    }
  }

  override def getFileSizeAndModificationTime(
      file: PartitionedFile): (Option[Long], Option[Long]) = {
    (None, None)
  }

  override def generateMetadataColumns(
      file: PartitionedFile,
      metadataColumnNames: Seq[String]): JMap[String, String] = {
    val metadataColumn = new JHashMap[String, String]()
    metadataColumn.put(InputFileName().prettyName, file.filePath)
    metadataColumn.put(InputFileBlockStart().prettyName, file.start.toString)
    metadataColumn.put(InputFileBlockLength().prettyName, file.length.toString)
    metadataColumn
  }

  def getAnalysisExceptionPlan(ae: AnalysisException): Option[LogicalPlan] = {
    ae.plan
  }

  override def getKeyGroupedPartitioning(batchScan: BatchScanExec): Option[Seq[Expression]] = null

  override def getCommonPartitionValues(batchScan: BatchScanExec): Option[Seq[(InternalRow, Int)]] =
    null

  override def dateTimestampFormatInReadIsDefaultValue(
      csvOptions: CSVOptions,
      timeZone: String): Boolean = {
    val default = new CSVOptions(CaseInsensitiveMap(Map()), csvOptions.columnPruning, timeZone)
    csvOptions.dateFormat == default.dateFormat &&
    csvOptions.timestampFormat == default.timestampFormat
  }

  override def createParquetFilters(
      conf: SQLConf,
      schema: MessageType,
      caseSensitive: Option[Boolean] = None): ParquetFilters = {
    new ParquetFilters(
      schema,
      conf.parquetFilterPushDownDate,
      conf.parquetFilterPushDownTimestamp,
      conf.parquetFilterPushDownDecimal,
      conf.parquetFilterPushDownStringStartWith,
      conf.parquetFilterPushDownInFilterThreshold,
      caseSensitive.getOrElse(conf.caseSensitiveAnalysis),
      LegacyBehaviorPolicy.CORRECTED
    )
  }

  override def getPushedDownFilters(
      relation: HadoopFsRelation,
      dataFilters: Seq[Expression]): Seq[org.apache.spark.sql.sources.Filter] = {
    val dataSourceUtilsModule =
      Class.forName("org.apache.spark.sql.execution.datasources.DataSourceUtils$")
        .getField("MODULE$")
        .get(null)
    val supportNestedPredicatePushdown = dataSourceUtilsModule
      .getClass
      .getDeclaredMethod("supportNestedPredicatePushdown", classOf[BaseRelation])
    supportNestedPredicatePushdown.setAccessible(true)
    val nestedPushdownEnabled = supportNestedPredicatePushdown
      .invoke(dataSourceUtilsModule, relation)
      .asInstanceOf[Boolean]

    val dataSourceStrategyModule =
      Class.forName("org.apache.spark.sql.execution.datasources.DataSourceStrategy$")
        .getField("MODULE$")
        .get(null)
    val translateFilter = dataSourceStrategyModule
      .getClass
      .getDeclaredMethod("translateFilter", classOf[Expression], classOf[Boolean])
    translateFilter.setAccessible(true)
    dataFilters.flatMap {
      filter =>
        translateFilter
          .invoke(dataSourceStrategyModule, filter, Boolean.box(nestedPushdownEnabled))
          .asInstanceOf[Option[org.apache.spark.sql.sources.Filter]]
    }
  }

  override def extractEquiJoinKeys(
      join: Join): Option[
    (JoinType, Seq[Expression], Seq[Expression], Option[Expression], LogicalPlan, LogicalPlan,
      JoinHint)] = {
    ExtractEquiJoinKeys.unapply(join).map {
      case (joinType, leftKeys, rightKeys, otherPredicates, left, right, hint) =>
        (joinType, leftKeys, rightKeys, otherPredicates, left, right, hint)
    }
  }

  override def executeWriteFiles(
      plan: SparkPlan,
      writeFilesSpec: Any): org.apache.spark.rdd.RDD[WriterCommitMessage] = {
    plan.asInstanceOf[WriteFilesExec].doExecuteWrite(writeFilesSpec.asInstanceOf[WriteFilesSpec])
  }

  override def checkColumnNameDuplication(
      columnNames: Seq[String],
      colType: String,
      caseSensitiveAnalysis: Boolean): Unit = {
    val normalized = if (caseSensitiveAnalysis) {
      columnNames
    } else {
      columnNames.map(_.toLowerCase(Locale.ROOT))
    }
    normalized
      .groupBy(identity)
      .collectFirst { case (name, values) if values.size > 1 => name }
      .foreach {
        dup =>
          throw new IllegalArgumentException(s"Found duplicate column(s) $dup $colType")
      }
  }

  override def unsupportedSaveModeError(mode: SaveMode, pathExists: Boolean): Throwable = {
    QueryExecutionErrors.unsupportedSaveModeError(mode.toString, pathExists)
  }

  override def taskFailedWhileWritingRowsError(path: String, cause: Throwable): Throwable = {
    QueryExecutionErrors.taskFailedWhileWritingRowsError(cause)
  }

  override def createHashAggregateExec(
      requiredChildDistributionExpressions: Option[Seq[Expression]],
      isStreaming: Boolean,
      numShufflePartitions: Option[Int],
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute],
      initialInputBufferOffset: Int,
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): HashAggregateExec = {
    HashAggregateExec(
      requiredChildDistributionExpressions = requiredChildDistributionExpressions,
      groupingExpressions = groupingExpressions,
      aggregateExpressions = aggregateExpressions,
      aggregateAttributes = aggregateAttributes,
      initialInputBufferOffset = initialInputBufferOffset,
      resultExpressions = resultExpressions,
      child = child
    )
  }

  override def createObjectHashAggregateExec(
      requiredChildDistributionExpressions: Option[Seq[Expression]],
      isStreaming: Boolean,
      numShufflePartitions: Option[Int],
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute],
      initialInputBufferOffset: Int,
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): ObjectHashAggregateExec = {
    ObjectHashAggregateExec(
      requiredChildDistributionExpressions = requiredChildDistributionExpressions,
      groupingExpressions = groupingExpressions,
      aggregateExpressions = aggregateExpressions,
      aggregateAttributes = aggregateAttributes,
      initialInputBufferOffset = initialInputBufferOffset,
      resultExpressions = resultExpressions,
      child = child
    )
  }

  override def createSortAggregateExec(
      requiredChildDistributionExpressions: Option[Seq[Expression]],
      isStreaming: Boolean,
      numShufflePartitions: Option[Int],
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute],
      initialInputBufferOffset: Int,
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): SortAggregateExec = {
    SortAggregateExec(
      requiredChildDistributionExpressions = requiredChildDistributionExpressions,
      groupingExpressions = groupingExpressions,
      aggregateExpressions = aggregateExpressions,
      aggregateAttributes = aggregateAttributes,
      initialInputBufferOffset = initialInputBufferOffset,
      resultExpressions = resultExpressions,
      child = child
    )
  }

  override def createProjectionOverSchema(
      schema: StructType,
      output: Seq[Attribute]): ProjectionOverSchema = {
    ProjectionOverSchema(schema)
  }

  override def getNativeWriteFormatForHiveCommand(
      cmd: DataWritingCommand,
      formatMapping: Map[String, String],
      isRegistered: String => Boolean): Option[String] = {
    cmd match {
      case command: InsertIntoHiveDirCommand =>
        command.storage.outputFormat.flatMap(formatMapping.get).filter(isRegistered)
      case command: InsertIntoHiveTable =>
        command.table.storage.outputFormat.flatMap(formatMapping.get).filter(isRegistered)
      case command: CreateHiveTableAsSelectBase =>
        command.tableDesc.storage.outputFormat.flatMap(formatMapping.get).filter(isRegistered)
      case _ =>
        None
    }
  }

  override def genDecimalRoundExpressionOutput(
      decimalType: DecimalType,
      toScale: Int): DecimalType = {
    val p = decimalType.precision
    val s = decimalType.scale
    DecimalType(p, if (toScale > s) s else toScale)
  }

  override def getOperatorId(plan: QueryPlan[_]): Option[Int] = {
    plan.getTagValue(QueryPlan.OP_ID_TAG)
  }

  override def setOperatorId(plan: QueryPlan[_], opId: Int): Unit = {
    plan.setTagValue(QueryPlan.OP_ID_TAG, opId)
  }

  override def unsetOperatorId(plan: QueryPlan[_]): Unit = {
    plan.unsetTagValue(QueryPlan.OP_ID_TAG)
  }

  override def createCTERelationRef(
      cteId: Long,
      resolved: Boolean,
      output: Seq[Attribute],
      isStreaming: Boolean,
      tatsOpt: Option[Statistics] = None): CTERelationRef = {
    CTERelationRef(cteId, resolved, output, tatsOpt)
  }
}
