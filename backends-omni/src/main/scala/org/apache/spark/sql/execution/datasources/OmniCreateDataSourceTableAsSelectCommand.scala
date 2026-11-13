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

package org.apache.spark.sql.execution.datasources

import java.net.URI

import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.{CommandExecutionMode, SparkPlan}
import org.apache.spark.sql.execution.command.{CommandUtils, DataWritingCommand, RepairTableCommand}
import org.apache.spark.sql.sources.BaseRelation

/**
 * Offload of data source CTAS (`CREATE TABLE ... USING format AS SELECT`). Mirrors the customer's
 * CreateDataSourceTableAsSelectCommand, but delegates the data write to
 * OmniInsertIntoHadoopFsRelationCommand so the data path stays on the Omni native writer without
 * shadowing any customer Spark class.
 */
case class OmniCreateDataSourceTableAsSelectCommand(
    table: CatalogTable,
    mode: SaveMode,
    query: LogicalPlan,
    outputColumnNames: Seq[String])
  extends DataWritingCommand {

  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    assert(table.tableType != CatalogTableType.VIEW)
    assert(table.provider.isDefined)

    val sessionState = sparkSession.sessionState
    val db = table.identifier.database.getOrElse(sessionState.catalog.getCurrentDatabase)
    val tableIdentWithDB = table.identifier.copy(database = Some(db))
    val tableName = tableIdentWithDB.unquotedString

    if (sessionState.catalog.tableExists(tableIdentWithDB)) {
      assert(mode != SaveMode.Overwrite,
        s"Expect the table $tableName has been dropped when the save mode is Overwrite")

      if (mode == SaveMode.ErrorIfExists) {
        throw SparkShimLoader.getSparkShims.writeTableAlreadyExists(tableIdentWithDB)
      }
      if (mode == SaveMode.Ignore) {
        // Since the table already exists and the save mode is Ignore, we will just return.
        return Seq.empty
      }

      saveDataIntoTable(
        sparkSession, table, table.storage.locationUri, child, SaveMode.Append, tableExists = true)
    } else {
      table.storage.locationUri.foreach { p =>
        DataWritingCommand.assertEmptyRootPath(p, mode, sparkSession.sessionState.newHadoopConf)
      }
      assert(table.schema.isEmpty)
      sparkSession.sessionState.catalog.validateTableLocation(table)
      val tableLocation = if (table.tableType == CatalogTableType.MANAGED) {
        Some(sessionState.catalog.defaultTablePath(table.identifier))
      } else {
        table.storage.locationUri
      }
      val result = saveDataIntoTable(
        sparkSession, table, tableLocation, child, SaveMode.Overwrite, tableExists = false)
      val tableSchema = CharVarcharUtils.getRawSchema(result.schema)
      val newTable = table.copy(
        storage = table.storage.copy(locationUri = tableLocation),
        // We will use the schema of resolved.relation as the schema of the table (instead of
        // the schema of df). It is important since the nullability may be changed by the relation
        // provider (for example, see org.apache.spark.sql.parquet.DefaultSource).
        schema = tableSchema)
      // Table location is already validated. No need to check it again during table creation.
      sessionState.catalog.createTable(newTable, ignoreIfExists = false, validateLocation = false)

      result match {
        case fs: HadoopFsRelation if table.partitionColumnNames.nonEmpty &&
            sparkSession.sqlContext.conf.manageFilesourcePartitions =>
          // Need to recover partitions into the metastore so our saved data is visible.
          sessionState.executePlan(RepairTableCommand(
            table.identifier,
            enableAddPartitions = true,
            enableDropPartitions = false), CommandExecutionMode.SKIP).toRdd
        case _ =>
      }
    }

    CommandUtils.updateTableStats(sparkSession, table)

    Seq.empty[Row]
  }

  private def saveDataIntoTable(
      session: SparkSession,
      table: CatalogTable,
      tableLocation: Option[URI],
      physicalPlan: SparkPlan,
      mode: SaveMode,
      tableExists: Boolean): BaseRelation = {
    // Create the relation based on the input logical plan: `query`.
    val pathOption = tableLocation.map("path" -> CatalogUtils.URIToString(_))
    val dataSource = DataSource(
      session,
      className = table.provider.get,
      partitionColumns = table.partitionColumnNames,
      bucketSpec = table.bucketSpec,
      options = table.storage.properties ++ pathOption,
      catalogTable = if (tableExists) Some(table) else None)

    try {
      dataSource.providingClass.getConstructor().newInstance() match {
        case format: FileFormat =>
          writeThroughOmniCommand(session, dataSource, format, mode, physicalPlan)
        case _ =>
          // Non file-format providers (e.g. JDBC) keep the customer Spark write path.
          SparkShimLoader.getSparkShims.writeAndRead(
            dataSource, mode, query, outputColumnNames, physicalPlan, metrics)
      }
    } catch {
      case ex: AnalysisException =>
        logError(s"Failed to write to table ${table.identifier.unquotedString}", ex)
        throw ex
    }
  }

  /**
   * Replicates the FileFormat branch of DataSource.writeAndRead (including the private
   * planForWritingFileFormat), but runs the Omni command instead of the vanilla one, so the
   * data path stays on the Omni native writer.
   */
  private def writeThroughOmniCommand(
      session: SparkSession,
      dataSource: DataSource,
      format: FileFormat,
      mode: SaveMode,
      physicalPlan: SparkPlan): BaseRelation = {
    val data = query
    val outputColumns = DataWritingCommand.logicalPlanOutputWithNames(data, outputColumnNames)

    // Don't glob path for the write path. The contracts here are:
    //  1. Only one output path can be specified on the write path;
    //  2. Output path must be a legal HDFS style file system path;
    //  3. It's OK that the output path doesn't exist yet.
    // caseInsensitiveOptions is private in DataSource; match its CaseInsensitiveMap.get("path")
    // behavior by scanning options case-insensitively.
    val pathFromOptions = dataSource.options.collectFirst {
      case (key, value) if key.equalsIgnoreCase("path") => value
    }
    val allPaths = dataSource.paths ++ pathFromOptions
    val outputPath = if (allPaths.length == 1) {
      val path = new Path(allPaths.head)
      val fs = path.getFileSystem(
        session.sessionState.newHadoopConfWithOptions(dataSource.options))
      path.makeQualified(fs.getUri, fs.getWorkingDirectory)
    } else {
      throw QueryExecutionErrors.multiplePathsSpecifiedError(allPaths)
    }

    val caseSensitive = session.sessionState.conf.caseSensitiveAnalysis
    PartitioningUtils.validatePartitionColumn(
      data.schema, dataSource.partitionColumns, caseSensitive)

    val fileIndex: Option[FileIndex] = dataSource.catalogTable.map(_.identifier).map { tableIdent =>
      session.table(tableIdent).queryExecution.analyzed.collect {
        case LogicalRelation(t: HadoopFsRelation, _, _, _) => t.location
      }.head
    }

    val resolver = session.sessionState.conf.resolver
    val unresolvedPartCols = dataSource.partitionColumns.map(UnresolvedAttribute.quoted)
    val resolvedPartCols = unresolvedPartCols.map { col =>
      assert(col.isInstanceOf[UnresolvedAttribute])
      val unresolvedCol = col.asInstanceOf[UnresolvedAttribute]
      assert(unresolvedCol.nameParts.length == 1)
      val name = unresolvedCol.nameParts.head
      outputColumns.find(a => resolver(a.name, name)).getOrElse {
        throw QueryCompilationErrors.cannotResolveAttributeError(
          name, data.output.map(_.name).mkString(", "))
      }
    }

    val omniCommand = OmniInsertIntoHadoopFsRelationCommand(
      outputPath,
      Map.empty,
      false,
      resolvedPartCols,
      dataSource.bucketSpec,
      format,
      dataSource.options,
      data,
      mode,
      dataSource.catalogTable,
      fileIndex,
      outputColumnNames)
    omniCommand.run(session, physicalPlan)
    DataWritingCommand.propogateMetrics(session.sparkContext, omniCommand, metrics)

    // Replace the schema with that of the DataFrame we just wrote out to avoid re-inferring
    dataSource.copy(userSpecifiedSchema = Some(outputColumns.toStructType.asNullable))
      .resolveRelation()
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(query = newChild)
}
