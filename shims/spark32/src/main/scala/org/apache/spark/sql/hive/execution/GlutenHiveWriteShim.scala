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

package org.apache.spark.sql.hive.execution

import org.apache.gluten.sql.shims.{HiveWriteStaging, SparkShims}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.{DDLUtils, DataWritingCommand, DataWritingCommandExec}
import org.apache.spark.sql.execution.datasources.{DataSource, GlutenWriteEmpty2Null}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.sources.BaseRelation

trait GlutenHiveWriteShim extends SparkShims {
  override def writeEmptyToNull(child: Expression): Expression = GlutenWriteEmpty2Null(child)

  override def writeTableAlreadyExists(table: TableIdentifier): Throwable =
    QueryCompilationErrors.tableAlreadyExistsError(table.unquotedString, " You need to drop it first.")

  override def writeAndRead(
      source: DataSource,
      mode: SaveMode,
      query: LogicalPlan,
      outputNames: Seq[String],
      plan: SparkPlan,
      metrics: Map[String, SQLMetric]): BaseRelation =
    source.writeAndRead(mode, query, outputNames, plan, metrics)

  override def checkHiveWriteColumns(table: CatalogTable): Unit =
    DDLUtils.checkDataColNames(table)

  override def hiveWriteStaging(
      command: InsertIntoHiveDirCommand,
      session: SparkSession,
      hadoopConf: Configuration,
      target: Path): HiveWriteStaging = {
    // Reuse Spark's legacy staging lifecycle through the trait that owns it.
    case class StagingCommand(query: LogicalPlan, outputColumnNames: Seq[String])
      extends SaveAsHiveFile {
      override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] =
        throw new UnsupportedOperationException("Hive staging helper cannot execute a write")

      override protected def withNewChildInternal(newChild: LogicalPlan): StagingCommand =
        copy(query = newChild)

      def open(): HiveWriteStaging = {
        val staging = getExternalTmpPath(session, hadoopConf, target)
        new HiveWriteStaging {
          override def path: Path = staging
          override def deleteUnlessStaging(path: Path, fs: FileSystem): Unit = {
            if (!createdTempDir.contains(path)) {
              fs.delete(path, true)
            }
          }
          override def close(): Unit = deleteExternalTmpPath(hadoopConf)
        }
      }
    }
    StagingCommand(command.query, command.outputColumnNames).open()
  }

  override def rewriteHiveCtas(plan: SparkPlan)(
      insert: (CatalogTable, LogicalPlan, Seq[String], Boolean) => DataWritingCommand): SparkPlan =
    plan match {
      case exec @ DataWritingCommandExec(ctas: CreateHiveTableAsSelectCommand, _) =>
        exec.copy(cmd = GlutenLegacyHiveCtas(
          ctas.tableDesc, ctas.query, ctas.outputColumnNames, ctas.mode, insert))
      case other => other
    }

  override def isSparkHiveCtas(command: DataWritingCommand): Boolean =
    command.isInstanceOf[CreateHiveTableAsSelectBase]
}

private case class GlutenLegacyHiveCtas(
    tableDesc: CatalogTable,
    query: LogicalPlan,
    outputColumnNames: Seq[String],
    mode: SaveMode,
    insert: (CatalogTable, LogicalPlan, Seq[String], Boolean) => DataWritingCommand)
  extends CreateHiveTableAsSelectBase with GlutenHiveCtas {

  // Keep the historical Omni plan name for compatibility with plan validators and reports.
  override def nodeName: String = "OmniCreateHiveTableAsSelectCommand"

  override def getWritingCommand(
      catalog: SessionCatalog,
      table: CatalogTable,
      tableExists: Boolean): DataWritingCommand =
    insert(table, query, outputColumnNames, !tableExists)

  override def writingCommandClassName: String = nodeName

  override protected def withNewChildInternal(newChild: LogicalPlan): GlutenLegacyHiveCtas =
    copy(query = newChild)
}
