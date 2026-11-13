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

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable}
import org.apache.spark.sql.catalyst.expressions.{Empty2Null, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.{DDLUtils, DataWritingCommand, ExecutedCommandExec}
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.sources.BaseRelation

trait GlutenHiveWriteShim extends SparkShims {
  override def writeEmptyToNull(child: Expression): Expression = Empty2Null(child)

  override def writeTableAlreadyExists(table: TableIdentifier): Throwable =
    QueryCompilationErrors.tableAlreadyExistsError(table)

  override def writeAndRead(
      source: DataSource,
      mode: SaveMode,
      query: LogicalPlan,
      outputNames: Seq[String],
      plan: SparkPlan,
      metrics: Map[String, SQLMetric]): BaseRelation =
    source.writeAndRead(mode, query, outputNames)

  override def checkHiveWriteColumns(table: CatalogTable): Unit =
    DDLUtils.checkTableColumns(table)

  override def hiveWriteStaging(
      command: InsertIntoHiveDirCommand,
      session: SparkSession,
      conf: Configuration,
      target: Path): HiveWriteStaging = {
    val staging = new HiveTempPath(session, conf, target)
    val output = staging.externalTempPath
    try {
      staging.createTmpPath()
    } catch {
      case scala.util.control.NonFatal(e) =>
        staging.deleteTmpPath()
        throw e
    }
    new HiveWriteStaging {
      override def path: Path = output
      override def deleteUnlessStaging(path: Path, fs: FileSystem): Unit =
        staging.deleteIfNotStagingDir(path, fs)
      override def close(): Unit = staging.deleteTmpPath()
    }
  }

  override def rewriteHiveCtas(plan: SparkPlan)(
      insert: (CatalogTable, LogicalPlan, Seq[String], Boolean) => DataWritingCommand): SparkPlan =
    plan match {
      case exec @ ExecutedCommandExec(
            ctas: CreateHiveTableAsSelectCommand) =>
        exec.copy(cmd = GlutenHiveCtas.runnable(
          ctas.tableDesc, ctas.query, ctas.outputColumnNames, ctas.mode,
          (schema, session) => CharVarcharUtils.getRawSchema(schema, session.sessionState.conf),
          insert))
      case other => other
    }
}
