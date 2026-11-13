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

import scala.util.control.NonFatal

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.{DataWritingCommand, LeafRunnableCommand}
import org.apache.spark.sql.types.StructType

/** Identifies a CTAS whose nested insert is provided by the backend. */
trait GlutenHiveCtas {
  def tableDesc: CatalogTable
}

object GlutenHiveCtas {
  /** Spark 3.4+ plans Hive CTAS without a physical query child. */
  def runnable(
      tableDesc: CatalogTable,
      query: LogicalPlan,
      outputNames: Seq[String],
      mode: SaveMode,
      rawSchema: (StructType, SparkSession) => StructType,
      insert: (CatalogTable, LogicalPlan, Seq[String], Boolean) => DataWritingCommand)
      : LeafRunnableCommand =
    RunnableHiveCtas(tableDesc, query, outputNames, mode, rawSchema, insert)

  private case class RunnableHiveCtas(
      tableDesc: CatalogTable,
      query: LogicalPlan,
      outputNames: Seq[String],
      mode: SaveMode,
      rawSchema: (StructType, SparkSession) => StructType,
      insert: (CatalogTable, LogicalPlan, Seq[String], Boolean) => DataWritingCommand)
    extends LeafRunnableCommand with GlutenHiveCtas {

    // Keep the historical Omni plan name for compatibility with plan validators and reports.
    override def nodeName: String = "OmniCreateHiveTableAsSelectCommand"

    override def innerChildren: Seq[LogicalPlan] = Seq(query)

    override def run(session: SparkSession): Seq[Row] = {
      val catalog = session.sessionState.catalog
      val identifier = tableDesc.identifier

      def write(table: CatalogTable): Unit =
        session.sessionState.executePlan(insert(table, query, outputNames, false))
          .assertCommandExecuted()

      if (catalog.tableExists(identifier)) {
        assert(mode != SaveMode.Overwrite, s"Expected table $identifier to have been dropped")
        if (mode == SaveMode.ErrorIfExists) {
          throw QueryCompilationErrors.tableIdentifierExistsError(identifier)
        }
        if (mode != SaveMode.Ignore) {
          write(tableDesc)
        }
      } else {
        tableDesc.storage.locationUri.foreach { path =>
          DataWritingCommand.assertEmptyRootPath(path, mode, session.sessionState.newHadoopConf)
        }
        val output = DataWritingCommand.logicalPlanOutputWithNames(query, outputNames)
        assert(tableDesc.schema.isEmpty)
        catalog.createTable(
          tableDesc.copy(schema = rawSchema(output.toStructType, session)), ignoreIfExists = false)
        try {
          write(catalog.getTableMetadata(identifier))
        } catch {
          case NonFatal(e) =>
            catalog.dropTable(identifier, ignoreIfNotExists = true, purge = false)
            throw e
        }
      }
      Seq.empty
    }
  }
}
