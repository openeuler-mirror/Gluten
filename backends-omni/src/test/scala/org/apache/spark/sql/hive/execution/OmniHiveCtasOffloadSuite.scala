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

import org.apache.gluten.extension.columnar.offload.OffloadWrite

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.command.{DataWritingCommand, DataWritingCommandExec, ExecutedCommandExec, RunnableCommand}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import org.scalatest.funsuite.AnyFunSuite

class OmniHiveCtasOffloadSuite extends AnyFunSuite {
  test("Hive CTAS preserves the Spark command when disabled and replaces it when enabled") {
    val table = CatalogTable(
      TableIdentifier("omni_ctas"),
      CatalogTableType.MANAGED,
      CatalogStorageFormat.empty,
      new StructType())
    val command: LogicalPlan =
      CreateHiveTableAsSelectCommand(table, LocalRelation(), Nil, SaveMode.ErrorIfExists)
    val plan = command match {
      case cmd: DataWritingCommand => DataWritingCommandExec(cmd, LocalTableScanExec(Nil, Nil))
      case cmd: RunnableCommand => ExecutedCommandExec(cmd)
    }
    val rule = OffloadWrite()
    val conf = new SQLConf
    val key = "spark.gluten.sql.native.writer.enabled"
    SQLConf.withExistingConf(conf) {
      conf.setConfString(key, "false")
      assert(rule.offload(plan) eq plan)

      conf.setConfString(key, "true")
      val offloaded = rule.offload(plan)
      offloaded match {
        case DataWritingCommandExec(_: GlutenHiveCtas, _) =>
        case ExecutedCommandExec(_: GlutenHiveCtas) =>
        case other => fail(s"Expected an offloaded Hive CTAS, got $other")
      }

      conf.setConfString(key, "false")
      assert(rule.offload(plan) eq plan)
    }
  }
}
