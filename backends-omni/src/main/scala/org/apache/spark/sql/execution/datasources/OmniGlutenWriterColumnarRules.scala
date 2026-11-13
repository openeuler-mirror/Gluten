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
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{ColumnarToRowExecBase}
import org.apache.gluten.execution.datasource.GlutenFormatFactory
import org.apache.gluten.logging.LogLevelUtil
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, DataWritingCommand, DataWritingCommandExec}
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, OverwriteByExpressionExec}
import org.apache.spark.sql.hive.execution.{GlutenHiveCtas, InsertIntoHiveDirCommand, InsertIntoHiveTable, OmniInsertIntoHiveDirCommand, OmniInsertIntoHiveTable}
import org.apache.spark.sql.sources.DataSourceRegister

import java.util.Locale

object OmniGlutenWriterColumnarRules extends Logging with LogLevelUtil {
  // TODO: support ctas in Spark3.4, see https://github.com/apache/spark/pull/39220
  // Dynamic partition and bucket writes are supported on this branch through the Omni command
  // path: OmniFileFormatWriter injects Empty2Null / `__bucket_value__` projections that the
  // native side evaluates per row (see FileFormatWriter.nativeWrap in the spark32 shim).
  private val formatMapping = Map(
    "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat" -> "orc",
    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat" -> "parquet"
  )

  // Same message shape and log level policy as GlutenFallbackReporter so that
  // native-write fallbacks are greppable alongside validation fallbacks.
  private def logFallback(session: SparkSession, nodeName: String, reason: String): Unit = {
    val executionIdInfo = Option(
      session.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY))
      .map(id => s"[QueryId=$id]")
      .getOrElse("")
    logOnLevel(
      GlutenConfig.get.validationLogLevel,
      s"Validation failed for plan: $nodeName$executionIdInfo, due to: $reason.")
  }

  private def getNativeFormat(
      session: SparkSession,
      cmd: DataWritingCommand): Option[String] = {
    if (!BackendsApiManager.getSettings.enableNativeWriteFiles()) {
      logFallback(
        session,
        cmd.getClass.getSimpleName,
        "spark.gluten.sql.native.writer.enabled is false")
      return None
    }

    cmd match {
      case command: OmniCreateDataSourceTableAsSelectCommand =>
        // Spark keeps the provider string exactly as written in the SQL (e.g. "ORC"
        // for `USING ORC`), while GlutenFormatFactory keys are lowercase.
        val format = command.table.provider
          .map(_.toLowerCase(Locale.ROOT))
          .filter(GlutenFormatFactory.isRegistered)
        if (format.isEmpty) {
          logFallback(
            session,
            command.getClass.getSimpleName,
            s"provider=${command.table.provider} of table ${command.table.identifier} " +
              "is not eligible for native write")
        }
        format
      case command: OmniInsertIntoHiveDirCommand =>
        val format = command.storage.outputFormat
          .flatMap(formatMapping.get)
          .filter(GlutenFormatFactory.isRegistered)
        if (format.isEmpty) {
          logFallback(
            session,
            command.getClass.getSimpleName,
            s"outputFormat=${command.storage.outputFormat} of directory " +
              s"${command.storage.locationUri} is not eligible for native write")
        }
        format
      case command: GlutenHiveCtas =>
        val format = command.tableDesc.storage.outputFormat
          .flatMap(formatMapping.get)
          .filter(GlutenFormatFactory.isRegistered)
        if (format.isEmpty) {
          logFallback(
            session,
            command.getClass.getSimpleName,
            s"outputFormat=${command.tableDesc.storage.outputFormat} of table " +
              s"${command.tableDesc.identifier} is not eligible for native write")
        }
        format
      case command: OmniInsertIntoHadoopFsRelationCommand =>
        val format = command.fileFormat match {
          case register: DataSourceRegister
            if GlutenFormatFactory.isRegistered(register.shortName()) =>
            Some(register.shortName())
          case other =>
            logFallback(
              session,
              command.getClass.getSimpleName,
              s"fileFormat=${other.getClass.getSimpleName} is not a registered native format")
            None
        }
        format
      case command: OmniInsertIntoHiveTable =>
        val format = command.table.storage.outputFormat
          .flatMap(formatMapping.get)
          .filter(GlutenFormatFactory.isRegistered)
        if (format.isEmpty) {
          logFallback(
            session,
            command.getClass.getSimpleName,
            s"outputFormat=${command.table.storage.outputFormat} of table " +
              s"${command.table.identifier} is not eligible for native write")
        }
        format
      // OffloadWrite rewrites eligible commands into Omni commands above. What reaches below are
      // raw customer Spark commands, e.g. writes kept by FallbackTags (partial fallback): their
      // writers are customer Spark writers (the shim native branches are unreachable with the
      // Omni bundle packaging), so never select native format for them. Injecting FakeRowAdaptor
      // into those row consumers would feed FakeRow into vanilla writers (FakeRow.isNullAt
      // throws UnsupportedOperationException).
      case _: InsertIntoHiveTable =>
        None
      case _: InsertIntoHiveDirCommand =>
        None
      case command if SparkShimLoader.getSparkShims.isSparkHiveCtas(command) =>
        None
      case _: CreateDataSourceTableAsSelectCommand =>
        None
      case _: InsertIntoHadoopFsRelationCommand =>
        None
      case other =>
        val format = SparkShimLoader.getSparkShims.getNativeWriteFormatForHiveCommand(
          cmd,
          formatMapping,
          GlutenFormatFactory.isRegistered)
        if (format.isEmpty) {
          logFallback(
            session,
            other.getClass.getSimpleName,
            "command or output format is not eligible for native write")
        }
        format
    }
  }

  case class NativeWritePostRule(session: SparkSession) extends Rule[SparkPlan] {

    private val NOOP_WRITE = "org.apache.spark.sql.execution.datasources.noop.NoopWrite$"

    override def apply(p: SparkPlan): SparkPlan = p match {
      case rc @ AppendDataExec(_, _, write)
        if write.getClass.getName == NOOP_WRITE &&
          BackendsApiManager.getSettings.enableNativeWriteFiles() =>
        injectFakeRowAdaptor(rc, rc.child)
      case rc @ OverwriteByExpressionExec(_, _, write)
        if write.getClass.getName == NOOP_WRITE &&
          BackendsApiManager.getSettings.enableNativeWriteFiles() =>
        injectFakeRowAdaptor(rc, rc.child)
      case rc @ DataWritingCommandExec(cmd, child) =>
        // The same thread can set these properties in the last query submission.
        val fields = child.output.toStructType.fields
        val schemaSupported = BackendsApiManager.getSettings.supportNativeWrite(fields)
        if (!schemaSupported) {
          logFallback(
            session,
            cmd.getClass.getSimpleName,
            "unsupported schema: " +
              fields.map(f => s"${f.name}:${f.dataType.simpleString}").mkString(", "))
        }
        val format =
          if (schemaSupported) getNativeFormat(session, cmd) else None
        injectSparkLocalProperty(session, format)
        format match {
          case Some(_) =>
            injectFakeRowAdaptor(rc, child)
          case None =>
            rc.withNewChildren(rc.children.map(apply))
        }

      case plan: SparkPlan => plan.withNewChildren(plan.children.map(apply))
    }

    private def injectFakeRowAdaptor(command: SparkPlan, child: SparkPlan): SparkPlan = {
      child match {
        // if the child is columnar, we can just wrap&transfer the columnar data
        case c2r: ColumnarToRowExecBase =>
          command.withNewChildren(Array(FakeRowAdaptor(c2r.child)))
        // If the child is aqe, we make aqe "support columnar",
        // then aqe itself will guarantee to generate columnar outputs.
        // So FakeRowAdaptor will always consumes columnar data,
        // thus avoiding the case of c2r->aqe->r2c->writer
        case aqe: AdaptiveSparkPlanExec =>
          command.withNewChildren(
            Array(
              FakeRowAdaptor(
                AdaptiveSparkPlanExec(
                  aqe.inputPlan,
                  aqe.context,
                  aqe.preprocessingRules,
                  aqe.isSubquery,
                  supportsColumnar = true
                ))))
        case other =>
          logFallback(
            session,
            command.nodeName,
            s"unexpected child plan ${other.getClass.getSimpleName} under the write command, " +
              "expect ColumnarToRowExecBase or AdaptiveSparkPlanExec")
          command
      }
    }
  }

  def injectSparkLocalProperty(spark: SparkSession, format: Option[String]): Unit = {
    if (format.isDefined) {
      spark.sparkContext.setLocalProperty("isNativeApplicable", true.toString)
      spark.sparkContext.setLocalProperty("nativeFormat", format.get)
      spark.sparkContext.setLocalProperty(
        "staticPartitionWriteOnly",
        BackendsApiManager.getSettings.staticPartitionWriteOnly().toString)
    } else {
      spark.sparkContext.setLocalProperty("isNativeApplicable", null)
      spark.sparkContext.setLocalProperty("nativeFormat", null)
      spark.sparkContext.setLocalProperty("staticPartitionWriteOnly", null)
    }
  }
}
