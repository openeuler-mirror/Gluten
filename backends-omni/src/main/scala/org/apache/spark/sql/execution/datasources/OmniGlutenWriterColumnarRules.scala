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
import org.apache.gluten.execution.{
  ColumnarToRowExecBase,
  OmniGlutenLabeledAppendDataExecV1,
  OmniGlutenLabeledOverwriteByExpressionExecV1
}
import org.apache.gluten.execution.datasource.GlutenFormatFactory
import org.apache.gluten.datasources.text.OmniTextOptionsAdapter

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, DataWritingCommand, DataWritingCommandExec}
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, AppendDataExecV1, OverwriteByExpressionExec, OverwriteByExpressionExecV1}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveDirCommand, InsertIntoHiveTable, OmniInsertIntoHiveTable}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.types.StringType

object OmniGlutenWriterColumnarRules extends Logging {
  // TODO: support ctas in Spark3.4, see https://github.com/apache/spark/pull/39220
  // TODO: support dynamic partition and bucket write
  //  1. pull out `Empty2Null` and required ordering to `WriteFilesExec`, see Spark3.4 `V1Writes`
  //  2. support detect partition value, partition path, bucket value, bucket path at native side,
  //     see `BaseDynamicPartitionDataWriter`
  private val formatMapping = Map(
    "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat" -> "orc",
    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat" -> "parquet"
  )

  private def supportsNativeTextWrite(
      fileFormat: FileFormat,
      options: Map[String, String],
      dataColumns: Seq[Attribute]): Boolean = fileFormat match {
    case _: TextFileFormat =>
      dataColumns.length == 1 &&
        dataColumns.head.dataType == StringType &&
        OmniTextOptionsAdapter.validateWriteOptions(options).ok()
    case _ => true
  }

  private def isTextWrite(cmd: DataWritingCommand): Boolean = cmd match {
    case command: CreateDataSourceTableAsSelectCommand =>
      command.table.provider.exists(_.equalsIgnoreCase("text"))
    case command: InsertIntoHadoopFsRelationCommand =>
      command.fileFormat.isInstanceOf[TextFileFormat]
    case command: OmniInsertIntoHadoopFsRelationCommand =>
      command.fileFormat.isInstanceOf[TextFileFormat]
    case _ => false
  }

  private[datasources] def resolveWriteValidationOutput(
      child: SparkPlan,
      textWrite: Boolean): Seq[Attribute] = {
    if (!textWrite) {
      child.output
    } else {
      child match {
        // ColumnarWriteFilesExec deliberately exposes an empty output because it is a terminal
        // write node. Text's one-String-column gate must inspect the actual input instead.
        case write: ColumnarWriteFilesExec => write.left.output
        case _ => child.output
      }
    }
  }

  private def getNativeFormat(
      cmd: DataWritingCommand,
      output: Seq[Attribute]): Option[String] = {
    if (!BackendsApiManager.getSettings.enableNativeWriteFiles()) {
      return None
    }

    cmd match {
      case command: CreateDataSourceTableAsSelectCommand
        if !BackendsApiManager.getSettings.skipNativeCtas(command) =>
        command.table.provider
          .filter(GlutenFormatFactory.isRegistered)
          .filter {
            case "text" =>
              val partitionNames = command.table.partitionColumnNames.toSet
              val dataColumns = output.filterNot(attr => partitionNames.contains(attr.name))
              dataColumns.length == 1 &&
                dataColumns.head.dataType == StringType &&
                OmniTextOptionsAdapter
                  .validateWriteOptions(command.table.storage.properties)
                  .ok()
            case _ => true
          }
      case command: InsertIntoHadoopFsRelationCommand
        if !BackendsApiManager.getSettings.skipNativeInsertInto(command) =>
        val partitionIds = command.partitionColumns.map(_.exprId).toSet
        val dataColumns = output.filterNot(attr => partitionIds.contains(attr.exprId))
        command.fileFormat match {
          case register: DataSourceRegister
            if GlutenFormatFactory.isRegistered(register.shortName()) &&
              supportsNativeTextWrite(command.fileFormat, command.options, dataColumns) =>
            Some(register.shortName())
          case _ => None
        }
      case command: OmniInsertIntoHadoopFsRelationCommand =>
        val partitionIds = command.partitionColumns.map(_.exprId).toSet
        val dataColumns = output.filterNot(attr => partitionIds.contains(attr.exprId))
        command.fileFormat match {
          case register: DataSourceRegister
            if GlutenFormatFactory.isRegistered(register.shortName()) &&
              supportsNativeTextWrite(command.fileFormat, command.options, dataColumns) =>
            Some(register.shortName())
          case _ => None
        }
      case command: InsertIntoHiveDirCommand =>
        command.storage.outputFormat
          .flatMap(formatMapping.get)
          .filter(GlutenFormatFactory.isRegistered)
      case command: InsertIntoHiveTable =>
        command.table.storage.outputFormat
          .flatMap(formatMapping.get)
          .filter(GlutenFormatFactory.isRegistered)
      case command: OmniInsertIntoHiveTable =>
        command.table.storage.outputFormat
          .flatMap(formatMapping.get)
          .filter(GlutenFormatFactory.isRegistered)
      case command: CreateHiveTableAsSelectCommand =>
        command.tableDesc.storage.outputFormat
          .flatMap(formatMapping.get)
          .filter(GlutenFormatFactory.isRegistered)
      case _ =>
        None
    }
  }

  private def isDeltaV1FallbackWrite(write: Any): Boolean = {
    write.getClass.getName.startsWith("org.apache.spark.sql.delta.catalog.WriteIntoDeltaBuilder")
  }

  private def resolveClassSource(className: String): String = {
    try {
      val clazz = Class.forName(className)
      Option(clazz.getProtectionDomain)
        .flatMap(domain => Option(domain.getCodeSource))
        .flatMap(source => Option(source.getLocation))
        .map(_.toString)
        .getOrElse("<unknown>")
    } catch {
      case _: Throwable => "<not-found>"
    }
  }

  case class NativeWritePostRule(session: SparkSession) extends Rule[SparkPlan] {

    private val NOOP_WRITE = "org.apache.spark.sql.execution.datasources.noop.NoopWrite$"

    override def apply(p: SparkPlan): SparkPlan = p match {
      case rc: AppendDataExecV1 =>
        if (isDeltaV1FallbackWrite(rc.write)) {
          logInfo(
            s"[Omni-Proof][WriteRule] AppendDataExecV1 detected for Delta V1 fallback write; " +
              s"final native offload is decided in DeltaLog.startTransaction / " +
              s"OmniOptimisticTransaction, not by NativeWritePostRule. " +
              s"write=${rc.write.getClass.getName}, " +
              s"deltaLogSource=${resolveClassSource("org.apache.spark.sql.delta.DeltaLog")}, " +
              s"omniTxnSource=${resolveClassSource("org.apache.spark.sql.delta.OmniOptimisticTransaction")}")
          logInfo(
            "[Omni-Proof][WriteRule] Replacing Delta AppendDataExecV1 with LeafV2CommandExec " +
              "delegate (same nodeName=AppendDataExecV1, Gluten+Omni line in stringArgs).")
          OmniGlutenLabeledAppendDataExecV1(rc)
        } else {
          logInfo(
            s"[Omni-Proof][WriteRule] AppendDataExecV1 detected; " +
              s"this V1 fallback write path is not rewritten to Omni native write " +
              s"in the current build. write=${rc.write.getClass.getName}")
          rc
        }
      case rc: OverwriteByExpressionExecV1 =>
        if (isDeltaV1FallbackWrite(rc.write)) {
          logInfo(
            s"[Omni-Proof][WriteRule] OverwriteByExpressionExecV1 detected for Delta V1 fallback; " +
              s"write=${rc.write.getClass.getName}")
          logInfo(
            "[Omni-Proof][WriteRule] Replacing Delta OverwriteByExpressionExecV1 with LeafV2CommandExec " +
              "delegate (same nodeName=OverwriteByExpressionExecV1, Gluten+Omni line in stringArgs).")
          OmniGlutenLabeledOverwriteByExpressionExecV1(rc)
        } else {
          logInfo(
            s"[Omni-Proof][WriteRule] OverwriteByExpressionExecV1 detected; " +
              s"this V1 fallback write path is not rewritten to Omni native write " +
              s"in the current build. write=${rc.write.getClass.getName}")
          rc
        }
      case rc @ AppendDataExec(_, _, write)
        if write.getClass.getName == NOOP_WRITE &&
          BackendsApiManager.getSettings.enableNativeWriteFiles() =>
        logInfo(
          s"[Omni-Proof][WriteRule] append uses NOOP writer; injecting FakeRowAdaptor " +
            s"for child=${rc.child.nodeName}")
        injectFakeRowAdaptor(rc, rc.child)
      case rc @ OverwriteByExpressionExec(_, _, write)
        if write.getClass.getName == NOOP_WRITE &&
          BackendsApiManager.getSettings.enableNativeWriteFiles() =>
        logInfo(
          s"[Omni-Proof][WriteRule] overwrite uses NOOP writer; injecting FakeRowAdaptor " +
            s"for child=${rc.child.nodeName}")
        injectFakeRowAdaptor(rc, rc.child)
      case rc @ DataWritingCommandExec(cmd, child) =>
        // The same thread can set these properties in the last query submission.
        val validationOutput = resolveWriteValidationOutput(child, isTextWrite(cmd))
        val fields = validationOutput.toStructType.fields
        val format =
          if (BackendsApiManager.getSettings.supportNativeWrite(fields)) {
            getNativeFormat(cmd, validationOutput)
          } else {
            None
          }
        injectSparkLocalProperty(session, format)
        format match {
          case Some(_) =>
            logInfo(
              s"[Omni-Proof][WriteRule] native write applicable; " +
                s"command=${cmd.getClass.getSimpleName}, child=${child.nodeName}, " +
                s"format=${format.get}, columns=${child.output.map(_.name).mkString("[", ",", "]")}")
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
        case other => command
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
      logInfo(
        s"[Omni-Proof][WriteRule] set Spark local properties: " +
          s"isNativeApplicable=true, nativeFormat=${format.get}, " +
          s"staticPartitionWriteOnly=${BackendsApiManager.getSettings.staticPartitionWriteOnly()}")
    } else {
      spark.sparkContext.setLocalProperty("isNativeApplicable", null)
      spark.sparkContext.setLocalProperty("nativeFormat", null)
      spark.sparkContext.setLocalProperty("staticPartitionWriteOnly", null)
    }
  }
}
