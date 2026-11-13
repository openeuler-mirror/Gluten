package org.apache.gluten.extension.columnar.offload

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.logging.LogLevelUtil
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, DataWritingCommandExec}
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, OmniCreateDataSourceTableAsSelectCommand, OmniInsertIntoHadoopFsRelationCommand, WriteFilesExec}
import org.apache.spark.sql.hive.execution.{ InsertIntoHiveDirCommand, InsertIntoHiveTable, OmniInsertIntoHiveDirCommand, OmniInsertIntoHiveTable}

case class OffloadWrite() extends OffloadSingleNode with LogLevelUtil {
  override def offload(plan: SparkPlan): SparkPlan = {
    // This rule must be a no-op when native writers are disabled. In particular, do not create an
    // Omni command whose run() method delegates to OmniFileFormatWriter even if its native format
    // selection later returns None.
    if (!BackendsApiManager.getSettings.enableNativeWriteFiles()) {
      plan
    } else {
      plan match {
        case p if FallbackTags.nonEmpty(p) =>
          p
        case d@DataWritingCommandExec(cmd, child) if !child.isInstanceOf[WriteFilesExec] =>
          (cmd: LogicalPlan) match {
            case hive: InsertIntoHiveTable =>
              d.copy(OmniInsertIntoHiveTable.apply(
                hive.table,
                hive.partition,
                hive.query,
                hive.overwrite,
                hive.ifPartitionNotExists,
                hive.outputColumnNames
              ))
            case dsCtas: CreateDataSourceTableAsSelectCommand =>
              d.copy(OmniCreateDataSourceTableAsSelectCommand(
                dsCtas.table,
                dsCtas.mode,
                dsCtas.query,
                dsCtas.outputColumnNames
              ))
            case dir: InsertIntoHiveDirCommand =>
              d.copy(OmniInsertIntoHiveDirCommand(
                dir.isLocal,
                dir.storage,
                dir.query,
                dir.overwrite,
                dir.outputColumnNames
              ))
            // Bucketed datasource writes stay on the Omni command: OmniFileFormatWriter's
            // nativeWrap injects a `__bucket_value__` projection (HashPartitioning partition id)
            // that the native side evaluates per row, and the writer carries the bucket id into
            // the output file name suffix, matching internet_commercial's verified behaviour.
            // Dynamic partition writes (pure or mixed with static ones) also stay on the Omni
            // command: OmniFileFormatWriter injects the shim's registered Empty2Null expression
            // for empty-string-to-null partition semantics.
            case hadoop: InsertIntoHadoopFsRelationCommand =>
              d.copy(OmniInsertIntoHadoopFsRelationCommand(
                hadoop.outputPath,
                hadoop.staticPartitions,
                hadoop.ifPartitionNotExists,
                hadoop.partitionColumns,
                hadoop.bucketSpec,
                hadoop.fileFormat,
                hadoop.options,
                hadoop.query,
                hadoop.mode,
                hadoop.catalogTable,
                hadoop.fileIndex,
                hadoop.outputColumnNames
              ))
            case _ => offloadHiveCtas(d)
          }
        case d: DataWritingCommandExec => d
        case other => offloadHiveCtas(other)
      }
    }
  }

  private def offloadHiveCtas(plan: SparkPlan): SparkPlan =
    SparkShimLoader.getSparkShims.rewriteHiveCtas(plan) { (table, query, outputNames, overwrite) =>
      OmniInsertIntoHiveTable(
        table,
        table.partitionColumnNames.map(_ -> None).toMap,
        query,
        overwrite,
        ifPartitionNotExists = false,
        outputColumnNames = outputNames)
    }
}

object OffloadWrite {
  private[offload] def nativeWriterEnabled: Boolean =
    BackendsApiManager.getSettings.enableNativeWriteFiles()
}
