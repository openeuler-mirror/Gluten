package org.apache.spark.sql.hive.execution

import java.util.Locale

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.ErrorMsg
import org.apache.hadoop.hive.ql.plan.{FileSinkDesc, TableDesc}

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, ExternalCatalogUtils}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.hive.client.HiveClientImpl

trait V1WritesHiveUtils {
  def getPartitionSpec(partition: Map[String, Option[String]]): Map[String, String] = {
    partition.map {
      case (key, Some(null)) => key -> ExternalCatalogUtils.DEFAULT_PARTITION_NAME
      case (key, Some(value)) => key -> value
      case (key, None) => key -> ""
    }
  }

  def getDynamicPartitionColumns(
      table: CatalogTable,
      partition: Map[String, Option[String]],
      query: LogicalPlan): Seq[Attribute] = {
    val numDynamicPartitions = partition.values.count(_.isEmpty)
    val numStaticPartitions = partition.values.count(_.nonEmpty)
    val partitionSpec = getPartitionSpec(partition)

    val hiveQlTable = HiveClientImpl.toHiveTable(table)
    val tableDesc = new TableDesc(
      hiveQlTable.getInputFormatClass,
      hiveQlTable.getOutputFormatClass,
      hiveQlTable.getMetadata)

    val partitionColumns = tableDesc.getProperties.getProperty("partition_columns")
    val partitionColumnNames = Option(partitionColumns).map(_.split("/")).getOrElse(Array.empty)

    if (partitionColumnNames.toSet != partition.keySet) {
      throw QueryExecutionErrors.requestedPartitionsMismatchTablePartitionsError(table, partition)
    }

    val sessionState = SparkSession.active.sessionState
    val hadoopConf = sessionState.newHadoopConf()

    if (numDynamicPartitions > 0) {
      if (!hadoopConf.get("hive.exec.dynamic.partition", "true").toBoolean) {
        throw new SparkException(ErrorMsg.DYNAMIC_PARTITION_DISABLED.getMsg)
      }
      if (numStaticPartitions == 0 &&
        hadoopConf.get("hive.exec.dynamic.partition.mode", "strict").equalsIgnoreCase("strict")) {
        throw new SparkException(ErrorMsg.DYNAMIC_PARTITION_STRICT_MODE.getMsg)
      }
      val isDynamic = partitionColumnNames.map(partitionSpec(_).isEmpty)
      if (isDynamic.init.zip(isDynamic.tail).contains((true, false))) {
        throw new AnalysisException(ErrorMsg.PARTITION_DYN_STA_ORDER.getMsg)
      }
    }

    partitionColumnNames.takeRight(numDynamicPartitions).map { name =>
      val attr = query.resolve(name :: Nil, sessionState.analyzer.resolver).getOrElse {
        throw QueryCompilationErrors.cannotResolveAttributeError(
          name, query.output.map(_.name).mkString(", "))
      }.asInstanceOf[Attribute]
      attr.withName(name.toLowerCase(Locale.ROOT))
    }
  }

  def getOptionsWithHiveBucketWrite(bucketSpec: Option[BucketSpec]): Map[String, String] = {
    bucketSpec
      .map(_ => Map("__hive_compatible_bucketed_table_insertion__" -> "true"))
      .getOrElse(Map.empty)
  }

  def setupHadoopConfForCompression(
      fileSinkConf: FileSinkDesc,
      hadoopConf: Configuration,
      sparkSession: SparkSession): Unit = {
    val isCompressed =
      fileSinkConf.getTableInfo.getOutputFileFormatClassName.toLowerCase(Locale.ROOT) match {
        case formatName if formatName.endsWith("orcoutputformat") => false
        case _ => hadoopConf.get("hive.exec.compress.output", "false").toBoolean
      }

    if (isCompressed) {
      hadoopConf.set("mapreduce.output.fileoutputformat.compress", "true")
      fileSinkConf.setCompressed(true)
      fileSinkConf.setCompressCodec(hadoopConf.get("mapreduce.output.fileoutputformat.compress.codec"))
      fileSinkConf.setCompressType(hadoopConf.get("mapreduce.output.fileoutputformat.compress.type"))
    } else {
      HiveOptions.getHiveWriteCompression(fileSinkConf.getTableInfo, sparkSession.sessionState.conf)
        .foreach { case (compression, codec) => hadoopConf.set(compression, codec) }
    }
  }
}
