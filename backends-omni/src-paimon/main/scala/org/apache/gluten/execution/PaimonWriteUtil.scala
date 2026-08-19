/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.execution

import org.apache.spark.sql.connector.write.{BatchWrite, Write}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.ExecutedCommandExec

import scala.collection.JavaConverters._
import scala.util.Try

object PaimonWriteUtil {
  private val AppendDataExecV1ClassName =
    "org.apache.spark.sql.execution.datasources.v2.AppendDataExecV1"
  private val PaimonDynamicPartitionOverwriteCommandName =
    "PaimonDynamicPartitionOverwriteCommand"

  sealed trait NativeWriteMode
  case object BucketUnawareAppend extends NativeWriteMode
  case object FixedBucketAppend extends NativeWriteMode
  case object FixedBucketPrimaryKeyUpsert extends NativeWriteMode

  def supportsWrite(write: Write): Boolean = {
    isPaimonClass(write) ||
      tableFromWrite(write).isDefined ||
      Try(write.toBatch).toOption.exists(batch => isPaimonClass(batch) || tableFromBatchWrite(batch).isDefined)
  }

  def supportsBatchWrite(batchWrite: BatchWrite): Boolean = {
    isPaimonClass(batchWrite) || tableFromBatchWrite(batchWrite).isDefined
  }

  def supportsNativeColumnarWrite(write: Write): Boolean = {
    tableFromWrite(write).flatMap(nativeWriteMode).isDefined
  }

  def supportsNativeColumnarTable(table: org.apache.paimon.table.FileStoreTable): Boolean = {
    nativeWriteMode(table).isDefined
  }

  def nativeWriteMode(table: org.apache.paimon.table.FileStoreTable): Option[NativeWriteMode] = {
    if (isAppendOnly(table)) {
      if (isBucketUnaware(table)) {
        Some(BucketUnawareAppend)
      } else if (isFixedBucket(table)) {
        Some(FixedBucketAppend)
      } else {
        None
      }
    } else if (isFixedBucket(table)) {
      Some(FixedBucketPrimaryKeyUpsert)
    } else {
      None
    }
  }

  def isPaimonAppendDataExecV1(plan: SparkPlan): Boolean = {
    plan.getClass.getName == AppendDataExecV1ClassName &&
      tableFromPlan(plan).exists(supportsNativeColumnarTable)
  }

  def isPaimonDynamicPartitionOverwriteCommand(plan: SparkPlan): Boolean = {
    dynamicPartitionOverwriteCommand(plan).exists { command =>
      tableFromAny(command).exists(supportsNativeColumnarTable)
    }
  }

  def dynamicPartitionOverwriteCommand(plan: SparkPlan): Option[AnyRef] = {
    plan match {
      case command if isPaimonDynamicPartitionOverwriteCommandObject(command) =>
        Some(command.asInstanceOf[AnyRef])
      case exec: ExecutedCommandExec if isPaimonDynamicPartitionOverwriteCommandObject(exec.cmd) =>
        Some(exec.cmd)
      case _ => None
    }
  }

  def queryFromPlan(plan: SparkPlan): Option[SparkPlan] = {
    val candidates =
      plan.children.collect { case child: SparkPlan => child } ++
        findSparkPlanMembers(plan, Seq("query", "child", "plan", "inputPlan")) ++
        productValues(plan).collect { case child: SparkPlan => child }.toSeq ++
        findSparkPlanFieldsByType(plan) ++
        findSparkPlanMethodsByType(plan)

    candidates
      .filterNot(_ eq plan)
      .distinct
      .sortBy { candidate =>
        val schemaSize = Try(candidate.schema.size).getOrElse(0)
        if (schemaSize > 0) 0 else 1
      }
      .headOption
  }

  def logicalQueryFromPlan(plan: SparkPlan): Option[LogicalPlan] = {
    logicalQueryFromAny(plan)
  }

  def logicalQueryFromAny(value: Any): Option[LogicalPlan] = {
    val candidates =
      findLogicalPlanMembers(value, Seq("query", "child", "plan", "inputPlan")) ++
        productValues(value).collect { case child: LogicalPlan => child }.toSeq ++
        findLogicalPlanFieldsByType(value) ++
        findLogicalPlanMethodsByType(value)
    val distinct = candidates.distinct
    distinct
      .filterNot(candidate => isSameRef(candidate, value))
      .filterNot(candidate => isTargetTableRelation(candidate))
      .filterNot(candidate => isPaimonDynamicPartitionOverwriteCommandObject(candidate))
      .headOption
      .orElse(distinct.filterNot(candidate => isSameRef(candidate, value)).headOption)
  }

  def tableFromPlan(plan: SparkPlan): Option[org.apache.paimon.table.FileStoreTable] = {
    dynamicPartitionOverwriteCommand(plan).flatMap(tableFromAny).orElse {
      findFieldValue(plan, "write").flatMap(tableFromAny)
      .orElse(findFieldValue(plan, "table").flatMap(tableFromAny))
      .orElse(productValues(plan).flatMap(tableFromAny).toSeq.headOption)
      .orElse(findPaimonTableField(plan))
    }
  }

  def tableFromWrite(write: Write): Option[org.apache.paimon.table.FileStoreTable] = {
    tableFromAny(write).orElse {
      Try(write.toBatch).toOption.flatMap(tableFromBatchWrite)
    }
  }

  def tableFromBatchWrite(batchWrite: BatchWrite): Option[org.apache.paimon.table.FileStoreTable] = {
    tableFromAny(batchWrite)
  }

  def tableFromAny(value: Any): Option[org.apache.paimon.table.FileStoreTable] = {
    value match {
      case t: org.apache.paimon.table.FileStoreTable => Some(t)
      case other =>
        findFieldValue(other, "table").flatMap(tableFromAny)
          .orElse(findFieldValue(other, "this$0").flatMap(tableFromAny))
          .orElse(findFieldValue(other, "write").flatMap(tableFromAny))
          .orElse(findFieldValue(other, "batchWrite").flatMap(tableFromAny))
          .orElse(findPaimonTableField(other))
    }
  }

  def fileFormat(table: org.apache.paimon.table.FileStoreTable): String = {
    Option(table.options().get("file.format")).filter(_.nonEmpty).getOrElse("parquet")
  }

  def numBuckets(table: org.apache.paimon.table.FileStoreTable): Int = table.schema().numBuckets()

  def tableFieldNames(table: org.apache.paimon.table.FileStoreTable): Seq[String] = {
    table.rowType().getFields.asScala.map(_.name()).toSeq
  }

  def bucketKeys(table: org.apache.paimon.table.FileStoreTable): Seq[String] = {
    val fromOption = Option(table.options().get("bucket-key"))
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq)
      .getOrElse(Seq.empty)
    if (fromOption.nonEmpty) {
      fromOption
    } else if (!table.primaryKeys().isEmpty) {
      table.primaryKeys().asScala.map(_.toString).toSeq
    } else {
      tableFieldNames(table)
    }
  }

  def hiddenBucketColumnIndex(
      table: org.apache.paimon.table.FileStoreTable,
      schema: org.apache.spark.sql.types.StructType): Int = {
    val fieldCount = tableFieldNames(table).size
    if (isFixedBucket(table) && schema.fields.length > fieldCount) fieldCount else -1
  }

  private def isBucketUnaware(table: org.apache.paimon.table.FileStoreTable): Boolean = {
    table.bucketMode().toString == "BUCKET_UNAWARE" || table.schema().numBuckets() == -1
  }

  private def isFixedBucket(table: org.apache.paimon.table.FileStoreTable): Boolean = {
    table.schema().numBuckets() > 0
  }

  private def isAppendOnly(table: org.apache.paimon.table.FileStoreTable): Boolean = {
    table.primaryKeys().isEmpty
  }

  def refreshCache(plan: SparkPlan): Unit = {
    findFieldValue(plan, "refreshCache").foreach {
      case f: Function0[_] => f()
      case other =>
        Try {
          val method = other.getClass.getMethod("apply")
          method.setAccessible(true)
          method.invoke(other)
        }
    }
  }

  def describePlanMembers(plan: SparkPlan): String = {
    val fields = declaredFields(plan).map { field =>
      field.getName + ":" + field.getType.getName
    }.mkString("[", ",", "]")
    val methods = declaredMethods(plan)
      .filter(_.getParameterCount == 0)
      .map(method => method.getName + ":" + method.getReturnType.getName)
      .mkString("[", ",", "]")
    val products = productValues(plan).map(value => className(value)).mkString("[", ",", "]")
    s"class=${className(plan)}, children=${plan.children.map(className).mkString("[", ",", "]")}, " +
      s"fields=$fields, methods=$methods, products=$products"
  }

  private def findSparkPlanMembers(value: Any, names: Seq[String]): Seq[SparkPlan] = {
    names.iterator
      .flatMap(name => findFieldValue(value, name).orElse(invokeNoArg(value, name)))
      .collect { case plan: SparkPlan => plan }
      .toSeq
  }

  private def findSparkPlanFieldsByType(value: Any): Seq[SparkPlan] = {
    declaredFields(value).iterator.flatMap { field =>
      try {
        field.setAccessible(true)
        Option(field.get(value))
      } catch {
        case _: SecurityException => None
        case _: IllegalAccessException => None
      }
    }.collect { case plan: SparkPlan => plan }.toSeq
  }

  private def findSparkPlanMethodsByType(value: Any): Seq[SparkPlan] = {
    declaredMethods(value).iterator
      .filter(method => method.getParameterCount == 0 && classOf[SparkPlan].isAssignableFrom(method.getReturnType))
      .flatMap { method =>
        try {
          method.setAccessible(true)
          Option(method.invoke(value))
        } catch {
          case _: SecurityException => None
          case _: IllegalAccessException => None
        }
      }
      .collect { case plan: SparkPlan => plan }
      .toSeq
  }

  private def findLogicalPlanMembers(value: Any, names: Seq[String]): Seq[LogicalPlan] = {
    names.iterator
      .flatMap(name => findFieldValue(value, name).orElse(invokeNoArg(value, name)))
      .collect { case plan: LogicalPlan => plan }
      .toSeq
  }

  private def findLogicalPlanFieldsByType(value: Any): Seq[LogicalPlan] = {
    declaredFields(value).iterator.flatMap { field =>
      try {
        field.setAccessible(true)
        Option(field.get(value))
      } catch {
        case _: SecurityException => None
        case _: IllegalAccessException => None
      }
    }.collect { case plan: LogicalPlan => plan }.toSeq
  }

  private def findLogicalPlanMethodsByType(value: Any): Seq[LogicalPlan] = {
    declaredMethods(value).iterator
      .filter(method => method.getParameterCount == 0 && classOf[LogicalPlan].isAssignableFrom(method.getReturnType))
      .flatMap { method =>
        try {
          method.setAccessible(true)
          Option(method.invoke(value))
        } catch {
          case _: SecurityException => None
          case _: IllegalAccessException => None
        }
      }
      .collect { case plan: LogicalPlan => plan }
      .toSeq
  }

  private def isPaimonClass(value: Any): Boolean = {
    value != null && value.getClass.getName.toLowerCase.contains("paimon")
  }

  private def isPaimonDynamicPartitionOverwriteCommandObject(value: Any): Boolean = {
    value != null &&
      (value.getClass.getName.endsWith(PaimonDynamicPartitionOverwriteCommandName) ||
        invokeNoArg(value, "nodeName")
          .exists(_.toString == PaimonDynamicPartitionOverwriteCommandName))
  }

  private def isSameRef(left: Any, right: Any): Boolean = {
    left.asInstanceOf[AnyRef] eq right.asInstanceOf[AnyRef]
  }

  private def isTargetTableRelation(plan: LogicalPlan): Boolean = {
    val name = plan.getClass.getName
    name.endsWith("DataSourceV2Relation") || name.endsWith("DataSourceV2ScanRelation")
  }

  private def findPaimonTableField(value: Any): Option[org.apache.paimon.table.FileStoreTable] = {
    declaredFields(value).iterator.flatMap { field =>
      try {
        field.setAccessible(true)
        Option(field.get(value))
      } catch {
        case _: SecurityException => None
        case _: IllegalAccessException => None
      }
    }.collectFirst {
      case table: org.apache.paimon.table.FileStoreTable => table
    }
  }

  private def productValues(value: Any): Iterator[Any] = {
    value match {
      case p: Product => p.productIterator
      case _ => Iterator.empty
    }
  }

  private def invokeNoArg(value: Any, name: String): Option[AnyRef] = {
    if (value == null) {
      return None
    }
    try {
      val method = declaredMethods(value).find(m => m.getName == name && m.getParameterCount == 0)
        .getOrElse(value.getClass.getMethod(name))
      method.setAccessible(true)
      Option(method.invoke(value).asInstanceOf[AnyRef])
    } catch {
      case _: NoSuchMethodException => None
      case _: SecurityException => None
    }
  }

  private def declaredFields(value: Any): Seq[java.lang.reflect.Field] = {
    if (value == null) {
      return Nil
    }
    val buffer = scala.collection.mutable.ArrayBuffer[java.lang.reflect.Field]()
    var cls: Class[_] = value.getClass
    while (cls != null) {
      buffer ++= cls.getDeclaredFields
      cls = cls.getSuperclass
    }
    buffer
  }

  private def declaredMethods(value: Any): Seq[java.lang.reflect.Method] = {
    if (value == null) {
      return Nil
    }
    val buffer = scala.collection.mutable.ArrayBuffer[java.lang.reflect.Method]()
    var cls: Class[_] = value.getClass
    while (cls != null) {
      buffer ++= cls.getDeclaredMethods
      cls = cls.getSuperclass
    }
    buffer
  }

  private def className(value: Any): String = {
    if (value == null) "null" else value.getClass.getName
  }

  private def findFieldValue(value: Any, name: String): Option[AnyRef] = {
    if (value == null) {
      return None
    }
    var cls: Class[_] = value.getClass
    while (cls != null) {
      try {
        val field = cls.getDeclaredField(name)
        field.setAccessible(true)
        return Option(field.get(value).asInstanceOf[AnyRef])
      } catch {
        case _: NoSuchFieldException => cls = cls.getSuperclass
      }
    }
    None
  }
}
