/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.connector.write.{
  ColumnarBatchDataWriterFactory,
  ColumnarStreamingDataWriterFactory,
  OmniPaimonDataWriteFactory
}
import org.apache.gluten.extension.ValidationResult

import org.apache.spark.sql.types.StructType

trait PaimonWriteExec extends ColumnarV2TableWriteExec {
  override def nodeName: String = "OmniPaimonWriteExec"

  override def doValidateInternal(): ValidationResult = {
    if (!PaimonWriteUtil.supportsWrite(write)) {
      return ValidationResult.failed(s"Not support the write ${write.getClass.getSimpleName}")
    }
    if (!PaimonWriteUtil.supportsNativeColumnarWrite(write)) {
      return ValidationResult.failed(
        "Paimon native columnar write supports append-only tables and fixed-bucket primary-key inserts")
    }
    BackendsApiManager.getValidatorApiInstance.doSchemaValidate(query.schema) match {
      case Some(reason) => ValidationResult.failed(reason)
      case None => ValidationResult.succeeded
    }
  }
}

abstract class AbstractPaimonWriteExec extends PaimonWriteExec {
  private def createOmniPaimonDataWriteFactory(): OmniPaimonDataWriteFactory = {
    val table = PaimonWriteUtil.tableFromWrite(write).getOrElse {
      throw new IllegalStateException("Cannot extract Paimon table from write: " + write)
    }
    OmniPaimonDataWriteFactory(
      query.schema,
      table,
      PaimonWriteUtil.fileFormat(table),
      java.util.UUID.randomUUID().toString)
  }

  override protected def createBatchWriterFactory(
      schema: StructType): ColumnarBatchDataWriterFactory = {
    createOmniPaimonDataWriteFactory()
  }

  override protected def createStreamingWriterFactory(
      schema: StructType): ColumnarStreamingDataWriterFactory = {
    createOmniPaimonDataWriteFactory()
  }
}
