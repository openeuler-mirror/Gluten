/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.backendsapi.omni

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.spark.sql.connector.write.WriterCommitMessage

import org.apache.gluten.connector.write.HudiFileInfoJson

/**
 * Builds Hudi WriterCommitMessage from columnar write result (file info JSON array).
 * Uses reflection to instantiate Hudi's HoodieWriterCommitMessage so we stay compatible
 * across Hudi versions.
 *
 * @since 2026
 */

object HudiCommitMessageBuilder {

  private val mapper = new ObjectMapper()

  /**
   * Parses fileInfoJsonArray (each element is HudiFileInfoJson JSON) and builds
   * WriterCommitMessage that Hudi's BatchWrite.commit() accepts.
   */
  def buildCommitMessage(fileInfoJsonArray: Array[String]): WriterCommitMessage = {
    if (fileInfoJsonArray == null || fileInfoJsonArray.isEmpty) {
      return buildViaReflection(java.util.Collections.emptyList[Object]())
    }
    val writeStatusList = new java.util.ArrayList[Object]()
    for (json <- fileInfoJsonArray) {
      val info = mapper.readValue(json, classOf[HudiFileInfoJson])
      val writeStatus = createWriteStatusFromFileInfo(info)
      if (writeStatus != null) {
        writeStatusList.add(writeStatus)
      }
    }
    buildViaReflection(writeStatusList)
  }

  private def createWriteStatusFromFileInfo(info: HudiFileInfoJson): Object = {
    try {
      // Hudi: WriteStatus with HoodieWriteStat (path, record count, file size)
      val writeStatusClass = Class.forName("org.apache.hudi.client.WriteStatus")
      val writeStatus: Object =
        writeStatusClass.getConstructor().newInstance().asInstanceOf[Object]
      val setStat = writeStatusClass.getMethod("setStat", Class.forName("org.apache.hudi.client.HoodieWriteStat"))
      val hoodieWriteStatClass = Class.forName("org.apache.hudi.client.HoodieWriteStat")
      val stat: Object =
        hoodieWriteStatClass.getConstructor().newInstance().asInstanceOf[Object]
      hoodieWriteStatClass.getMethod("setPath", classOf[String]).invoke(stat, info.getPath)
      hoodieWriteStatClass.getMethod("setNumWrites", classOf[Long]).invoke(stat, java.lang.Long.valueOf(info.getRecordCount))
      hoodieWriteStatClass.getMethod("setFileSizeInBytes", classOf[Long]).invoke(stat, java.lang.Long.valueOf(info.getFileSizeInBytes))
      setStat.invoke(writeStatus, stat)
      writeStatus
    } catch {
      case _: Throwable => null
    }
  }

  private def buildViaReflection(writeStatusList: java.util.List[Object]): WriterCommitMessage = {
    try {
      val msgClass = Class.forName("org.apache.spark.sql.hudi.command.HoodieWriterCommitMessage")
      val ctor = msgClass.getConstructor(classOf[java.util.List[_]])
      ctor.newInstance(writeStatusList).asInstanceOf[WriterCommitMessage]
    } catch {
      case e: Throwable =>
        try {
          val msgClass = Class.forName("org.apache.hudi.internal.HoodieWriterCommitMessage")
          val ctor = msgClass.getConstructor(classOf[java.util.List[_]])
          ctor.newInstance(writeStatusList).asInstanceOf[WriterCommitMessage]
        } catch {
          case _: Throwable =>
            throw new IllegalStateException(
              "Failed to build Hudi WriterCommitMessage from columnar write result. " +
                "Hudi commit message type may have changed.", e)
        }
    }
  }
}
