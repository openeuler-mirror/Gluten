/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.extension

import org.apache.spark.sql.execution.SparkPlan

/**
 * Dispatches Omni scan filter pushdown to optional format-specific handlers.
 *
 * Handlers are loaded reflectively so Delta/Hudi-specific pushdown code can live in optional source
 * sets without adding hard dependencies to the default Omni backend build.
 */
object PushDownFilterToOmniScanRegistry {
  private val handlers = Seq(
    "org.apache.gluten.backendsapi.omni.DeltaOffloadRegistry$",
    "org.apache.gluten.backendsapi.omni.HudiOffloadRegistry$",
    "org.apache.gluten.backendsapi.omni.PaimonOffloadRegistry$"
  )

  def pushDown(plan: SparkPlan): Option[SparkPlan] = {
    handlers.iterator
      .flatMap(loadHandler)
      .flatMap(_(plan))
      .toSeq
      .headOption
  }

  private def loadHandler(className: String): Option[SparkPlan => Option[SparkPlan]] = {
    try {
      val clazz: Class[_] = try {
        Class.forName(className)
      } catch {
        case _: ClassNotFoundException if !className.endsWith("$") =>
          Class.forName(className + "$")
      }
      val module = clazz.getField("MODULE$").get(null)
      val method = clazz.getMethod("pushDownFilterToScan", classOf[SparkPlan])
      Some((plan: SparkPlan) => method.invoke(module, plan).asInstanceOf[Option[SparkPlan]])
    } catch {
      case _: ClassNotFoundException | _: NoSuchFieldException | _: NoSuchMethodException =>
        None
    }
  }
}
