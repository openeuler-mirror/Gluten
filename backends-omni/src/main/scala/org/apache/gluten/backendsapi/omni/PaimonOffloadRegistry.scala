/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.backendsapi.omni

import org.apache.gluten.extension.columnar.offload.OffloadSingleNode
import org.apache.gluten.extension.injector.GlutenInjector.LegacyInjector

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

import scala.util.Try

object PaimonOffloadRegistry {
  private val PaimonSparkSessionExtension = "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions"
  private val PaimonSparkCatalog = "org.apache.paimon.spark.SparkCatalog"
  private val SparkCatalogConfKey = "spark.sql.catalog.paimon"

  private lazy val paimonRuntimeAvailable: Boolean = isPaimonSessionConfigured()

  def offloads: Seq[OffloadSingleNode] = {
    if (paimonRuntimeAvailable) {
      loadScanOffloads() ++ loadWriteOffloads()
    } else {
      Seq.empty
    }
  }

  def injectPreTransformRules(injector: LegacyInjector): Unit = {
    if (!paimonRuntimeAvailable) {
      return
    }
    loadScanPreRule().foreach { rule =>
      injector.injectPreTransform(_ => rule)
    }
  }

  def pushDownFilterToScan(plan: SparkPlan): Option[SparkPlan] = {
    if (!paimonRuntimeAvailable) {
      return None
    }
    try {
      val clazz = Class.forName("org.apache.gluten.extension.PushDownFilterToOmniPaimonScan$")
      val module = clazz.getField("MODULE$").get(null)
      val method = clazz.getMethod("tryPushDown", classOf[SparkPlan])
      method.invoke(module, plan).asInstanceOf[Option[SparkPlan]]
    } catch {
      case _: ClassNotFoundException | _: NoSuchFieldException | _: NoSuchMethodException =>
        None
    }
  }

  private def readSparkConf(key: String, default: String = ""): String = {
    Try(SparkContext.getOrCreate().getConf.getOption(key)).toOption.flatten.getOrElse(default)
  }

  private def isPaimonSessionConfigured(): Boolean = {
    val extensionsKey = StaticSQLConf.SPARK_SESSION_EXTENSIONS.key
    val sparkConfExtensions = readSparkConf(extensionsKey)
    val sqlConfExtensions = Try(SQLConf.get.getConfString(extensionsKey, "")).getOrElse("")
    val extensions = if (sparkConfExtensions.nonEmpty) sparkConfExtensions else sqlConfExtensions
    val extensionList = extensions.split(",").map(_.trim).filter(_.nonEmpty)
    val extensionsConfigured = extensionList.contains(PaimonSparkSessionExtension)

    val sparkConfCatalog = readSparkConf(SparkCatalogConfKey)
    val sqlConfCatalog = Try(SQLConf.get.getConfString(SparkCatalogConfKey, "")).getOrElse("")
    val catalog = if (sparkConfCatalog.nonEmpty) sparkConfCatalog else sqlConfCatalog
    val catalogConfigured = catalog == PaimonSparkCatalog

    extensionsConfigured && catalogConfigured
  }

  private def loadScanOffloads(): Seq[OffloadSingleNode] = {
    try {
      val clazz = Class.forName("org.apache.gluten.execution.OffloadPaimonScan")
      val ctor = clazz.getConstructor()
      Seq(ctor.newInstance().asInstanceOf[OffloadSingleNode])
    } catch {
      case _: ClassNotFoundException | _: NoSuchMethodException =>
        Seq.empty
    }
  }

  private def loadScanPreRule(): Option[Rule[SparkPlan]] = {
    try {
      val clazz =
        Class.forName("org.apache.gluten.extension.columnar.offload.OffloadOmniPaimonScanPreRule$")
      Class.forName("org.apache.gluten.execution.PaimonScanTransformer")
      val module = clazz.getField("MODULE$").get(null)
      val method = clazz.getMethod("apply")
      Some(method.invoke(module).asInstanceOf[Rule[SparkPlan]])
    } catch {
      case _: ClassNotFoundException | _: NoSuchFieldException | _: NoSuchMethodException =>
        None
    }
  }

  private def loadWriteOffloads(): Seq[OffloadSingleNode] = {
    try {
      val clazz = Class.forName("org.apache.gluten.extension.columnar.offload.OffloadPaimonWrite$")
      val module = clazz.getField("MODULE$").get(null)
      val method = clazz.getMethod("offloads")
      method.invoke(module).asInstanceOf[Seq[OffloadSingleNode]]
    } catch {
      case _: ClassNotFoundException | _: NoSuchFieldException | _: NoSuchMethodException =>
        Seq.empty
    }
  }
}
