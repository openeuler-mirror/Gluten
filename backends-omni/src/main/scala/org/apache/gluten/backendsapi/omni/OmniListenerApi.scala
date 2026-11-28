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
package org.apache.gluten.backendsapi.omni

import org.apache.commons.lang3.StringUtils
import org.apache.gluten.backendsapi.ListenerApi
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.expression.UDFMappings
import org.apache.gluten.init.OmniNativeBackendInitializer
import org.apache.spark.memory.OmniBroadcastStorageTarget
import org.apache.gluten.jni.JniLibLoader
import org.apache.gluten.udf.UdfJniWrapper
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expression.UDFResolver
import org.apache.spark.sql.internal.GlutenConfigUtil
import org.apache.spark.util.{SparkDirectoryUtil, SparkResourceUtil, SparkShutdownManagerUtil}
import org.apache.gluten.datasources.OmniOrcFormatWriterInjects
import org.apache.gluten.datasources.parquet.OmniParquetFormatWriterInjects
import org.apache.gluten.datasources.text.OmniTextFormatWriterInjects
import org.apache.gluten.execution.datasource.GlutenFormatFactory
import org.apache.gluten.vectorized.OmniNativePlanEvaluator.destroyNative
import org.apache.spark.sql.execution.datasources.OmniGlutenWriterColumnarRules
import org.apache.spark.sql.execution.omni.OmniBroadcastCacheRpc
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.control.NonFatal
import scala.util.matching.Regex

class OmniListenerApi extends ListenerApi with Logging {

  import OmniListenerApi._

  override def onDriverStart(sc: SparkContext, pc: PluginContext): Unit = {
    val conf = pc.conf()
    if (driverInitialized.compareAndSet(false, true)) {
      SparkDirectoryUtil.init(conf)
      UDFResolver.resolveUdfConf(conf, isDriver = true)
      initialize(conf, isDriver = true)
      UdfJniWrapper.registerFunctionSignatures()
    } else {
      logInfo(
        "Skip rerunning static initializers since they are only supposed to run once." +
          " You see this message probably because you are creating a new SparkSession.")
    }
    // The coordinator follows each SparkContext even when native/UDF static
    // initialization was already completed in this JVM.
    if (conf.getBoolean("spark.gluten.omni.buildHashTableOncePerExecutor.enabled", false)) {
      OmniBroadcastCacheRpc.startDriver(sc)
    }
  }

  override def onExecutorStart(pc: PluginContext): Unit = {
    val conf = pc.conf()

    val firstInitialization = executorInitialized.compareAndSet(false, true)
    if (!firstInitialization) {
      logInfo(
        "Skip rerunning static initializers since they are only supposed to run once." +
          " You see this message probably because you are creating a new SparkSession.")
    }
    if (firstInitialization && inLocalMode(conf)) {
      logInfo(
        "Gluten is running with Spark local mode. Skip running static initializer for executor.")
    } else if (firstInitialization) {
      SparkDirectoryUtil.init(conf)
      // Executors run native UDAF code, so they must load the UDF/UDAF configuration.
      UDFResolver.resolveUdfConf(conf, isDriver = false)
      initialize(conf, isDriver = false)
    }
    // Cache storage and RPC endpoint are SparkContext-scoped, also in local mode.
    startBroadcastCache(conf, pc)
  }

  override def onDriverMessage(message: AnyRef): Option[AnyRef] = {
    OmniBroadcastCacheRpc.onDriverMessage(message)
  }

  private def startBroadcastCache(conf: SparkConf, pc: PluginContext): Unit = {
    if (!conf.getBoolean("spark.gluten.omni.buildHashTableOncePerExecutor.enabled", false)) {
      return
    }
    initializeBroadcastCache(conf)
    try {
      OmniBroadcastCacheRpc.startExecutor(pc)
    } catch {
      case NonFatal(e) =>
        OmniNativeBackendInitializer.forBackend(OmniBackend.BACKEND_NAME)
          .shutdownBroadcastCache()
        throw e
    }
  }

  private def initializeBroadcastCache(conf: SparkConf): Unit = {
    if (!conf.getBoolean("spark.gluten.omni.buildHashTableOncePerExecutor.enabled", false)) {
      return
    }
    val maxBytes = conf.getSizeAsBytes(
      "spark.gluten.omni.broadcastCache.maxBytes",
      conf.get("spark.memory.offHeap.size", "1g"))
    val target = OmniBroadcastStorageTarget.forCurrentExecutor(maxBytes)
    OmniNativeBackendInitializer.forBackend(OmniBackend.BACKEND_NAME)
      .initializeBroadcastCache(target)
  }

  override def onExecutorShutdown(): Unit = {
    OmniNativeBackendInitializer.forBackend(OmniBackend.BACKEND_NAME)
      .shutdownBroadcastCache()
    OmniBroadcastCacheRpc.stopExecutor()
    OmniBroadcastStorageTarget.clearCurrent()
  }

  override def onDriverShutdown(): Unit = {
    // The same JVM owns both plugins in local mode; shutdown is idempotent.
    OmniNativeBackendInitializer.forBackend(OmniBackend.BACKEND_NAME)
      .shutdownBroadcastCache()
    OmniBroadcastCacheRpc.stopExecutor()
    OmniBroadcastStorageTarget.clearCurrent()
    OmniBroadcastCacheRpc.stopDriver()
  }

  private def replaceEnvVarsWithDefaults(input: String, defaultForMissing: String = ""): String = {
    val pattern: Regex = """\$\{([^}]+)\}""".r

    pattern.replaceAllIn(input, { m =>
      val envVarName = m.group(1)
      sys.env.getOrElse(envVarName, defaultForMissing)
    })
  }

  private def initialize(conf: SparkConf, isDriver: Boolean): Unit = {
    // Initial native backend with configurations.
    val parsed = GlutenConfigUtil.parseConfig(conf.getAll.toMap)

    val libPath = conf.get(GlutenConfig.GLUTEN_LIB_PATH, StringUtils.EMPTY)
    if (StringUtils.isBlank(libPath)) {
      throw new IllegalArgumentException(
        "Please set spark.gluten.sql.columnar.libpath to enable omni backend")
    }

    // Load supported hive/python/scala udfs
    UDFMappings.loadFromSparkConf(conf)

    if (isDriver) {
      JniLibLoader.loadFromPath(libPath)
    } else {
      val executorLibPath = conf.get(GlutenConfig.GLUTEN_EXECUTOR_LIB_PATH, libPath)
      val actualLibPath = replaceEnvVarsWithDefaults(executorLibPath)
      JniLibLoader.loadFromPath(actualLibPath)
    }
    // Inject backend-specific implementations to override spark classes.
    GlutenFormatFactory.register(
      new OmniOrcFormatWriterInjects(),
      new OmniParquetFormatWriterInjects(),
      new OmniTextFormatWriterInjects(),
      new org.apache.gluten.datasources.text.OmniCsvFormatWriterInjects())
    GlutenFormatFactory.injectPostRuleFactory(
      session => OmniGlutenWriterColumnarRules.NativeWritePostRule(session))
    OmniNativeBackendInitializer.forBackend(OmniBackend.BACKEND_NAME).initialize(parsed)
  }

  if (!shutdownHookAdded) {
    SparkShutdownManagerUtil.addHookForLibUnloading(() => destroyNative())
    shutdownHookAdded = true
  }
}

object OmniListenerApi {
  // TODO: Implement graceful shutdown and remove these flags.
  //  As spark conf may change when active Spark session is recreated.
  private val driverInitialized: AtomicBoolean = new AtomicBoolean(false)
  private val executorInitialized: AtomicBoolean = new AtomicBoolean(false)
  private var shutdownHookAdded: Boolean = false

  private def inLocalMode(conf: SparkConf): Boolean = {
    SparkResourceUtil.isLocalMaster(conf)
  }
}
