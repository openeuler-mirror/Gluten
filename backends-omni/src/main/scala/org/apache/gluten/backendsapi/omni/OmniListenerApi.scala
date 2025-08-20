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
import org.apache.gluten.init.OmniNativeBackendInitializer
import org.apache.gluten.jni.JniLibLoader
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.GlutenConfigUtil
import org.apache.spark.util.{SparkDirectoryUtil, SparkResourceUtil}

import java.util.concurrent.atomic.AtomicBoolean

class OmniListenerApi extends ListenerApi with Logging {

  import OmniListenerApi._
  override def onDriverStart(sc: SparkContext, pc: PluginContext): Unit = {
    // Static initializers for driver.
    if (!driverInitialized.compareAndSet(false, true)) {
      // Make sure we call the static initializers only once.
      logInfo(
        "Skip rerunning static initializers since they are only supposed to run once." +
          " You see this message probably because you are creating a new SparkSession.")
      return
    }
    val conf = pc.conf()
    SparkDirectoryUtil.init(conf)
    initialize(conf, isDriver = true)
  }

  override def onExecutorStart(pc: PluginContext): Unit = {
    val conf = pc.conf()

    // Static initializers for executor.
    if (!executorInitialized.compareAndSet(false, true)) {
      // Make sure we call the static initializers only once.
      logInfo(
        "Skip rerunning static initializers since they are only supposed to run once." +
          " You see this message probably because you are creating a new SparkSession.")
      return
    }
    if (inLocalMode(conf)) {
      // Don't do static initializations from executor side in local mode.
      // Driver already did that.
      logInfo(
        "Gluten is running with Spark local mode. Skip running static initializer for executor.")
      return
    }

    SparkDirectoryUtil.init(conf)
    initialize(conf, isDriver = false)
  }
  private def initialize(conf: SparkConf, isDriver: Boolean): Unit = {
    // Initial native backend with configurations.
    val parsed = GlutenConfigUtil.parseConfig(conf.getAll.toMap)

    OmniNativeBackendInitializer.forBackend(OmniBackend.BACKEND_NAME).initialize(parsed)

    val libPath = conf.get(GlutenConfig.GLUTEN_LIB_PATH, StringUtils.EMPTY)
    if (StringUtils.isBlank(libPath)) {
      throw new IllegalArgumentException(
        "Please set spark.gluten.sql.columnar.libpath to enable omni backend")
    }
    JniLibLoader.loadFromPath(libPath, true)
  }
}

object OmniListenerApi {
  // TODO: Implement graceful shutdown and remove these flags.
  //  As spark conf may change when active Spark session is recreated.
  private val driverInitialized: AtomicBoolean = new AtomicBoolean(false)
  private val executorInitialized: AtomicBoolean = new AtomicBoolean(false)

  private def inLocalMode(conf: SparkConf): Boolean = {
    SparkResourceUtil.isLocalMaster(conf)
  }
}
