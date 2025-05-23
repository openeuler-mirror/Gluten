/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.apache.gluten.runtime

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.utils.ConfigUtil

import org.apache.spark.sql.internal.{GlutenConfigUtil, SQLConf}
import org.apache.spark.task.TaskResource

import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicBoolean

trait OmniRuntime {
  def getHandle(): Long
}

object OmniRuntime {
  private[runtime] def apply(backendName: String, name: String): OmniRuntime with TaskResource = {
    new RuntimeImpl(backendName, name)
  }

  private class RuntimeImpl(backendName: String, name: String) extends OmniRuntime with TaskResource {
    private val LOGGER = LoggerFactory.getLogger(classOf[OmniRuntime])

    private val handle = OmniRuntimeJniWrapper.createRuntime(
      backendName,
      -1,
      ConfigUtil.serialize(
        GlutenConfig
          .getOmniConf(backendName, GlutenConfigUtil.parseConfig(SQLConf.get.getAllConfs)))
    )

    private val released: AtomicBoolean = new AtomicBoolean(false)

    override def getHandle(): Long = handle

    override def release(): Unit = {
      if (!released.compareAndSet(false, true)) {
        throw new GlutenException(
          s"Runtime instance already released: $handle, ${resourceName()}, ${priority()}")
      }
      OmniRuntimeJniWrapper.releaseRuntime(handle)

    }

    override def priority(): Int = 20

    override def resourceName(): String = s"runtime"
  }
}
