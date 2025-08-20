/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.apache.gluten.runtime

import org.apache.spark.task.{TaskResource, TaskResources}

object OmniRuntimes {

  /** Get or create the runtime which bound with Spark TaskContext. */
  def contextInstance(backendName: String, name: String): OmniRuntime = {
    if (!TaskResources.inSparkTask()) {
      throw new IllegalStateException("This method must be called in a Spark task.")
    }

    val resourceName = String.format("%s:%s", backendName, name)
    TaskResources.addResourceIfNotRegistered(resourceName, () => create(backendName, name))
  }

  private def create(backendName: String, name: String): OmniRuntime with TaskResource = {
    OmniRuntime(backendName, name)
  }
}
