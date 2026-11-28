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

package org.apache.gluten.runtime

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.memory.OmniNativeMemoryManager
import org.apache.gluten.utils.ConfigUtil

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.sql.internal.{GlutenConfigUtil, SQLConf}
import org.apache.spark.task.TaskResource

import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicBoolean

trait OmniRuntime {
  def getHandle(): Long

  // Expose the task-shared NMM handle. Runtime names remain separate native
  // state/lifecycle domains, while all runtimes for one backend in a Spark task
  // allocate through the same OmniTaskMemoryPool. The handle decodes to the
  // gluten::MemoryManager* on the native side via jniCastOrThrow.
  def getNmmHandle(): Long

  // Immutable task-pool address cached by the shared NMM.
  def getPoolAddress(): Long
}

object OmniRuntime {
  private final class CachedConf(
      val context: TaskContext,
      val backendName: String,
      val bytes: Array[Byte])

  // Every runtime of a task (plan evaluator, shuffle writer/reader, serializer)
  // serializes the same SparkConf + SQLConf. Build it once per task attempt on
  // each thread instead of once per runtime; TaskContext identity is unique per
  // attempt, so a pooled thread rebuilds it for its next task.
  private val cachedConf = new ThreadLocal[CachedConf]

  private[runtime] def runtimeConfBytes(backendName: String): Array[Byte] = {
    val context = TaskContext.get()
    val cached = cachedConf.get()
    if (context != null && cached != null && (cached.context eq context) &&
      cached.backendName == backendName) {
      return cached.bytes
    }
    // SQLConf in executor task threads may omit application SparkConf entries.
    // Keep SparkConf as the base and let explicit SQL session settings win.
    val runtimeConf =
      Option(SparkEnv.get).map(_.conf.getAll.toMap).getOrElse(Map.empty[String, String]) ++
        SQLConf.get.getAllConfs
    val bytes = ConfigUtil.serialize(
      GlutenConfig.getOmniConf(backendName, GlutenConfigUtil.parseConfig(runtimeConf)))
    if (context != null) {
      cachedConf.set(new CachedConf(context, backendName, bytes))
    }
    bytes
  }

  private[runtime] def apply(backendName: String, name: String): OmniRuntime with TaskResource = {
    new RuntimeImpl(backendName, name)
  }

  private class RuntimeImpl(backendName: String, name: String) extends OmniRuntime with TaskResource {
    private val LOGGER = LoggerFactory.getLogger(classOf[OmniRuntime])

    // Every independently named runtime in this Spark task shares one backend NMM.
    // The NMM is a separate TaskResource (priority=0), so it tears down only after
    // all Runtime resources (priority=20) and their pool-bound buffers are released.
    private val nmm: OmniNativeMemoryManager = OmniNativeMemoryManager.contextInstance(backendName)

    private val handle = OmniRuntimeJniWrapper.createRuntime(
      backendName,
      nmm.getHandle(),
      runtimeConfBytes(backendName)
    )

    private val released: AtomicBoolean = new AtomicBoolean(false)

    override def getHandle(): Long = handle

    // Every runtime in the task returns the same handle, so buffers crossing
    // runtime boundaries retain one accounting/reclaim owner.
    override def getNmmHandle(): Long = nmm.getHandle()
    override def getPoolAddress(): Long = nmm.getPoolAddress()

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
