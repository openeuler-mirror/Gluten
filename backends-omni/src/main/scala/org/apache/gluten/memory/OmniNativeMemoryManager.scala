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
package org.apache.gluten.memory

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.memory.listener.ReservationListeners
import org.apache.gluten.memory.memtarget.{MemoryTarget, Spiller, Spillers}
import org.apache.gluten.proto.MemoryUsageStats
import org.apache.gluten.utils.ConfigUtil

import org.apache.spark.TaskContext
import org.apache.spark.sql.internal.{GlutenConfigUtil, SQLConf}
import org.apache.spark.task.{TaskResource, TaskResources}

import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Per-task `gluten::MemoryManager` lifecycle wrapper for the Omni backend.
 *
 * Mirrors the Gluten native-memory bridge, with one important Omni invariant:
 *  - Constructed once per backend in each Spark task (registered as a named TaskResource).
 *    Independently named runtimes share it because VectorBatches cross runtime boundaries.
 *  - `getHandle()` returns the jlong opaque handle that, on the native side,
 *    decodes via `jniCastOrThrow<gluten::MemoryManager>(nmmHandle)` to the
 *    `gluten::OmniMemoryManager*` created by the registered "omni" factory.
 *  - `release()` is called by `TaskResources` at task end with priority=0,
 *    so it tears down AFTER OmniRuntime (priority=20).
 *
 * Spark-driven reclaim is synchronous: the reservation listener invokes the
 * SPILL spiller on the allocating task thread, which calls native spill
 * and returns the bytes actually repaid to Spark.
 *
 * JDK 8 / Scala 2.12 compatible: uses scala.collection.JavaConverters
 * (not the Scala 2.13+ converters), no mutable locals via `v` `a` `r`, no records.
 */
trait OmniNativeMemoryManager {
  def getHandle(): Long
  def hold(): Unit

  /**
   * Expose the embedded
   * `omniruntime::mem::OmniTaskMemoryPool*` as a jlong. Used by callers in
   * `omni/bindings/` (e.g. `ShuffleHashHelper.computePartitionIds`) that
   * need to bind allocations to the per-task pool but cannot link against
   * gluten/cpp/core types directly. Returns 0 when the manager is not a
   * `gluten::OmniMemoryManager` (test harness / mock); production always
   * returns a non-zero pool address.
   */
  def getPoolAddress(): Long
}

object OmniNativeMemoryManager {
  private val SHARED_RESOURCE_NAME = "omni-shared-nmm"
  private[memory] type ManagedNmm = OmniNativeMemoryManager with TaskResource

  /** A managed NMM that can say whether TaskResources already released it. */
  private[memory] trait ReleaseTracked {
    def isReleased: Boolean
  }

  private final class CachedNmm(
      val context: TaskContext,
      val backendName: String,
      val nmm: ManagedNmm with ReleaseTracked)

  // contextInstance() runs for every vector batch (pool-address lookup). Going
  // through TaskResources each time takes the executor-wide registry monitor,
  // which all task threads contend on. A TaskContext is unique per task
  // attempt, so remembering the last lookup per thread is safe across thread
  // reuse; a released NMM is never served from here.
  private val cachedNmm = new ThreadLocal[CachedNmm]

  // Keep the whole callback lazy so SHRINK cannot enter JNI or mutate the
  // reservation ledger, even if a caller presents a positive byte request.
  private[memory] def spillInPhase(phase: Spiller.Phase)(spill: => Long): Long = {
    if (Spillers.PHASE_SET_SPILL_ONLY.contains(phase)) spill else 0L
  }

  private[memory] def repayAndReconcile(repay: => Long)(reconcile: Long => Unit): Long = {
    var repaid = 0L
    try {
      repaid = repay
      repaid
    } finally {
      reconcile(repaid)
    }
  }

  private def environmentSwitchEnabled(name: String): Boolean = {
    Option(System.getenv(name)).exists(value => value.nonEmpty && value != "0")
  }

  private class Impl(backendName: String, name: String)
    extends OmniNativeMemoryManager
    with TaskResource
    with ReleaseTracked {
    private val LOGGER = LoggerFactory.getLogger(classOf[OmniNativeMemoryManager])
    // The native handle is created with this appendable container, then the
    // spill callback is installed once the handle is available.
    private val spillers = Spillers.appendable()
    private val mutableStats: mutable.Map[String, MemoryUsageStatsBuilder] = mutable.Map()
    private val rl = ReservationListeners.create(name, spillers, mutableStats.asJava, true)
    private val taskContext = TaskResources.getLocalTaskContext()
    private val sharedUsage = TaskResources.getSharedUsage()
    private val taskMemoryLock = OmniMemoryRepayment.currentTaskMemoryLock()
    private val reclaimSuccessTelemetryEnabled =
      environmentSwitchEnabled("OMNI_RECLAIM_VERBOSE") ||
        environmentSwitchEnabled("OMNI_RECLAIM_VERBOSE_SUCCESS")
    private def taskProperty(key: String): String =
      Option(taskContext).flatMap(context => Option(context.getLocalProperty(key))).getOrElse("unknown")
    private val handle = OmniNativeMemoryManagerJniWrapper.create(
      backendName,
      rl,
      ConfigUtil.serialize(
        GlutenConfig
          .getOmniConf(backendName, GlutenConfigUtil.parseConfig(SQLConf.get.getAllConfs))),
      taskProperty("omni.telemetry.run_id"),
      taskProperty("omni.telemetry.qid"),
      taskProperty("omni.telemetry.round"),
      Option(taskContext).map(_.taskAttemptId()).getOrElse(-1L)
    )
    require(handle > 0L, "Native Omni memory manager creation returned an invalid handle")
    // The embedded pool is immutable for the NMM lifetime. Resolve the native
    // address once per task instead of crossing JNI for every vector wrapper.
    private val poolAddress = OmniNativeMemoryManagerJniWrapper.getPoolAddress(handle)
    spillers.append(new Spiller() {
      override def spill(self: MemoryTarget, phase: Spiller.Phase, size: Long): Long = {
        spillInPhase(phase) {
          val usedBefore = if (reclaimSuccessTelemetryEnabled) self.usedBytes() else 0L
          val freed = OmniNativeMemoryManagerJniWrapper.spill(handle, size)
          if (freed <= 0L) {
            0L
          } else {
            // Spark's task memory accounting changes only when the owning
            // MemoryTarget repays. Native spill has lowered Omni's reservation
            // marker without listener-unbilling, so this is the matching credit.
            val repaid = repayAndReconcile(
              OmniMemoryRepayment.repayAndRecord(self, freed, sharedUsage, taskMemoryLock)) {
              actual =>
                // Also restore the marker when repayment throws before returning a credit.
                OmniNativeMemoryManagerJniWrapper.reconcileExternalRepay(handle, freed, actual)
            }
            if (reclaimSuccessTelemetryEnabled) {
              System.err.println(
                s"[OMNI-SPARK-REPAY] manager_handle=$handle recipient_type=${self.getClass.getSimpleName} " +
                  s"recipient_identity=${System.identityHashCode(self)} thread=${Thread.currentThread.getId} " +
                  s"requested=$size native_freed=$freed used_before=$usedBefore repaid=$repaid " +
                  s"repay_gap=${freed - repaid} used_after=${self.usedBytes()}")
            }
            repaid
          }
        }
      }
    })
    mutableStats += "single" -> new MemoryUsageStatsBuilder {
      override def toStats: MemoryUsageStats = collectUsage()
    }

    private def collectUsage(): MemoryUsageStats = {
      MemoryUsageStats.parseFrom(OmniNativeMemoryManagerJniWrapper.collectUsage(handle))
    }

    private val released: AtomicBoolean = new AtomicBoolean(false)

    override def getHandle(): Long = handle
    override def hold(): Unit = OmniNativeMemoryManagerJniWrapper.hold(handle)
    override def getPoolAddress(): Long = poolAddress

    override def isReleased: Boolean = released.get()

    override def release(): Unit = {
      if (!released.compareAndSet(false, true)) {
        throw new GlutenException(
          s"Memory manager instance already released: $handle, ${resourceName()}, ${priority()}")
      }
      forgetCached(this)
      OmniNativeMemoryManagerJniWrapper.release(handle)
      if (rl.getUsedBytes != 0) {
        LOGGER.warn(
          s"$name Reservation listener still reserved non-zero bytes: ${rl.getUsedBytes}")
      }
    }

    // Priority 0 → released AFTER OmniRuntime (priority=20). TaskResources
    // releases higher priority first, so Runtime tears down before its NMM.
    override def priority(): Int = 0
    override def resourceName(): String = "omni-nmm"
  }

  /**
   * Return the single native memory manager for this backend in the current Spark task.
   *
   * Native runtimes remain independent state/lifecycle domains, but memory can cross those
   * boundaries in a VectorBatch (for example ShuffleReader -> HashBuild). Sharing the NMM makes
   * the Spark task, rather than the runtime name, the accounting and reclaim domain.
   */
  def contextInstance(backendName: String): OmniNativeMemoryManager = {
    contextInstanceWithFactory(
      backendName,
      () => new Impl(backendName, SHARED_RESOURCE_NAME))
  }

  private[memory] def contextInstanceWithFactory(
      backendName: String,
      factory: () => ManagedNmm): OmniNativeMemoryManager = {
    if (!TaskResources.inSparkTask()) {
      throw new IllegalStateException("This method must be called in a Spark task.")
    }
    val context = TaskResources.getLocalTaskContext()
    val cached = cachedNmm.get()
    if (cached != null && (cached.context eq context) && cached.backendName == backendName &&
      !cached.nmm.isReleased) {
      return cached.nmm
    }
    // Keep this key outside OmniRuntimes' `backend:name` namespace so a runtime
    // name can never accidentally alias the task-shared memory manager.
    val resourceId = s"${classOf[OmniNativeMemoryManager].getName}:$backendName"
    val nmm: ManagedNmm = TaskResources.addResourceIfNotRegistered(resourceId, factory)
    nmm match {
      case tracked: ReleaseTracked if !tracked.isReleased =>
        cachedNmm.set(
          new CachedNmm(context, backendName, nmm.asInstanceOf[ManagedNmm with ReleaseTracked]))
      case _ =>
    }
    nmm
  }

  /** Drop this thread's memo so a finished task does not pin its NMM. */
  private[memory] def forgetCached(nmm: OmniNativeMemoryManager): Unit = {
    val cached = cachedNmm.get()
    if (cached != null && (cached.nmm eq nmm)) {
      cachedNmm.remove()
    }
  }

  /** Compatibility entry point. The name identifies a runtime, not a memory domain. */
  def apply(backendName: String, _name: String): OmniNativeMemoryManager = {
    contextInstance(backendName)
  }
}
