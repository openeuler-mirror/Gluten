/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
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

import org.apache.gluten.memory.memtarget.{MemoryTarget, MemoryTargetVisitor, Spiller}

import org.apache.spark.task.{TaskResource, TaskResources}

import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.atomic.AtomicLong

class OmniNativeMemoryManagerSuite extends AnyFunSuite {
  test("failed external repayment restores the full native credit") {
    val failure = new IllegalStateException("repayment rejected")
    var reconciled = -1L
    val thrown = intercept[IllegalStateException] {
      OmniNativeMemoryManager.repayAndReconcile(throw failure)(actual => reconciled = actual)
    }
    assert(thrown eq failure)
    assert(reconciled == 0L)
  }

  test("partial external repayment reconciles only the actual credit") {
    var reconciled = -1L
    val repaid = OmniNativeMemoryManager.repayAndReconcile(4L)(actual => reconciled = actual)
    assert(repaid == 4L)
    assert(reconciled == 4L)
  }

  test("Omni calls its spill body only for SPILL phase") {
    var calls = 0
    def spillBody: Long = { calls += 1; 4L }
    assert(OmniNativeMemoryManager.spillInPhase(Spiller.Phase.SHRINK)(spillBody) == 0L)
    assert(calls == 0)
    assert(OmniNativeMemoryManager.spillInPhase(Spiller.Phase.SPILL)(spillBody) == 4L)
    assert(calls == 1)
  }

  private class FakeNmm(released: AtomicLong)
    extends OmniNativeMemoryManager
    with TaskResource {
    private val handle = FakeNmm.nextHandle.incrementAndGet()

    override def getHandle(): Long = handle
    override def getPoolAddress(): Long = handle + 1000L
    override def hold(): Unit = {}
    override def release(): Unit = released.incrementAndGet()
    override def priority(): Int = 0
    override def resourceName(): String = "fake-omni-nmm"
  }

  private class TrackedFakeNmm(released: AtomicLong)
    extends FakeNmm(released)
    with OmniNativeMemoryManager.ReleaseTracked {
    @volatile private var done = false
    override def isReleased: Boolean = done
    override def release(): Unit = {
      done = true
      OmniNativeMemoryManager.forgetCached(this)
      super.release()
    }
  }

  private object FakeNmm {
    val nextHandle = new AtomicLong(0L)
  }

  test("runtime names in one task share one backend NMM") {
    val created = new AtomicLong(0L)
    val released = new AtomicLong(0L)

    TaskResources.runUnsafe {
      def create(): OmniNativeMemoryManager.ManagedNmm = {
        created.incrementAndGet()
        new FakeNmm(released)
      }

      val first = OmniNativeMemoryManager.contextInstanceWithFactory("omni", () => create())
      val second = OmniNativeMemoryManager.contextInstanceWithFactory("omni", () => create())

      assert(first eq second)
      assert(first.getHandle() == second.getHandle())
      assert(first.getPoolAddress() == second.getPoolAddress())
      assert(created.get() == 1L)
      assert(released.get() == 0L)
    }

    assert(released.get() == 1L)
  }

  test("repeated lookups in one task skip the TaskResources registry") {
    val created = new AtomicLong(0L)
    val released = new AtomicLong(0L)
    var first: OmniNativeMemoryManager = null

    TaskResources.runUnsafe {
      def create(): OmniNativeMemoryManager.ManagedNmm = {
        created.incrementAndGet()
        new TrackedFakeNmm(released)
      }

      first = OmniNativeMemoryManager.contextInstanceWithFactory("omni", () => create())
      // A throwing factory proves the second lookup is served from the memo.
      val second = OmniNativeMemoryManager.contextInstanceWithFactory(
        "omni",
        () => throw new IllegalStateException("registry consulted again"))
      assert(first eq second)
      assert(created.get() == 1L)
    }
    assert(released.get() == 1L)

    // A new task on the same thread must not see the released NMM.
    TaskResources.runUnsafe {
      val next = OmniNativeMemoryManager.contextInstanceWithFactory(
        "omni",
        () => new TrackedFakeNmm(released))
      assert(!(next eq first))
    }
  }

  test("backend remains part of the task memory-domain key") {
    val created = new AtomicLong(0L)
    val released = new AtomicLong(0L)

    TaskResources.runUnsafe {
      def create(): OmniNativeMemoryManager.ManagedNmm = {
        created.incrementAndGet()
        new FakeNmm(released)
      }

      val omni = OmniNativeMemoryManager.contextInstanceWithFactory("omni", () => create())
      val other = OmniNativeMemoryManager.contextInstanceWithFactory("other", () => create())

      assert(!(omni eq other))
      assert(omni.getHandle() != other.getHandle())
      assert(created.get() == 2L)
    }

    assert(released.get() == 2L)
  }

  test("external repayment updates shared usage once and preserves the real peak") {
    val usage = new SimpleMemoryUsageRecorder()
    val lock = new Object()
    var repays = 0
    var repayLimit = 4L
    val target = new MemoryTarget {
      private var used = 0L
      override def borrow(size: Long): Long = { used += size; size }
      override def repay(size: Long): Long = {
        assert(Thread.holdsLock(lock))
        repays += 1
        val repaid = math.min(used, math.min(size, repayLimit))
        used -= repaid
        repaid
      }
      override def usedBytes(): Long = used
      override def accept[T](visitor: MemoryTargetVisitor[T]): T =
        throw new UnsupportedOperationException()
    }
    // Another manager contributes 2 bytes to the same task recorder.
    usage.inc(2)
    usage.inc(target.borrow(8))
    for (_ <- 0 until 10) {
      assert(OmniMemoryRepayment.repayAndRecord(target, 8, usage, lock) == 4)
      assert(usage.current() == 6)
      usage.inc(target.borrow(4))
      assert(usage.current() == 10)
      assert(usage.peak() == 10)
    }
    assert(repays == 10)
    repayLimit = 0
    assert(OmniMemoryRepayment.repayAndRecord(target, 8, usage, lock) == 0)
    assert(usage.current() == 10)
    repayLimit = 8
    assert(OmniMemoryRepayment.repayAndRecord(target, 8, usage, lock) == 8)
    assert(usage.current() == 2)
    usage.inc(-2)
    assert(usage.current() == 0)
    assert(usage.peak() == 10)
    assert(repays == 12)
  }
}
