/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.memory

import nova.hetu.omniruntime.vector.serialize.VecBatchSerde
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.OmniColumnarBuildSideRelation
import org.apache.spark.TaskContext
import org.apache.spark.util.TaskCompletionListener
import org.apache.spark.sql.catalyst.plans.physical.IdentityBroadcastMode
import org.apache.spark.task.{TaskResource, TaskResources}

import org.scalatest.funsuite.AnyFunSuite

class OmniBroadcastPoolLookupSuite extends AnyFunSuite {
  private class FakeNmm(address: Long) extends OmniNativeMemoryManager with TaskResource {
    var poolReads = 0
    var releases = 0
    var failNextRead: RuntimeException = null
    override def getHandle(): Long = address
    override def getPoolAddress(): Long = {
      poolReads += 1
      if (failNextRead != null) {
        val error = failNextRead
        failNextRead = null
        throw error
      }
      address
    }
    override def hold(): Unit = {}
    override def release(): Unit = { releases += 1 }
    override def priority(): Int = 0
    override def resourceName(): String = "fake-broadcast-nmm"
  }

  private def register(nmm: FakeNmm): Unit = {
    OmniNativeMemoryManager.contextInstanceWithFactory(
      BackendsApiManager.getBackendName, () => nmm)
  }

  private def relation(batchCount: Int): OmniColumnarBuildSideRelation = {
    // Zero-column payloads exercise the actual producer without dereferencing fake pools.
    val payload = VecBatchSerde.VecBatch.newBuilder().setVecCount(0).setRowCount(3).build()
    OmniColumnarBuildSideRelation(
      IdentityBroadcastMode, Seq.empty, Array.fill(batchCount)(payload.toByteArray))
  }

  test("broadcast iteration resolves one pool for successive batches in the same task") {
    val nmm = new FakeNmm(123L)
    TaskResources.runUnsafe {
      register(nmm)
      val iterator = relation(3).deserialized
      assert(iterator.hasNext)
      assert(nmm.poolReads == 0)
      (1 to 3).foreach { _ =>
        val batch = iterator.next()
        try assert(batch.numRows() == 3) finally batch.close()
      }
      assert(nmm.poolReads == 1)
      assert(!iterator.hasNext)
    }
    assert(nmm.releases == 1)
  }

  test("an empty broadcast does not resolve a task pool") {
    val nmm = new FakeNmm(234L)
    TaskResources.runUnsafe {
      register(nmm)
      assert(!relation(0).deserialized.hasNext)
      assert(nmm.poolReads == 0)
    }
  }

  test("the same iterator refreshes its pool when its task context changes") {
    val iterator = relation(4).deserialized
    Seq(345L, 456L).foreach { address =>
      val nmm = new FakeNmm(address)
      TaskResources.runUnsafe {
        register(nmm)
        (1 to 2).foreach { _ => iterator.next().close() }
        assert(nmm.poolReads == 1)
      }
      assert(nmm.releases == 1)
    }
    assert(!iterator.hasNext)
  }

  test("pool lookup failure propagates without consuming a batch or caching failure") {
    val nmm = new FakeNmm(567L)
    val error = new IllegalStateException("pool unavailable")
    TaskResources.runUnsafe {
      register(nmm)
      val iterator = relation(2).deserialized
      nmm.failNextRead = error
      assert(intercept[IllegalStateException](iterator.next()) eq error)
      assert(iterator.hasNext)
      val first = iterator.next()
      try assert(first.numRows() == 3) finally first.close()
      val second = iterator.next()
      try assert(second.numRows() == 3) finally second.close()
      assert(nmm.poolReads == 2)
      assert(!iterator.hasNext)
    }
  }
  test("standalone iteration cannot retain a zero pool in a later Spark task") {
    val iterator = relation(3).deserialized
    iterator.next().close()
    val nmm = new FakeNmm(678L)
    TaskResources.runUnsafe {
      register(nmm)
      iterator.next().close()
      assert(nmm.poolReads == 1)
    }
    iterator.next().close()
    assert(nmm.releases == 1)
    assert(!iterator.hasNext)
  }

  test("a completed task rejects iteration before using its released pool") {
    val nmm = new FakeNmm(789L)
    var completionChecked = false
    TaskResources.runUnsafe {
      register(nmm)
      val iterator = relation(2).deserialized
      iterator.next().close()
      TaskContext.get().addTaskCompletionListener(new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext): Unit = {
          assert(context.isCompleted())
          assert(intercept[IllegalStateException](iterator.next()).getMessage.contains(
            "after task completion"))
          assert(nmm.poolReads == 1)
          assert(iterator.hasNext)
          completionChecked = true
        }
      })
    }
    assert(completionChecked)
    assert(nmm.releases == 1)
  }
}
