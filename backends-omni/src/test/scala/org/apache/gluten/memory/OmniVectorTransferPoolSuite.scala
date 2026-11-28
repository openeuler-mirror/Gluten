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

import nova.hetu.omniruntime.vector.{IntVec, StructVec, Vec}
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.substrait.`type`.{I32TypeNode, StructNode, TypeNode}
import org.apache.gluten.vectorized.{OmniColumnVector, VectorTransferUtils}
import org.apache.spark.task.{TaskResource, TaskResources}

import org.mockito.Mockito.{mock, when}
import org.scalatest.funsuite.AnyFunSuite

import java.util.Arrays

class OmniVectorTransferPoolSuite extends AnyFunSuite {
  private class FakeNmm(val address: Long) extends OmniNativeMemoryManager with TaskResource {
    var poolReads = 0
    var releases = 0
    override def getHandle(): Long = address
    override def getPoolAddress(): Long = { poolReads += 1; address }
    override def hold(): Unit = {}
    override def release(): Unit = { releases += 1 }
    override def priority(): Int = 0
    override def resourceName(): String = "fake-transfer-nmm"
  }

  private def register(nmm: FakeNmm): Unit = {
    OmniNativeMemoryManager.contextInstanceWithFactory(
      BackendsApiManager.getBackendName, () => nmm)
  }

  private def intVector(): IntVec = {
    val vec = mock(classOf[IntVec])
    when(vec.getSize).thenReturn(3)
    vec
  }

  private def wrapperPool(vector: OmniColumnVector): Long = {
    val field = classOf[OmniColumnVector].getDeclaredField("taskPoolAddress")
    field.setAccessible(true)
    field.getLong(vector)
  }

  test("a wide batch resolves its task pool once and keeps the original vectors") {
    val nmm = new FakeNmm(123L)
    TaskResources.runUnsafe {
      register(nmm)
      val vecs: Array[Vec] = Array.fill(8)(intVector())
      val types = Arrays.asList[TypeNode](Array.fill(8)(new I32TypeNode(true)): _*)
      val batch = VectorTransferUtils.vectorsToNativeColumnarBatch(vecs, types, 3)
      try {
        assert(nmm.poolReads == 1)
        assert(batch.numRows() == 3 && batch.numCols() == 8)
        vecs.indices.foreach { i =>
          val column = batch.column(i).asInstanceOf[OmniColumnVector]
          assert(column.getVec eq vecs(i))
          assert(wrapperPool(column) == nmm.address)
        }
      } finally batch.close()
    }
    assert(nmm.releases == 1)
  }

  test("a producer can reuse its resolved pool without another registry lookup") {
    val nmm = new FakeNmm(456L)
    TaskResources.runUnsafe {
      register(nmm)
      val pool = nmm.getPoolAddress()
      val batch = VectorTransferUtils.vectorsToNativeColumnarBatch(
        Array[Vec](intVector()), Arrays.asList[TypeNode](new I32TypeNode(true)), 3, pool)
      try {
        assert(nmm.poolReads == 1)
        assert(wrapperPool(batch.column(0).asInstanceOf[OmniColumnVector]) == pool)
      } finally batch.close()
      intercept[IllegalStateException] {
        VectorTransferUtils.vectorsToNativeColumnarBatch(
          Array.empty[Vec], Arrays.asList[TypeNode](), 0, 0L)
      }
    }
    assert(nmm.releases == 1)
  }

  test("complex column children inherit the producer pool") {
    val nmm = new FakeNmm(789L)
    TaskResources.runUnsafe {
      register(nmm)
      val child = intVector()
      val parent = mock(classOf[StructVec])
      when(parent.getSize).thenReturn(3)
      when(parent.getChild(0)).thenReturn(child)
      val structType = new StructNode(false, Arrays.asList[TypeNode](new I32TypeNode(true)))
      val batch = VectorTransferUtils.vectorsToNativeColumnarBatch(
        Array[Vec](parent), Arrays.asList[TypeNode](structType), 3, nmm.address)
      try {
        val column = batch.column(0).asInstanceOf[OmniColumnVector]
        val childColumn = column.getChild(0).asInstanceOf[OmniColumnVector]
        assert(wrapperPool(column) == nmm.address)
        assert(wrapperPool(childColumn) == nmm.address)
        assert(childColumn.getVec eq child)
        assert(nmm.poolReads == 0)
      } finally batch.close()
    }
  }

  test("successive tasks use distinct pools without retaining the preceding task") {
    Seq(111L, 222L).foreach { address =>
      val nmm = new FakeNmm(address)
      TaskResources.runUnsafe {
        register(nmm)
        val batch = VectorTransferUtils.vectorsToNativeColumnarBatch(
          Array[Vec](intVector()), Arrays.asList[TypeNode](new I32TypeNode(true)), 3)
        try {
          assert(wrapperPool(batch.column(0).asInstanceOf[OmniColumnVector]) == address)
          assert(nmm.poolReads == 1)
        } finally batch.close()
      }
      assert(nmm.releases == 1)
    }
  }

  test("task-free wrapping retains the standalone zero-pool path") {
    assert(!TaskResources.inSparkTask())
    val vec = intVector()
    val batch = VectorTransferUtils.vectorsToNativeColumnarBatch(
      Array[Vec](vec), Arrays.asList[TypeNode](new I32TypeNode(true)), 3)
    try {
      val column = batch.column(0).asInstanceOf[OmniColumnVector]
      assert(wrapperPool(column) == 0L)
      assert(column.getVec eq vec)
    } finally batch.close()
  }
}
