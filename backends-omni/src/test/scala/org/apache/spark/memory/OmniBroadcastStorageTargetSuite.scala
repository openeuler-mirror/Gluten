/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package org.apache.spark.memory

import org.apache.spark.{SparkConf, SparkContext, SparkEnv, TaskContext}
import org.scalatest.funsuite.AnyFunSuite

class OmniBroadcastStorageTargetSuite extends AnyFunSuite {
  test("broadcast reservations use executor storage without a task") {
    val conf = new SparkConf(false)
      .setMaster("local[1]")
      .setAppName("omni-broadcast-storage-target")
      .set("spark.ui.enabled", "false")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "64m")
    val sc = new SparkContext(conf)
    try {
      assert(TaskContext.get() == null)
      val manager = SparkEnv.get.memoryManager
      val baseline = manager.offHeapStorageMemoryUsed
      val target = OmniBroadcastStorageTarget.forCurrentExecutor(4L << 20)
      assert(target.reserve(1L << 20) == (1L << 20))
      assert(manager.offHeapStorageMemoryUsed - baseline == (1L << 20))
      assert(target.getUsedBytes() == (1L << 20))
      assert(target.reserve(4L << 20) == 0L)
      assert(manager.offHeapStorageMemoryUsed - baseline == (1L << 20))
      assert(target.unreserve(1L << 20) == (1L << 20))
      assert(manager.offHeapStorageMemoryUsed == baseline)
      assert(target.getUsedBytes() == 0L)
      intercept[IllegalArgumentException](target.unreserve(1L))

      // Multiple acquisitions under one owner must still add independent charges.
      val chunk = 1L << 20
      (1 to 4).foreach { n =>
        assert(target.reserve(chunk) == chunk)
        assert(manager.offHeapStorageMemoryUsed - baseline == n * chunk)
      }
      assert(target.reserve(1L) == 0L)
      assert(target.unreserve(2 * chunk) == 2 * chunk)
      assert(target.reserve(chunk) == chunk)
      assert(target.getUsedBytes() == 3 * chunk)
      assert(manager.offHeapStorageMemoryUsed - baseline == 3 * chunk)
      assert(target.unreserve(3 * chunk) == 3 * chunk)
      assert(manager.offHeapStorageMemoryUsed == baseline)
      assert(target.totalGrantedBytes == 6 * chunk)
    } finally {
      sc.stop()
    }
  }
}
