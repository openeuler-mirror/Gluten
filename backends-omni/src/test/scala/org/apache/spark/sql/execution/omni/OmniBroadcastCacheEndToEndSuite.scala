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
package org.apache.spark.sql.execution.omni

import org.apache.gluten.vectorized.OmniPlanEvaluatorJniWrapper
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.memory.OmniBroadcastStorageTarget
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast
import org.scalatest.funsuite.AnyFunSuite

/** Run explicitly with OMNI_BHJ_CACHE_TEST_LIBPATH after installing the native plugin. */
class OmniBroadcastCacheEndToEndSuite extends AnyFunSuite {
  test("a separate executor reuses and retires its Spark-accounted native table") {
    val library = sys.env.getOrElse(
      "OMNI_BHJ_CACHE_TEST_LIBPATH",
      cancel("Set OMNI_BHJ_CACHE_TEST_LIBPATH to run the native multi-process test"))
    val conf = new SparkConf(false)
      .setMaster("local-cluster[1,1,1024]")
      .setAppName("omni-broadcast-cache-end-to-end")
      .set("spark.ui.enabled", "false")
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.gluten.sql.columnar.libpath", library)
      .set("spark.executor.extraClassPath", System.getProperty("java.class.path"))
      .set("spark.gluten.omni.buildHashTableOncePerExecutor.enabled", "true")
      .set("spark.gluten.omni.broadcastCache.maxBytes", "16m")
      .set("spark.executor.memory", "512m")
      .set("spark.task.maxFailures", "1")
      .set("spark.stage.maxConsecutiveAttempts", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "64m")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.shuffle.partitions", "1")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    try {
      def executorSnapshot(): (Seq[Long], Long, Long, Long, Long) = {
        spark.sparkContext.parallelize(Seq(1), 1).mapPartitions { _ =>
          val cache = OmniPlanEvaluatorJniWrapper.nativeBroadcastCacheSnapshot().toSeq
          val storage = SparkEnv.get.memoryManager.offHeapStorageMemoryUsed
          val registrations = OmniBroadcastCacheRpc.executorRegistrationCounts
          Iterator.single((cache, storage, registrations._1, registrations._2,
            OmniBroadcastStorageTarget.currentTotalGrantedBytes))
        }.collect().head
      }

      val before = executorSnapshot()
      val left = spark.range(0, 8192, 1, 8).toDF("k")
      val right = spark.range(0, 4096).toDF("k")
      val joined = left.join(broadcast(right), Seq("k"))
      val joinPlan = joined.queryExecution.executedPlan.treeString
      println(s"[OmniBHJCacheTest] physical join plan:\n$joinPlan")
      assert(joined.count() == 4096L)
      val first = executorSnapshot()
      println(s"[OmniBHJCacheTest] first=$first driverAcks=${OmniBroadcastCacheRpc.cleanupAcks}")
      assert(first._1(1) > before._1(1),
        s"first SQL execution did not build a native table; plan:\n$joinPlan")
      assert(joined.count() == 4096L)

      val deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(20)
      var after = executorSnapshot()
      while ((after._1(3) < before._1(3) + 2 || after._1(0) != 0L ||
          after._1(4) != 0L || after._2 != before._2) &&
          System.nanoTime() < deadline) {
        Thread.sleep(250)
        after = executorSnapshot()
      }
      println(s"[OmniBHJCacheTest] after=$after driverAcks=${OmniBroadcastCacheRpc.cleanupAcks}")
      assert(after._1(1) >= before._1(1) + 2, "reused SQL plan did not build each execution")
      assert(after._1(2) >= before._1(2) + 2, "the executor did not reuse cached tables")
      assert(after._1(3) >= before._1(3) + 2, "the driver did not retire both executions")
      assert(after._1(0) == 0L, "retired table is still indexed")
      assert(after._1(4) == 0L, "native reservation listener still has Spark grants")
      assert(after._2 == before._2, "Spark off-heap storage did not return to baseline")
      assert(after._5 > before._5, "native broadcast build never reserved Spark storage")
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }
}
