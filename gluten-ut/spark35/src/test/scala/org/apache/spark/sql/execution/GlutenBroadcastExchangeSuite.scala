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
package org.apache.spark.sql.execution

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.broadcast.{Broadcast, TorrentBroadcast}
import org.apache.spark.sql.{GlutenSQLTestsBaseTrait, GlutenTestsBaseTrait, SparkSession}
import org.apache.spark.sql.catalyst.plans.physical.IdentityBroadcastMode
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.functions.broadcast

import org.mockito.invocation.InvocationOnMock
import org.mockito.Mockito.{doThrow, mock, never, verify, when}
import org.mockito.stubbing.Answer

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.concurrent.{CompletableFuture, Future}

class GlutenBroadcastExchangeSuite extends BroadcastExchangeSuite with GlutenSQLTestsBaseTrait {

  private def newExchange(
      child: SparkPlan = LocalTableScanExec(Nil, Nil)): ColumnarBroadcastExchangeExec = {
    spark.withActive {
      ColumnarBroadcastExchangeExec(IdentityBroadcastMode, child)
    }
  }

  private def cleanupFuture(exchange: ColumnarBroadcastExchangeExec): Future[Broadcast[Any]] = {
    val field = classOf[ColumnarBroadcastExchangeExec].getDeclaredField("relationFutureForCleanup")
    field.setAccessible(true)
    field.get(exchange).asInstanceOf[Future[Broadcast[Any]]]
  }

  private def setCleanupFuture(
      exchange: ColumnarBroadcastExchangeExec,
      future: Future[Broadcast[Any]]): Unit = {
    val field = classOf[ColumnarBroadcastExchangeExec].getDeclaredField("relationFutureForCleanup")
    field.setAccessible(true)
    field.set(exchange, future)
  }

  private def roundTrip(exchange: ColumnarBroadcastExchangeExec): ColumnarBroadcastExchangeExec = {
    val bytes = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bytes)
    try {
      out.writeObject(exchange)
    } finally {
      out.close()
    }
    val in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray))
    try {
      in.readObject().asInstanceOf[ColumnarBroadcastExchangeExec]
    } finally {
      in.close()
    }
  }

  testGluten("broadcast cleanup: unused plans do not start a broadcast") {
    val exchange = newExchange()
    // Driver cleanup must leave broadcast execution unstarted, even with an active session.
    assert(exchange.session != null)
    assert(cleanupFuture(exchange) == null)
    exchange.cleanupResources()
    exchange.cleanupResources()
    assert(cleanupFuture(exchange) == null)
  }

  testGluten("broadcast cleanup: deserialized plans do not start a broadcast") {
    val exchange = roundTrip(newExchange())
    assert(exchange.session == null)
    exchange.cleanupResources()
  }

  testGluten("broadcast cleanup: futures are not serialized into executor plans") {
    val exchange = newExchange()
    // CompletableFuture is not serializable. Only the driver copy may retain this reference.
    setCleanupFuture(exchange, new CompletableFuture[Broadcast[Any]]())
    val executorExchange = roundTrip(exchange)
    assert(executorExchange.session == null)
    assert(cleanupFuture(executorExchange) == null)
    executorExchange.cleanupResources()
  }

  testGluten("broadcast cleanup: execution publishes a driver-only cleanup future") {
    withSQLConf(
      "spark.sql.adaptive.enabled" -> "false",
      "spark.gluten.sql.columnar.broadcastExchange" -> "true",
      "spark.gluten.sql.columnar.broadcastJoin" -> "true") {
      val streamed = spark.range(0L, 4L, 1L, 2)
      val build = spark.range(0L, 2L, 1L, 1)
      val joined = streamed.join(broadcast(build), "id")
      assert(joined.collect().map(_.getLong(0)).sorted.toSeq == Seq(0L, 1L))

      val exchanges = joined.queryExecution.executedPlan.collect {
        case exchange: ColumnarBroadcastExchangeExec => exchange
      }
      assert(exchanges.size == 1, "the broadcast join must use one columnar broadcast exchange")
      val exchange = exchanges.head
      val future = cleanupFuture(exchange)
      // Read the reference populated by real execution; do not inject a test future here.
      assert(future != null)
      assert(future eq exchange.relationFuture)
      assert(future.isDone)

      val executorExchange = roundTrip(exchange)
      assert(executorExchange.session == null)
      assert(cleanupFuture(executorExchange) == null)
      executorExchange.cleanupResources()
      // All query tasks have finished, so driver cleanup cannot invalidate an in-use relation.
      exchange.cleanupResources()
    }
  }

  testGluten("broadcast cleanup: completed relations are reset and child cleanup runs") {
    val child = mock(classOf[SparkPlan])
    val exchange = newExchange(child)
    val relation = mock(classOf[BuildSideRelation])
    val broadcastValue = mock(classOf[Broadcast[Any]])
    when(broadcastValue.value).thenAnswer(new Answer[Any] {
      override def answer(invocation: InvocationOnMock): Any = relation
    })
    setCleanupFuture(exchange, CompletableFuture.completedFuture[Broadcast[Any]](broadcastValue))

    exchange.cleanupResources()

    verify(relation).reset()
    verify(child).cleanupResources()
  }

  testGluten("broadcast cleanup: unfinished broadcasts do not block child cleanup") {
    val child = mock(classOf[SparkPlan])
    val exchange = newExchange(child)
    val future = mock(classOf[Future[Broadcast[Any]]])
    when(future.isDone).thenReturn(false)
    setCleanupFuture(exchange, future)

    exchange.cleanupResources()

    verify(future, never()).get()
    verify(child).cleanupResources()
  }

  testGluten("broadcast cleanup: failed or cancelled broadcasts allow child cleanup") {
    val failed = new CompletableFuture[Broadcast[Any]]()
    failed.completeExceptionally(new IllegalStateException("broadcast failed"))
    val cancelled = new CompletableFuture[Broadcast[Any]]()
    cancelled.cancel(false)

    Seq(failed, cancelled).foreach {
      future =>
        val child = mock(classOf[SparkPlan])
        val exchange = newExchange(child)
        setCleanupFuture(exchange, future)

        exchange.cleanupResources()

        verify(child).cleanupResources()
    }
  }

  testGluten("broadcast cleanup: reset failure does not prevent child cleanup") {
    val child = mock(classOf[SparkPlan])
    val exchange = newExchange(child)
    val relation = mock(classOf[BuildSideRelation])
    val broadcastValue = mock(classOf[Broadcast[Any]])
    when(broadcastValue.value).thenAnswer(new Answer[Any] {
      override def answer(invocation: InvocationOnMock): Any = relation
    })
    doThrow(new IllegalStateException("reset failed")).when(relation).reset()
    setCleanupFuture(exchange, CompletableFuture.completedFuture[Broadcast[Any]](broadcastValue))

    exchange.cleanupResources()

    verify(child).cleanupResources()
  }
}


// Additional tests run in 'local-cluster' mode.
class GlutenLocalBroadcastExchangeSuite
  extends SparkFunSuite
  with LocalSparkContext
  with GlutenTestsBaseTrait
  with AdaptiveSparkPlanHelper {

  def newSparkConf(): SparkConf = {
    val conf = new SparkConf().setMaster("local-cluster[2,1,1024]")
    GlutenSQLTestsBaseTrait.nativeSparkConf(conf, warehouse)
  }

  test("SPARK-39983 - Broadcasted relation is not cached on the driver") {
    // Use distributed cluster as in local mode the broabcast value is actually cached.
    val conf = newSparkConf()
    sc = new SparkContext(conf)
    val spark = new SparkSession(sc)

    val df = spark.range(1).toDF()
    val joinDF = df.join(broadcast(df), "id")
    joinDF.collect()
    val broadcastExchangeExec = collect(joinDF.queryExecution.executedPlan) {
      case p: ColumnarBroadcastExchangeExec => p
    }
    assert(broadcastExchangeExec.size == 1, "one and only ColumnarBroadcastExchangeExec")

    // The broadcasted relation should not be cached on the driver.
    val broadcasted =
      broadcastExchangeExec(0).relationFuture.get().asInstanceOf[TorrentBroadcast[Any]]
    assert(!broadcasted.hasCachedValue)
  }
}
