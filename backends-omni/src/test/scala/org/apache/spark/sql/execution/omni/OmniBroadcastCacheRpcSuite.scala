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

import com.google.protobuf.{Any => ProtoAny, StringValue}
import io.substrait.proto.{JoinRel, Plan, PlanRel, Rel, RelRoot}
import io.substrait.proto.AdvancedExtension
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointAddress, RpcEnv}
import org.scalatest.funsuite.AnyFunSuite

import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}

class OmniBroadcastCacheRpcSuite extends AnyFunSuite {
  test("finds native broadcast cache keys inside a serialized Substrait plan") {
    val key = "omni-bhj:app-1:42:exchange-1#join-a"
    val optimization = StringValue.newBuilder()
      .setValue(s"JoinParameters:\nisBHJ=1\nbuildHashTableId=$key\nisBuildLeft=0\n")
      .build()
    val join = JoinRel.newBuilder()
      .setAdvancedExtension(AdvancedExtension.newBuilder()
        .setOptimization(ProtoAny.pack(optimization, "")))
      .build()
    val plan = Plan.newBuilder()
      .addRelations(PlanRel.newBuilder()
        .setRoot(RelRoot.newBuilder().setInput(Rel.newBuilder().setJoin(join))))
      .build()

    assert(OmniBroadcastCacheRpc.cacheKeys(Plan.parseFrom(plan.toByteArray)) == Set(key))
  }

  test("execution retirement reaches a holder and rejects late registration") {
    import OmniBroadcastCacheRpc._

    val conf = new SparkConf(false)
      .setMaster("local[1]")
      .setAppName("omni-broadcast-cache-rpc")
      .set("spark.ui.enabled", "false")
    val sc = new SparkContext(conf)
    val rpcEnv = SparkEnv.get.rpcEnv
    val acknowledged = new CountDownLatch(1)
    val endpoint = new RpcEndpoint {
      override val rpcEnv: RpcEnv = SparkEnv.get.rpcEnv
      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case RetireExecution("42", prefixes) =>
          assert(prefixes == Seq("omni-bhj:app:42:exchange"))
          acknowledged.countDown()
          context.reply(java.lang.Boolean.TRUE)
      }
    }
    val ref = rpcEnv.setupEndpoint("omni-broadcast-cache-test-" + UUID.randomUUID(), endpoint)
    try {
      startDriver(sc)
      openExecution("42")
      val uri = RpcEndpointAddress(rpcEnv.address, ref.name).toString
      val executor = RegisterExecutor("test-executor", "incarnation-1", uri)
      val holder = RegisterHolder(
        "42", "omni-bhj:app:42:exchange#signature", "test-executor", "incarnation-1")
      assert(onDriverMessage(executor).contains(java.lang.Boolean.TRUE))
      assert(onDriverMessage(holder).contains(java.lang.Boolean.TRUE))

      retireExecution("42")
      assert(acknowledged.await(10, TimeUnit.SECONDS))
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
      while (cleanupAcks == 0 && System.nanoTime() < deadline) {
        Thread.sleep(10)
      }
      assert(cleanupAcks == 1)
      assert(cleanupFailures == 0)
      assert(onDriverMessage(holder).contains(java.lang.Boolean.FALSE))
    } finally {
      stopDriver()
      rpcEnv.stop(ref)
      sc.stop()
    }
  }
}
