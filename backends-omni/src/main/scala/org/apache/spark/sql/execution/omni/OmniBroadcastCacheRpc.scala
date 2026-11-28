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

import com.google.protobuf.{Message, StringValue}
import io.substrait.proto.{JoinRel, Plan}
import org.apache.gluten.execution.OmniBroadcastBuildSideCache
import org.apache.spark.{SparkContext, SparkEnv, TaskContext}
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointAddress, RpcEndpointRef, RpcEnv, RpcTimeout}
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent, SparkListenerExecutorRemoved}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.util.Utils

import java.util.UUID
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.control.NonFatal

/** Spark-private RPC shim. No task memory manager owns this process-local service. */
object OmniBroadcastCacheRpc extends Logging {
  private[omni] final case class RegisterExecutor(id: String, incarnation: String, uri: String)
    extends Serializable
  private[omni] final case class RegisterHolder(
      executionId: String,
      key: String,
      executorId: String,
      incarnation: String)
    extends Serializable
  private[omni] final case class RetireExecution(executionId: String, prefixes: Seq[String])
    extends Serializable

  private val endpointName = "omni-broadcast-cache-" + UUID.randomUUID().toString
  @volatile private var driver: DriverState = _
  @volatile private var executor: ExecutorState = _
  private val discoveredKeys = new AtomicLong
  private val acceptedHolders = new AtomicLong

  private[omni] def executorRegistrationCounts: (Long, Long) =
    (discoveredKeys.get(), acceptedHolders.get())

  def startDriver(sc: SparkContext): Unit = synchronized {
    if (driver != null) return
    val state = new DriverState
    driver = state
    sc.addSparkListener(new SparkListener {
      override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
        case e: SparkListenerSQLExecutionStart => state.open(e.executionId.toString)
        case e: SparkListenerSQLExecutionEnd => state.retire(e.executionId.toString)
        case _ =>
      }
      override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
        state.removeExecutor(event.executorId)
      }
    })
  }

  /** Called while constructing the driver plan, before its tasks can run. */
  def openExecution(executionId: String): Unit = Option(driver).foreach(_.open(executionId))

  private[omni] def retireExecution(executionId: String): Unit = {
    Option(driver).foreach(_.retire(executionId))
  }

  private[omni] def cleanupAcks: Long = Option(driver).map(_.acknowledged).getOrElse(0L)

  private[omni] def cleanupFailures: Long = Option(driver).map(_.failed).getOrElse(0L)

  def onDriverMessage(message: AnyRef): Option[AnyRef] = message match {
    case m: RegisterExecutor =>
      Some(java.lang.Boolean.valueOf(Option(driver).exists(_.registerExecutor(m))))
    case m: RegisterHolder =>
      Some(java.lang.Boolean.valueOf(Option(driver).exists(_.registerHolder(m))))
    case _ => None
  }

  def stopDriver(): Unit = synchronized {
    val state = driver
    driver = null
    if (state != null) state.stop()
  }

  def startExecutor(pc: PluginContext): Unit = synchronized {
    if (executor != null) return
    discoveredKeys.set(0L)
    acceptedHolders.set(0L)
    val sparkEnv = SparkEnv.get
    // Spark executors may use a client-only RpcEnv with no advertised address.
    // Run the cache endpoint in a small server RpcEnv that the driver can reach.
    // ExecutorPlugin.init runs before BlockManagerId is assigned in local mode.
    val advertisedHost = Option(sparkEnv.blockManager)
      .flatMap(manager => Option(manager.blockManagerId))
      .map(_.host)
      .getOrElse(Utils.localCanonicalHostName())
    val cacheRpcEnv = RpcEnv.create(
      "omni-broadcast-cache",
      "0.0.0.0",
      advertisedHost,
      0,
      sparkEnv.conf,
      sparkEnv.securityManager,
      1,
      false)
    val incarnation = UUID.randomUUID().toString
    val endpoint = new RpcEndpoint {
      override val rpcEnv: RpcEnv = cacheRpcEnv
      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case RetireExecution(_, prefixes) =>
          try {
            prefixes.foreach(OmniBroadcastBuildSideCache.invalidate)
            context.reply(java.lang.Boolean.TRUE)
          } catch {
            case NonFatal(e) => context.sendFailure(e)
          }
      }
    }
    val ref = cacheRpcEnv.setupEndpoint(endpointName, endpoint)
    val uri = RpcEndpointAddress(cacheRpcEnv.address, ref.name).toString
    val state = new ExecutorState(pc, cacheRpcEnv, ref, incarnation)
    try {
      require(pc.ask(RegisterExecutor(pc.executorID(), incarnation, uri)) == java.lang.Boolean.TRUE,
        "Driver rejected Omni broadcast cache executor registration")
      executor = state
    } catch {
      case NonFatal(e) =>
        cacheRpcEnv.shutdown()
        throw e
    }
  }

  def stopExecutor(): Unit = synchronized {
    val state = executor
    executor = null
    if (state != null) state.rpcEnv.shutdown()
  }

  /** Register every native cache key before the kernel may create a build table. */
  def registerPlan(planBytes: Array[Byte]): Unit = {
    val state = executor
    if (state == null) return
    val keys = cacheKeys(Plan.parseFrom(planBytes))
    if (keys.isEmpty) return
    discoveredKeys.addAndGet(keys.size.toLong)
    val task = TaskContext.get()
    require(task != null, "Omni broadcast cache registration requires a Spark task")
    val executionId = task.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    require(executionId != null, "Omni broadcast cache registration requires a SQL execution")
    keys.foreach { key =>
      val segments = key.split(":", 4)
      require(segments.length == 4 && segments(2) == executionId,
        s"Omni broadcast cache key has wrong SQL execution: $key")
      val message = RegisterHolder(executionId, key, state.pc.executorID(), state.incarnation)
      require(state.pc.ask(message) == java.lang.Boolean.TRUE,
        s"Omni broadcast cache admission was rejected for SQL execution $executionId")
      acceptedHolders.incrementAndGet()
    }
  }

  private[omni] def cacheKeys(message: Message): Set[String] = {
    val own = message match {
      case join: JoinRel if join.hasAdvancedExtension &&
          join.getAdvancedExtension.hasOptimization =>
        val extension = join.getAdvancedExtension.getOptimization
        if (extension.is(classOf[StringValue])) {
          val content = extension.unpack(classOf[StringValue]).getValue
          content.split("\n").iterator
            .filter(_.startsWith("buildHashTableId=omni-bhj:"))
            .map(_.stripPrefix("buildHashTableId="))
            .toSet
        } else Set.empty[String]
      case _ => Set.empty[String]
    }
    own ++ message.getAllFields.values().asScala.flatMap {
      case child: Message => cacheKeys(child)
      case list: java.util.List[_] => list.asScala.collect { case child: Message => child }
          .flatMap(cacheKeys)
      case _ => Set.empty[String]
    }
  }

  private final class ExecutorState(
      val pc: PluginContext,
      val rpcEnv: RpcEnv,
      val ref: RpcEndpointRef,
      val incarnation: String)

  private final class DriverState {
    private val rpcEnv = SparkEnv.get.rpcEnv
    private val cleanupTimeout = new RpcTimeout(5.seconds, "omni.broadcast.cache.cleanupTimeout")
    @volatile private var stopped = false
    private val active = mutable.Map.empty[String, mutable.Map[String, mutable.Set[String]]]
    private val retired = mutable.Set.empty[String]
    private val executors = mutable.Map.empty[String, (String, String)]
    private var acknowledgedCount = 0L
    private var failedCount = 0L
    private val cleanup: ScheduledExecutorService =
      Executors.newSingleThreadScheduledExecutor { runnable =>
        val thread = new Thread(runnable, "omni-broadcast-cache-cleanup")
        thread.setDaemon(true)
        thread
      }

    def open(id: String): Unit = synchronized {
      if (id != null && !retired.contains(id)) {
        active.getOrElseUpdate(id, mutable.Map.empty)
      }
    }

    def registerExecutor(message: RegisterExecutor): Boolean = synchronized {
      if (stopped) return false
      if (executors.get(message.id).exists(_._1 != message.incarnation)) {
        active.values.foreach(_.remove(message.id))
      }
      executors(message.id) = (message.incarnation, message.uri)
      true
    }

    def registerHolder(message: RegisterHolder): Boolean = synchronized {
      val prefix = message.key.takeWhile(_ != '#')
      val parts = prefix.split(":", 4)
      if (retired.contains(message.executionId) ||
          stopped ||
          !active.contains(message.executionId) ||
          !executors.get(message.executorId).exists(_._1 == message.incarnation) ||
          parts.length != 4 || parts(0) != "omni-bhj" ||
          parts(2) != message.executionId) {
        false
      } else {
        active(message.executionId)
          .getOrElseUpdate(message.executorId, mutable.Set.empty) += prefix
        true
      }
    }

    def retire(id: String): Unit = {
      val targets = synchronized {
        if (stopped) return
        retired += id
        active.remove(id).toSeq.flatMap(_.toSeq).flatMap { case (executorId, prefixes) =>
          executors.get(executorId).map { case (incarnation, uri) =>
            (executorId, incarnation, uri, prefixes.toVector)
          }
        }
      }
      targets.foreach { case (executorId, incarnation, uri, prefixes) =>
        scheduleRetirement(id, executorId, incarnation, uri, prefixes, 0)
      }
    }

    private def scheduleRetirement(
        executionId: String,
        executorId: String,
        incarnation: String,
        uri: String,
        prefixes: Seq[String],
        attempt: Int): Unit = {
      cleanup.schedule(new Runnable {
        override def run(): Unit = {
          if (!DriverState.this.synchronized {
              executors.get(executorId).exists(_._1 == incarnation)
            }) return
          try {
            val ref = rpcEnv.setupEndpointRefByURI(uri)
            require(ref.askSync[Boolean](RetireExecution(executionId, prefixes), cleanupTimeout),
              "Executor rejected cache retirement")
            DriverState.this.synchronized { acknowledgedCount += 1 }
          } catch {
            case NonFatal(e) if attempt < 3 =>
              scheduleRetirement(executionId, executorId, incarnation, uri, prefixes, attempt + 1)
            case NonFatal(e) =>
              DriverState.this.synchronized { failedCount += 1 }
              logError(s"Omni broadcast cache retirement was not acknowledged by " +
                s"executor $executorId for SQL execution $executionId", e)
          }
        }
      }, if (attempt == 0) 0L else 1L << attempt, TimeUnit.SECONDS)
    }

    def removeExecutor(id: String): Unit = synchronized {
      executors.remove(id)
      active.values.foreach(_.remove(id))
    }

    def acknowledged: Long = synchronized { acknowledgedCount }

    def failed: Long = synchronized { failedCount }

    def stop(): Unit = synchronized {
      stopped = true
      cleanup.shutdownNow()
    }
  }
}
