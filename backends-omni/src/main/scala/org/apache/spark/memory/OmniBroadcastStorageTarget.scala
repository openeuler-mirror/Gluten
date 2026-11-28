/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package org.apache.spark.memory

import org.apache.gluten.memory.listener.ReservationListener
import org.apache.spark.SparkEnv
import org.apache.spark.storage.TempLocalBlockId

import java.util.UUID

/**
 * Executor-scoped reservation owner for retained Omni broadcast tables.
 *
 * Package placement is intentional: Spark's storage reservation methods are
 * package-private. Capture the owning MemoryManager once so a later SparkEnv
 * cannot silently receive the old executor's repayments.
 */
final class OmniBroadcastStorageTarget private (
    private val memoryManager: MemoryManager,
    val maxBytes: Long)
  extends ReservationListener {

  require(maxBytes > 0, "Broadcast cache limit must be positive")

  // Reservations share one executor-scoped owner; no block is materialized
  // in BlockManager. Reuse its identity instead of generating a UUID per allocation.
  private val reservationBlockId = TempLocalBlockId(UUID.randomUUID())

  private var reservedBytes: Long = 0L
  private var grantedBytesTotal: Long = 0L

  override def reserve(size: Long): Long = synchronized {
    require(size > 0, "Broadcast reservation must be positive")
    if (size > maxBytes - reservedBytes) {
      0L
    } else {
      if (memoryManager.acquireStorageMemory(reservationBlockId, size, MemoryMode.OFF_HEAP)) {
        reservedBytes += size
        grantedBytesTotal += size
        size
      } else {
        0L
      }
    }
  }

  override def unreserve(size: Long): Long = synchronized {
    require(size > 0, "Broadcast repayment must be positive")
    require(size <= reservedBytes, "Broadcast repayment exceeds outstanding reservations")
    memoryManager.releaseStorageMemory(size, MemoryMode.OFF_HEAP)
    reservedBytes -= size
    size
  }

  override def getUsedBytes(): Long = synchronized { reservedBytes }

  def totalGrantedBytes: Long = synchronized { grantedBytesTotal }
}

object OmniBroadcastStorageTarget {
  @volatile private var currentTarget: OmniBroadcastStorageTarget = _

  def currentTotalGrantedBytes: Long =
    Option(currentTarget).map(_.totalGrantedBytes).getOrElse(0L)

  def clearCurrent(): Unit = { currentTarget = null }

  def forCurrentExecutor(maxBytes: Long): OmniBroadcastStorageTarget = {
    val env = SparkEnv.get
    require(env != null, "SparkEnv must exist before broadcast cache initialization")
    require(
      env.conf.getBoolean("spark.memory.offHeap.enabled", false),
      "Broadcast cache requires Spark off-heap memory")
    val target = new OmniBroadcastStorageTarget(env.memoryManager, maxBytes)
    currentTarget = target
    target
  }
}
