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
package org.apache.gluten.execution

import org.apache.gluten.vectorized.OmniPlanEvaluatorJniWrapper

import org.apache.spark.internal.Logging

/**
 * Manages the lifecycle of the executor-level BHJ hash table cache on the native side.
 *
 * The native C++ `BroadcastHashTableCache` singleton holds hash tables keyed by
 * `buildHashTableId`. This object provides the Scala-side entry point for cache
 * invalidation, which must be called when Spark unpersists a broadcast relation so
 * that the native memory is freed in a timely manner.
 */
object OmniBroadcastBuildSideCache extends Logging {

  /**
   * Notify the native cache to free the hash table associated with the given id.
   * Safe to call even if no entry exists for the id (no-op on the native side).
   *
   * @param buildHashTableId the unique id returned by [[BroadcastHashJoinExecTransformerBase.buildHashTableId]]
   */
  def invalidate(buildHashTableId: String): Unit = {
    if (buildHashTableId == null || buildHashTableId.isEmpty) {
      return
    }
    logDebug(s"[OmniBHJCache] Invalidating native hash table cache for id: $buildHashTableId")
    try {
      OmniPlanEvaluatorJniWrapper.nativeInvalidateBroadcastHashTable(buildHashTableId)
    } catch {
      case e: Exception =>
        logWarning(
          s"[OmniBHJCache] Failed to invalidate native hash table for id: $buildHashTableId", e)
    }
  }
}
