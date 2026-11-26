/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
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

package org.apache.gluten.memory;

import org.apache.gluten.memory.listener.ReservationListener;

/**
 * JNI binding for the Omni backend's per-task `gluten::MemoryManager` (concretely
 * `gluten::OmniMemoryManager`, which owns the per-task `OmniTaskMemoryPool`).
 *
 * Mirrors {@link NativeMemoryManagerJniWrapper} from gluten-arrow — the
 * native methods are registered in `gluten/cpp-omni/src/jni/SparkJniWrapperMemory.cpp`
 * via JNI export names matching the canonical
 * `Java_org_apache_gluten_memory_OmniNativeMemoryManagerJniWrapper_*` pattern.
 *
 * JDK 8 compatible: plain class, private ctor, native decls only.
 *
 * @since 2026
 */
public class OmniNativeMemoryManagerJniWrapper {
    private OmniNativeMemoryManagerJniWrapper() {
    }

    /**
     * Creates the native memory manager of a task.
     *
     * @param backendType backend name the native factory is registered under
     * @param listener listener through which native reservations reach Spark
     * @param sessionConf serialized conf map with what the native construction reads: the
     *        reservation block size and the telemetry labels of the task
     * @param taskAttemptId Spark task attempt id, -1 outside a Spark task
     * @return handle of the native memory manager
     */
    public static native long create(
            String backendType, ReservationListener listener, byte[] sessionConf, long taskAttemptId);

    /**
     * Collects the memory usage statistics of a manager.
     *
     * @param handle handle returned by {@link #create}
     * @return the serialized usage statistics
     */
    public static native byte[] collectUsage(long handle);

    /**
     * Shrink entry of the shared MemoryManager contract, which Omni does not support.
     *
     * @param handle handle returned by {@link #create}
     * @param size bytes asked for
     * @return bytes released, always 0
     */
    public static native long shrink(long handle, long size);

    /**
     * Asks the native side to spill.
     *
     * @param handle handle returned by {@link #create}
     * @param size bytes to free
     * @return bytes the native side offers to repay, for the JVM to reconcile
     */
    public static native long spill(long handle, long size);

    /**
     * Holds a manager so that its pools outlive the objects still allocated from them.
     *
     * @param handle handle returned by {@link #create}
     */
    public static native void hold(long handle);

    /**
     * Reconciles Spark's actual repayment and retains any native offer that was not repaid.
     *
     * @param handle handle returned by {@link #create}
     * @param nativeFreed bytes the native side offered
     * @param repaid bytes Spark accepted
     */
    public static native void reconcileExternalRepay(long handle, long nativeFreed, long repaid);

    /**
     * Releases a manager.
     *
     * @param handle handle returned by {@link #create}
     */
    public static native void release(long handle);

    /**
     * Given a gluten::MemoryManager handle
     * (returned by {@link #create}), return the address of its embedded
     * `omniruntime::mem::OmniTaskMemoryPool` as a jlong. Used by callers in
     * `omni/bindings/` (e.g. {@code ShuffleHashHelper.computePartitionIds})
     * that need to bind allocations to the per-task pool but cannot link
     * against gluten/cpp/core types directly.
     *
     * Returns 0 if the handle is not an `gluten::OmniMemoryManager` (test
     * harness / mock); the caller MUST check and fall back to a legacy
     * unbound allocation path. Production (Spark/Gluten path) always
     * returns a valid pool address.
     *
     * @param handle handle returned by {@link #create}
     * @return address of the task pool, 0 when the manager has none
     */
    public static native long getPoolAddress(long handle);
}
