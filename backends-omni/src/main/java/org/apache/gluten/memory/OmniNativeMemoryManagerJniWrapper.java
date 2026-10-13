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
package org.apache.gluten.memory;

import org.apache.gluten.memory.listener.ReservationListener;

/**
 * JNI binding for the Omni backend's per-task `gluten::MemoryManager` (concretely
 * `gluten::OmniMemoryManager`, which owns the per-task `OmniTaskMemoryPool`).
 *
 * Mirrors {@link NativeMemoryManagerJniWrapper} from gluten-arrow exactly — the
 * native methods are registered in `gluten/cpp-omni/src/jni/SparkJniWrapper.cpp`
 * via JNI export names matching the canonical
 * `Java_org_apache_gluten_memory_OmniNativeMemoryManagerJniWrapper_*` pattern.
 *
 * JDK 8 compatible: plain class, private ctor, native decls only.
 */
public class OmniNativeMemoryManagerJniWrapper {
    private OmniNativeMemoryManagerJniWrapper() {}

    public static native long create(
        String backendType,
        ReservationListener listener,
        byte[] sessionConf,
        String runId,
        String queryId,
        String round,
        long taskAttemptId);

    public static native byte[] collectUsage(long handle);

    /** The shared MemoryManager SHRINK contract is unsupported by Omni. */
    public static native long shrink(long handle, long size);

    /** Explicit Omni SPILL request; returns the native repayment offer for JVM reconciliation. */
    public static native long spill(long handle, long size);

    public static native void hold(long handle);

    /** Reconcile Spark's actual repayment and retain any unrepaid native offer. */
    public static native void reconcileExternalRepay(
        long handle, long nativeFreed, long repaid);

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
     */
    public static native long getPoolAddress(long handle);
}
