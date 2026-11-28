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
/*
 * OmniSparkAllocationListener header.
 *
 * Promoted from SparkJniWrapperMemory.cpp (LocalSparkAllocationListener).
 * The .cpp body adds:
 *   ExceptionClear before propagating a typed denial or fatal callback failure
 *   Deferred delivery when GetEnv() reports a detached thread
 *
 * Clearing the Java exception before C++ unwind prevents a pending exception from
 * crashing the JVM; never attaching JIT threads avoids attach + ClassNotFound crashes.
 */
#pragma once

#include <jni.h>
#include <atomic>
#include "memory/AllocationListener.h"   // gluten::AllocationListener base
#include "JniGlobalRef.h"  // RAII owner for the Java listener reference

// NOTE: JniCommon.h is deliberately NOT included — transitive arrow/shuffle
// headers from JniCommon.h break the build under omnioperator-euler:v2.
// The exception bridge clears pending Java exceptions before C++ unwind.

namespace gluten {

class OmniSparkAllocationListener final : public AllocationListener {
 public:
    OmniSparkAllocationListener(JavaVM* vm, jobject jListenerLocalRef,
                                jmethodID jReserveMethod, jmethodID jUnreserveMethod);
    ~OmniSparkAllocationListener() override = default;  // RAII handles teardown

    OmniSparkAllocationListener(const OmniSparkAllocationListener&) = delete;
    OmniSparkAllocationListener& operator=(const OmniSparkAllocationListener&) = delete;
    OmniSparkAllocationListener(OmniSparkAllocationListener&&) = delete;
    OmniSparkAllocationListener& operator=(OmniSparkAllocationListener&&) = delete;

    // Signed delta: only typed Spark OOM becomes ReservationDenied; other failures propagate.
    void allocationChanged(int64_t size) override;

    // Ledger-only credit for an already completed MemoryTarget.repay().
    void reconcileExternalRepay(int64_t bytes);

    int64_t currentBytes() override { return usedBytes_.load(); }
    int64_t peakBytes()    override { return peakBytes_.load(); }

 private:
    jlong invokeReservation(JNIEnv* env, int64_t size);
    JavaVM* vm_;
    JniGlobalRef listenerRef_;  // RAII replaces bare jobject
    JniGlobalRef reservationDeniedClass_;
    jmethodID throwableToString_{nullptr};
    const jmethodID jReserveMethod_;
    const jmethodID jUnreserveMethod_;
    std::atomic<int64_t> usedBytes_{0};
    std::atomic<int64_t> peakBytes_{0};
    // Delta deferred when allocationChanged runs on a JVM-detached thread (JIT codegen /
    // LLVM ORC compile thread) where the reserve/unreserve JNI upcall is impossible.
    // Instead of dropping that delta — which left the per-task pool and Spark TMM
    // permanently out of sync — we accumulate it here as a single signed running total and
    // replay it on the next allocationChanged that lands on an attached thread. See the
    // "accumulate-and-replay" comments in OmniSparkAllocationListener.cpp for the mechanism.
    std::atomic<int64_t> pendingDelta_{0};
};

} // namespace gluten
