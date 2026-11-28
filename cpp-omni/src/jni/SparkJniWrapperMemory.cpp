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
 *
 * ---
 *
 * JNI entry points for OmniNativeMemoryManager.
 *
 * This file holds:
 *   1. DecodeNmmHandleOrThrow — the nmmHandle decode helper that the sibling
 *      SparkJniWrapper.cpp::createRuntime calls to obtain the per-task
 *      gluten::MemoryManager*.
 *   2. OmniNativeMemoryManagerJniWrapper lifecycle, usage and repayment exports.
 *
 * Why a separate TU:
 *   Including gluten/cpp/core/jni/JniCommon.h transitively pulls
 *   compute/Runtime.h (gluten's), operators/writer/ArrowWriter.h, shuffle/...
 *   etc. — and the per-file INCLUDE_DIRECTORIES that makes those resolvable
 *   also shadows OmniOperator's compute/ResultIterator.h chain in every
 *   sibling TU. By splitting into this dedicated .cpp and keeping cpp/core
 *   reach localized to only the small set of headers we strictly need
 *   (memory/MemoryManager.h, memory/AllocationListener.h, memory.pb.h —
 *   none of which pull `compute` or `shuffle` subtrees), the shadow trap is avoided.
 *
 * NB: We deliberately do NOT include gluten/cpp/core/jni/JniCommon.h or
 * cpp-omni/src/jni/jni_common.h here. The former drags in
 * operators/writer/ArrowWriter.h (which hits an Arrow API mismatch in the
 * build image); the latter pulls cpp-omni's compute/Runtime.h which would
 * collide with the gluten cpp/core compute/Runtime.h under our include path.
 * The few helpers we need (jniCastOrThrow, SparkAllocationListener) are
 * inlined locally below.
 *
 * Lifecycle: the create() return value is an opaque jlong owned by the
 * Java-side OmniNativeMemoryManager.Impl. TaskResources guarantees release()
 * is called at task end (priority=0, after the Runtime which is priority=20).
 */

#include "ScopedJniResources.h"
#include <jni.h>
#include <atomic>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

// Gluten cpp/core headers — resolved via per-file INCLUDE_DIRECTORIES on this
// TU in cpp-omni/src/CMakeLists.txt. Each of these is carefully selected to
// avoid the transitive arrow / shuffle chain that fails to compile under the
// omnioperator-euler:v2 arrow version.
#include "memory/MemoryManager.h"           // gluten::MemoryManager, MemoryManager::create/release
#include "memory/AllocationListener.h"      // gluten::AllocationListener (base for our local SparkListener)
#include "memory.pb.h"                       // gluten::MemoryUsageStats — wire format for collectUsage
#include "config.pb.h"                       // gluten::ConfigMap — NMM creation settings

// Promoted listener (replaces inline LocalSparkAllocationListener).
#include "OmniSparkAllocationListener.h"

// Forward-decl-only bridge so we can
// reach OmniTaskMemoryPool* from this TU without pulling the full omni-core
// memory header (which would conflict with cpp/core in this TU's include
// scope). RuntimeMemoryHelper.h is forward-decl only; backend-kind validation
// and the checked static cast live in OmniMemoryManager.cc.
#include "memory/RuntimeMemoryHelper.h"

// The cpp-omni runtime-exception class symbol — defined in cpp-omni/src/jni/jni_common.cpp,
// linked into the same .so. extern-declared here to avoid pulling the full
// jni_common.h chain.
extern jclass runtimeExceptionClass;

// ============================================================================
// Locally-inlined helpers (deliberately not pulled from gluten cpp/core
// JniCommon.h — that header has transitive arrow/shuffle includes that trip
// the omnioperator-euler:v2 arrow API mismatch).
// ============================================================================

namespace {


// Mirror of gluten::jniCastOrThrow from gluten/cpp/core/jni/JniCommon.h:126-130.
// Throws std::runtime_error (caught by the catch handler below) on null handle.
template <typename T>
T *JniCastOrThrowOmni(jlong handle) {
    if (handle <= 0) {
        throw std::runtime_error("Invalid Omni native memory manager handle");
    }
    return reinterpret_cast<T *>(handle);
}

// Lazy-initialized JNI handles for ReservationListener.reserve/unreserve.
// Kept TU-local; first call to create() runs the init exactly once.
jclass g_reservationListenerClass = nullptr;
jmethodID g_reserveMethod = nullptr;
jmethodID g_unreserveMethod = nullptr;

// Drive the one-time init through std::call_once so every caller observes a
// fully-initialized set of globals (class + both method ids) atomically and
// exactly once. call_once establishes a happens-before from the body's
// completion to every other caller's return, which both fixes the weak-memory
// publication race (a racing first-use could otherwise see class != null with a
// still-null method id and call CallLongMethod with a null jmethodID) and
// prevents a double-init from leaking a second global ref.
std::once_flag g_reservationListenerInitFlag;
bool g_reservationListenerInitOk = false;

bool EnsureReservationListenerInit(JNIEnv *env) {
    std::call_once(g_reservationListenerInitFlag, [env]() {
        jclass localCls = env->FindClass("org/apache/gluten/memory/listener/ReservationListener");
        if (localCls == nullptr) {
            gluten::ThrowJavaExceptionIfNone(env, runtimeExceptionClass,
                "Failed to FindClass org/apache/gluten/memory/listener/ReservationListener");
            return;
        }
        // NewGlobalRef may return nullptr (JNI global-ref table exhausted).
        // Verify before using it as a class.
        jclass globalCls = static_cast<jclass>(env->NewGlobalRef(localCls));
        env->DeleteLocalRef(localCls);
        if (globalCls == nullptr) {
            gluten::ThrowJavaExceptionIfNone(env, runtimeExceptionClass,
                "NewGlobalRef(ReservationListener) failed — JNI global ref table exhausted");
            return;
        }
        jmethodID reserveM = env->GetMethodID(globalCls, "reserve", "(J)J");
        jmethodID unreserveM = reserveM == nullptr ? nullptr : env->GetMethodID(globalCls, "unreserve", "(J)J");
        if (reserveM == nullptr || unreserveM == nullptr) {
            // Preserve the original lookup exception; do not call more JNI lookups while pending.
            env->DeleteGlobalRef(globalCls);
            gluten::ThrowJavaExceptionIfNone(env, runtimeExceptionClass,
                "ReservationListener.reserve/unreserve method id lookup failed");
            return;
        }
        // Publish to the globals only after every step succeeded; the success
        // flag is published last and gates every reader via call_once.
        g_reservationListenerClass = globalCls;
        g_reserveMethod = reserveM;
        g_unreserveMethod = unreserveM;
        g_reservationListenerInitOk = true;
    });
    if (!g_reservationListenerInitOk && !env->ExceptionCheck()) {
        gluten::ThrowJavaExceptionIfNone(env, runtimeExceptionClass, "ReservationListener JNI initialization previously failed");
    }
    return g_reservationListenerInitOk;
}

// Value of `key` in the conf sent by the JVM, or `fallback` when it is absent or empty.
std::string ConfValueOr(const gluten::ConfigMap &conf, const char *key, const std::string &fallback)
{
    const auto it = conf.configs().find(key);
    return (it == conf.configs().end() || it->second.empty()) ? fallback : it->second;
}

} // namespace

// ============================================================================
// Decode helper invoked from sibling SparkJniWrapper.cpp::createRuntime.
// ============================================================================

gluten::MemoryManager *DecodeNmmHandleOrThrow(JNIEnv * /*env*/, jlong nmmHandle)
{
    return JniCastOrThrowOmni<gluten::MemoryManager>(nmmHandle);
}

// ============================================================================
// OmniNativeMemoryManagerJniWrapper JNI suite (matches the Java class).
// All bodies use a local try/catch to translate C++ exceptions into Java
// RuntimeException — mirrors cpp-omni's JNI_FUNC_START/_END pattern but
// inlined so we don't have to pull jni_common.h (which would pull
// compute/Runtime.h and collide with the gluten cpp/core compute/Runtime.h on
// our include path).
// ============================================================================

extern "C" {

JNIEXPORT jlong JNICALL Java_org_apache_gluten_memory_OmniNativeMemoryManagerJniWrapper_create(
    JNIEnv *env,
    jclass,
    jstring jBackendType,
    jobject jListener,
    jbyteArray jSessionConf,
    jlong jTaskAttemptId)
{
    try {
        if (!EnsureReservationListenerInit(env)) {
            return 0L;
        }

        JavaVM *vm = nullptr;
        if (env->GetJavaVM(&vm) != JNI_OK) {
            gluten::ThrowJavaExceptionIfNone(env, runtimeExceptionClass, "Unable to get JavaVM instance");
            return -1L;
        }

        // A null jstring passed to GetStringUTFChars dereferences a null
        // jstring inside the JNI runtime — a SIGSEGV, not a catchable
        // std::exception, so the surrounding try/catch cannot protect it.
        if (jBackendType == nullptr) {
            gluten::ThrowJavaExceptionIfNone(env, runtimeExceptionClass, "backendType argument must not be null");
            return -1L;
        }
        // Pass nullptr (not JNI_FALSE) for the jboolean* isCopy parameter.
        const char *backendTypeC = env->GetStringUTFChars(jBackendType, nullptr);
        // GetStringUTFChars can return nullptr on OOM (pending
        // OutOfMemoryError); constructing std::string from null is UB.
        if (backendTypeC == nullptr) {
            gluten::ThrowJavaExceptionIfNone(env, runtimeExceptionClass, "GetStringUTFChars failed (OOM)");
            return -1L;
        }
        std::string backendType(backendTypeC);
        env->ReleaseStringUTFChars(jBackendType, backendTypeC);

        // The JVM sends only the settings NMM construction needs (not the
        // whole session conf): spark.gluten.memory.reservationBlockSize and the
        // telemetry labels of the task (omni.telemetry.run_id / qid / round).
        int64_t reservationBlockSize = 0;
        std::string runId = "unknown";
        std::string queryId = "unknown";
        std::string round = "unknown";
        if (jSessionConf != nullptr) {
            const jsize length = env->GetArrayLength(jSessionConf);
            std::vector<jbyte> bytes(static_cast<size_t>(length));
            env->GetByteArrayRegion(jSessionConf, 0, length, bytes.data());
            gluten::ConfigMap conf;
            if (length > 0 && !conf.ParseFromArray(bytes.data(), length)) {
                gluten::ThrowJavaExceptionIfNone(env, runtimeExceptionClass, "Malformed memory manager conf");
                return -1L;
            }
            const auto it = conf.configs().find("spark.gluten.memory.reservationBlockSize");
            if (it != conf.configs().end()) {
                reservationBlockSize = std::stoll(it->second);
            }
            runId = ConfValueOr(conf, "omni.telemetry.run_id", runId);
            queryId = ConfValueOr(conf, "omni.telemetry.qid", queryId);
            round = ConfValueOr(conf, "omni.telemetry.round", round);
        }

        // Use promoted gluten::OmniSparkAllocationListener (ExceptionClear +
        // GetEnv short-circuit discipline).
        auto listener = std::make_unique<gluten::OmniSparkAllocationListener>(
                vm, jListener, g_reserveMethod, g_unreserveMethod);

        // Dispatches through the factory registered by OmniMemoryManagerRegistrar
        // at libspark_columnar_plugin.so load time. Returns a
        // gluten::MemoryManager* whose dynamic type is gluten::OmniMemoryManager.
        auto* sparkListener = listener.get();
        std::unique_ptr<gluten::MemoryManager, decltype(&gluten::MemoryManager::release)> mm(
            gluten::MemoryManager::create(backendType, std::move(listener)),
            &gluten::MemoryManager::release);
        omniruntime::BindOmniSparkListener(mm.get(), sparkListener);
        if (reservationBlockSize > 0) {
            omniruntime::SetOmniReservationBlockSize(mm.get(), reservationBlockSize);
        }
        omniruntime::SetOmniTaskTelemetryContext(
            mm.get(),
            runId,
            queryId,
            round,
            static_cast<int64_t>(jTaskAttemptId));
        return reinterpret_cast<jlong>(mm.release());
    } catch (const std::exception &e) {
        gluten::ThrowJavaExceptionIfNone(env, runtimeExceptionClass, e.what());
        return -1L;
    } catch (...) {
        gluten::ThrowJavaExceptionIfNone(env, runtimeExceptionClass, "Unknown native memory manager failure");
        return -1L;
    }
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_gluten_memory_OmniNativeMemoryManagerJniWrapper_collectUsage(
    JNIEnv *env, jclass, jlong nmmHandle)
{
    try {
        auto *memoryManager = JniCastOrThrowOmni<gluten::MemoryManager>(nmmHandle);
        const gluten::MemoryUsageStats &stats = memoryManager->collectMemoryUsageStats();
        auto size = stats.ByteSizeLong();
        // ByteSizeLong() is size_t; it is narrowed to jsize (signed 32-bit)
        // below. The flat-pool stats never overflow, but a future
        // per-spillable-children proto could — a silent truncation to
        // a negative jsize would corrupt the byte array. Bound it defensively.
        if (size > static_cast<size_t>(std::numeric_limits<jsize>::max())) {
            gluten::ThrowJavaExceptionIfNone(env, runtimeExceptionClass,
                "Omni memory usage stats proto exceeds maximum jbyteArray size");
            return nullptr;
        }
        jbyteArray out = env->NewByteArray(static_cast<jsize>(size));
        // NewByteArray returns nullptr on allocation failure and leaves a
        // pending OutOfMemoryError; SetByteArrayRegion on a null array is UB.
        if (out == nullptr) {
            return nullptr;   // OutOfMemoryError already pending from NewByteArray
        }
        std::vector<uint8_t> buffer(size);
        if (!stats.SerializeToArray(reinterpret_cast<void *>(buffer.data()), static_cast<int>(size))) {
            gluten::ThrowJavaExceptionIfNone(env, runtimeExceptionClass,
                "Serialization failed when collecting Omni memory usage stats");
            return nullptr;
        }
        env->SetByteArrayRegion(out, 0, static_cast<jsize>(size),
            reinterpret_cast<jbyte *>(buffer.data()));
        return out;
    } catch (const std::exception &e) {
        gluten::ThrowJavaExceptionIfNone(env, runtimeExceptionClass, e.what());
        return nullptr;
    } catch (...) {
        gluten::ThrowJavaExceptionIfNone(env, runtimeExceptionClass, "Unknown native memory manager failure");
        return nullptr;
    }
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_memory_OmniNativeMemoryManagerJniWrapper_shrink(
    JNIEnv *, jclass, jlong, jlong)
{
    return 0;
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_memory_OmniNativeMemoryManagerJniWrapper_spill(
    JNIEnv *env, jclass, jlong nmmHandle, jlong size)
{
    try {
        auto *memoryManager = JniCastOrThrowOmni<gluten::MemoryManager>(nmmHandle);
        return static_cast<jlong>(omniruntime::SpillOmniMemory(memoryManager, static_cast<int64_t>(size)));
    } catch (const std::exception &e) {
        gluten::ThrowJavaExceptionIfNone(env, runtimeExceptionClass, e.what());
        return 0;
    } catch (...) {
        gluten::ThrowJavaExceptionIfNone(env, runtimeExceptionClass, "Unknown native memory manager failure");
        return 0;
    }
}

JNIEXPORT void JNICALL Java_org_apache_gluten_memory_OmniNativeMemoryManagerJniWrapper_hold(
    JNIEnv *env, jclass, jlong nmmHandle)
{
    try {
        auto *memoryManager = JniCastOrThrowOmni<gluten::MemoryManager>(nmmHandle);
        memoryManager->hold();
    } catch (const std::exception &e) {
        gluten::ThrowJavaExceptionIfNone(env, runtimeExceptionClass, e.what());
    } catch (...) {
        gluten::ThrowJavaExceptionIfNone(env, runtimeExceptionClass, "Unknown native memory manager failure");
    }
}

JNIEXPORT void JNICALL Java_org_apache_gluten_memory_OmniNativeMemoryManagerJniWrapper_reconcileExternalRepay(
    JNIEnv *env, jclass, jlong nmmHandle, jlong nativeFreed, jlong repaid)
{
    try {
        auto *memoryManager = JniCastOrThrowOmni<gluten::MemoryManager>(nmmHandle);
        omniruntime::ReconcileExternalRepay(
            memoryManager, static_cast<int64_t>(nativeFreed), static_cast<int64_t>(repaid));
    } catch (const std::exception &e) {
        gluten::ThrowJavaExceptionIfNone(env, runtimeExceptionClass, e.what());
    } catch (...) {
        gluten::ThrowJavaExceptionIfNone(env, runtimeExceptionClass, "Unknown native memory manager failure");
    }
}

JNIEXPORT void JNICALL Java_org_apache_gluten_memory_OmniNativeMemoryManagerJniWrapper_release(
    JNIEnv *env, jclass, jlong nmmHandle)
{
    try {
        auto *memoryManager = JniCastOrThrowOmni<gluten::MemoryManager>(nmmHandle);
        gluten::MemoryManager::release(memoryManager);
    } catch (const std::exception &e) {
        gluten::ThrowJavaExceptionIfNone(env, runtimeExceptionClass, e.what());
    } catch (...) {
        gluten::ThrowJavaExceptionIfNone(env, runtimeExceptionClass, "Unknown native memory manager failure");
    }
}

// Expose the embedded OmniTaskMemoryPool*
// as a jlong. Callers in omni/bindings/ (which cannot link against gluten core
// types) decode this opaque address back to `omniruntime::mem::OmniTaskMemoryPool*`
// via reinterpret_cast and use it to bind allocations to the per-task pool.
// Returns 0 if the manager is not a gluten::OmniMemoryManager (test harness /
// mock); production Spark/Gluten path always returns a non-zero pool address.
JNIEXPORT jlong JNICALL Java_org_apache_gluten_memory_OmniNativeMemoryManagerJniWrapper_getPoolAddress(
    JNIEnv *env, jclass, jlong nmmHandle)
{
    try {
        // Reuse the existing free-function bridge (decl in RuntimeMemoryHelper.h,
        // defn in OmniMemoryManager.cc). Null-tolerant by contract — returns
        // nullptr when the manager is not a gluten::OmniMemoryManager.
        auto *mm = JniCastOrThrowOmni<gluten::MemoryManager>(nmmHandle);
        omniruntime::mem::OmniTaskMemoryPool *pool = omniruntime::ToOmniTaskPoolOrNull(mm);
        return reinterpret_cast<jlong>(pool);
    } catch (const std::exception &e) {
        gluten::ThrowJavaExceptionIfNone(env, runtimeExceptionClass, e.what());
        return 0;
    } catch (...) {
        gluten::ThrowJavaExceptionIfNone(env, runtimeExceptionClass, "Unknown native memory manager failure");
        return 0;
    }
}

} // extern "C"
