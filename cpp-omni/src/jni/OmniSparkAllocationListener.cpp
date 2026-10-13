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
 * OmniSparkAllocationListener implementation.
 *
 * gluten::AllocationListener subclass with signed-delta allocationChanged.
 * Pending Java exceptions are cleared before C++ unwind. Only typed Spark
 * reservation denials are advisory; fatal callback failures retain their message.
 * vm_->GetEnv() != JNI_OK on a detached thread (JIT codegen / LLVM ORC compile thread)
 * makes the reserve/unreserve JNI upcall impossible at allocationChanged entry. Rather
 * than DROPPING the delta (which left the per-task pool and Spark TMM permanently out of
 * sync), the delta is parked in pendingDelta_ and replayed on the next allocationChanged
 * that runs on an attached thread — see "accumulate-and-replay" in allocationChanged.
 *
 * NOTE: JniCommon.h is deliberately NOT included.
 * The ExceptionClear + GetEnv disciplines are inlined here instead.
 */

#include "OmniSparkAllocationListener.h"
#include "memory/reservation_denied.h"
#include <stdexcept>

#ifdef OMNI_MEM_DEBUG
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#endif

namespace {

// Resolves the JNIEnv for the ctor thread. Assumption: the ctor runs on an
// already-attached Spark task thread, so GetEnv must succeed. We deliberately do
// NOT attach the thread here: a daemon attach performed by this ctor would never
// be detached (the listener owns no thread lifecycle), leaking the attach for the
// life of the process. If that assumption is ever violated (e.g. NMM created lazily
// from a non-task thread) we fail loudly instead of silently leaking.
inline void ResolveAttachedEnvOrThrow(JavaVM* vm, JNIEnv** out) {
    int getEnvStat = vm->GetEnv(reinterpret_cast<void**>(out), JNI_VERSION_1_8);
    if (getEnvStat == JNI_EDETACHED) {
        throw std::runtime_error(
            "OmniSparkAllocationListener ctor ran on a detached thread — "
            "must be constructed on an attached Spark task thread.");
    }
    if (getEnvStat != JNI_OK) {
        throw std::runtime_error("Failed to obtain JNIEnv for current thread.");
    }
}

} // anonymous namespace

namespace gluten {

#ifdef OMNI_MEM_DEBUG
// OMNI_MEM_DEBUG-only listener reconciliation counters (REVERTIBLE diagnostic). Process-global
// (per-query-fresh-process => per-query). Splits the pool->Spark delivery gap into its
// mechanisms: partial grants (granted<requested, no exception — the silent pool/Spark
// divergence), hard rejects (MEM_CAP throw), and the detached-thread path. The latter is now
// "defer" (delta parked in pendingDelta_, NOT dropped) + "replay" (a parked delta later
// delivered to Spark on an attached thread; replay_reject = that delivery was denied and the
// delta was re-parked for a future retry). Dumped once at process exit.
namespace {
std::atomic<int64_t> g_lsnReserveCalls{0}, g_lsnReserveReq{0}, g_lsnReserveGranted{0};
std::atomic<int64_t> g_lsnUnreserveCalls{0}, g_lsnUnreserveReq{0}, g_lsnUnreserveDone{0};
std::atomic<int64_t> g_lsnPartialCount{0}, g_lsnPartialGap{0};
std::atomic<int64_t> g_lsnRejectCount{0};
std::atomic<int64_t> g_lsnDetachedDeferCount{0}, g_lsnDetachedDeferReserveBytes{0}, g_lsnDetachedDeferUnreserveBytes{0};
std::atomic<int64_t> g_lsnDeferReplayCount{0}, g_lsnDeferReplayBytes{0}, g_lsnDeferReplayRejectCount{0};
std::once_flag g_lsnInitFlag;
void DumpListener()
{
    fprintf(stderr,
        "[MEMPROBE-LISTENER] reserve_calls=%lld reserve_req=%lld reserve_granted=%lld "
        "partial_count=%lld partial_gap=%lld reject_count=%lld "
        "unreserve_calls=%lld unreserve_req=%lld unreserve_done=%lld "
        "detached_defer_count=%lld detached_defer_reserve_bytes=%lld detached_defer_unreserve_bytes=%lld "
        "defer_replay_count=%lld defer_replay_bytes=%lld defer_replay_reject_count=%lld\n",
        (long long)g_lsnReserveCalls.load(), (long long)g_lsnReserveReq.load(), (long long)g_lsnReserveGranted.load(),
        (long long)g_lsnPartialCount.load(), (long long)g_lsnPartialGap.load(), (long long)g_lsnRejectCount.load(),
        (long long)g_lsnUnreserveCalls.load(), (long long)g_lsnUnreserveReq.load(), (long long)g_lsnUnreserveDone.load(),
        (long long)g_lsnDetachedDeferCount.load(), (long long)g_lsnDetachedDeferReserveBytes.load(),
        (long long)g_lsnDetachedDeferUnreserveBytes.load(),
        (long long)g_lsnDeferReplayCount.load(), (long long)g_lsnDeferReplayBytes.load(),
        (long long)g_lsnDeferReplayRejectCount.load());
    fflush(stderr);
}
} // namespace
#endif

OmniSparkAllocationListener::OmniSparkAllocationListener(
        JavaVM* vm, jobject jListenerLocalRef,
        jmethodID jReserveMethod, jmethodID jUnreserveMethod)
    : vm_(vm), jReserveMethod_(jReserveMethod), jUnreserveMethod_(jUnreserveMethod) {
    JNIEnv* env = nullptr;
    // The ctor runs on the Spark task thread, which is attached.
    // Only allocationChanged switches to GetEnv-only.
    ResolveAttachedEnvOrThrow(vm_, &env);
    listenerRef_ = JniGlobalRef(vm_, env, jListenerLocalRef);  // RAII ctor handles NewGlobalRef
    // NewGlobalRef returns nullptr when the JNI global-ref table is
    // exhausted. Fail loudly instead of constructing a
    // half-dead listener whose first allocationChanged would CallLongMethod
    // on a null jobject (UB / JVM crash).
    if (jListenerLocalRef != nullptr && !listenerRef_) {
        throw std::runtime_error(
            "NewGlobalRef(ReservationListener) failed — JNI global ref table exhausted");
    }
    if (listenerRef_) {
        jclass denied = env->FindClass(
            "org/apache/gluten/memory/memtarget/ThrowOnOomMemoryTarget$OutOfMemoryException");
        if (denied == nullptr) {
            env->ExceptionClear();
            throw std::runtime_error("Cannot resolve Spark reservation-denial class");
        }
        reservationDeniedClass_ = JniGlobalRef(vm_, env, denied);
        env->DeleteLocalRef(denied);
        jclass throwable = env->FindClass("java/lang/Throwable");
        if (throwable != nullptr) {
            throwableToString_ = env->GetMethodID(throwable, "toString", "()Ljava/lang/String;");
            env->DeleteLocalRef(throwable);
        }
        if (!reservationDeniedClass_ || throwableToString_ == nullptr || env->ExceptionCheck()) {
            env->ExceptionClear();
            throw std::runtime_error("Cannot initialize Spark reservation exception bridge");
        }
    }
}
// Dtor is = default (declared in header). JniGlobalRef listenerRef_ handles DeleteGlobalRef
// via its own GetEnv-guarded destructor.

jlong OmniSparkAllocationListener::invokeReservation(JNIEnv* env, int64_t size) {
    const jlong granted = size < 0
        ? -env->CallLongMethod(listenerRef_.get(), jUnreserveMethod_, -size)
        : env->CallLongMethod(listenerRef_.get(), jReserveMethod_, size);
    if (!env->ExceptionCheck()) return granted;

    // Clear before any further JNI call or C++ unwind, while retaining the
    // throwable long enough to classify it and preserve its original message.
    jthrowable failure = env->ExceptionOccurred();
    env->ExceptionClear();
    const bool denied = size > 0 && failure != nullptr && env->IsInstanceOf(
        failure, static_cast<jclass>(reservationDeniedClass_.get()));
#ifdef OMNI_MEM_DEBUG
    g_lsnRejectCount.fetch_add(1, std::memory_order_relaxed);
#endif
    if (denied) {
        // Admission denial is an expected self-spill signal. Do not format or
        // copy the JVM's potentially large memory-tree diagnostic across JNI.
        env->DeleteLocalRef(failure);
        throw omniruntime::mem::ReservationDenied(
            "Spark denied reservation of " + std::to_string(size) + " bytes");
    }
    std::string description = "Java reservation callback failed";
    if (failure != nullptr) {
        auto detail = static_cast<jstring>(env->CallObjectMethod(failure, throwableToString_));
        if (!env->ExceptionCheck() && detail != nullptr) {
            const char* utf = env->GetStringUTFChars(detail, nullptr);
            if (utf != nullptr) {
                description = utf;
                env->ReleaseStringUTFChars(detail, utf);
            }
        }
        if (env->ExceptionCheck()) env->ExceptionClear();
        if (detail != nullptr) env->DeleteLocalRef(detail);
        env->DeleteLocalRef(failure);
    }
    throw std::runtime_error(description);
}

void OmniSparkAllocationListener::reconcileExternalRepay(int64_t bytes) {
    if (bytes < 0) throw std::invalid_argument("Negative external repayment");
    int64_t current = usedBytes_.load(std::memory_order_acquire);
    do {
        if (bytes > current) throw std::underflow_error("External repayment exceeds Spark ledger");
    } while (!usedBytes_.compare_exchange_weak(current, current - bytes,
                std::memory_order_acq_rel, std::memory_order_acquire));
}

void OmniSparkAllocationListener::allocationChanged(int64_t size) {
#ifdef OMNI_MEM_DEBUG
    std::call_once(g_lsnInitFlag, []() { std::atexit(DumpListener); });
#endif

    // ── No listener bound: no-op grant ───────────────────────────────────────────────
    // The ctor permits construction with a null jListener (no-pool / test discipline). With
    // no listener there is no Spark TaskMemoryManager to reserve against, so every JNI upcall
    // below would be CallLongMethod(nullptr, ...) → SIGSEGV. Short-circuit before any upcall:
    // treat the change as an unconditionally granted no-op, consistent with the no-pool
    // fallback. Parking the delta would be pointless — there is no listener to replay it to.
    if (!listenerRef_) {
        return;
    }

    // ── Detached-thread path: accumulate, do NOT drop ────────────────────────────────
    // allocationChanged can run on a thread with no attached JNIEnv — JIT codegen / LLVM
    // ORC compile threads allocate while the owning Spark task thread is elsewhere, so the
    // reserve/unreserve JNI upcall cannot be made here. The previous code just returned,
    // DROPPING the delta: the per-task pool had already charged it (OmniTaskMemoryPool::
    // AddMemory runs before the listener), so the pool's view and Spark's TaskMemoryManager
    // drifted apart with no log or metric. Instead we park the delta in pendingDelta_ — a
    // single signed running total, so a later +X and -X naturally cancel and we only ever
    // owe Spark the net — and replay it below on the next call that reaches an attached
    // thread. pendingDelta_ is atomic because allocationChanged is called concurrently.
    JNIEnv* env = nullptr;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8) != JNI_OK) {
        if (size != 0) {
            pendingDelta_.fetch_add(size, std::memory_order_acq_rel);
#ifdef OMNI_MEM_DEBUG
            g_lsnDetachedDeferCount.fetch_add(1, std::memory_order_relaxed);
            if (size > 0) {
                g_lsnDetachedDeferReserveBytes.fetch_add(size, std::memory_order_relaxed);
            } else {
                g_lsnDetachedDeferUnreserveBytes.fetch_add(-size, std::memory_order_relaxed);
            }
#endif
        }
        return;
    }

    // usedBytes_/peakBytes_ mirror what Spark actually GRANTED (not what we asked — the
    // reserve/unreserve(J)J upcalls return the bytes actually granted/released, which can be
    // a partial grant). Bump usedBytes_ by `granted` and push the high-watermark with a CAS
    // loop that snapshots the post-increment value (loading usedBytes_ twice could record a
    // peak that was never an actual high-watermark). Shared by both deliveries below so they
    // stay byte-for-byte identical.
    auto applyGranted = [this](int64_t granted) {
        int64_t newUsed = (usedBytes_ += granted);
        int64_t prevPeak = peakBytes_.load();
        while (newUsed > prevPeak && !peakBytes_.compare_exchange_weak(prevPeak, newUsed)) {
            // compare_exchange_weak refreshes prevPeak on failure; loop until we win the CAS
            // or another thread has already pushed peak past newUsed.
        }
    };

    // ── Replay path: flush deltas parked by earlier detached calls ───────────────────
    // We are on an attached thread, so atomically take everything parked so far (exchange
    // resets it to 0) and deliver it to Spark as one net reserve/unreserve. This is
    // best-effort catch-up for memory the pool already holds. If Spark DENIES it (the task is
    // genuinely over its cap because of the detached allocations), we must NOT fail the
    // current, unrelated allocationChanged: we clear the exception, re-park the delta, and
    // let a future attached call retry. Re-parking with fetch_add (not store) is deliberate —
    // a detached thread may have added more between our exchange and here, and fetch_add
    // preserves that. A partial grant on replay is left as-is (the same silent partial-grant
    // divergence the normal path has below — a separate, pre-existing issue, not this fix's).
    int64_t deferred = pendingDelta_.exchange(0, std::memory_order_acq_rel);
    if (deferred != 0) {
        try {
            const jlong replayGranted = invokeReservation(env, deferred);
#ifdef OMNI_MEM_DEBUG
            g_lsnDeferReplayCount.fetch_add(1, std::memory_order_relaxed);
            g_lsnDeferReplayBytes.fetch_add(replayGranted, std::memory_order_relaxed);
#endif
            applyGranted(replayGranted);
        } catch (const omniruntime::mem::ReservationDenied&) {
            pendingDelta_.fetch_add(deferred, std::memory_order_acq_rel);
#ifdef OMNI_MEM_DEBUG
            g_lsnDeferReplayRejectCount.fetch_add(1, std::memory_order_relaxed);
#endif
        } catch (...) {
            pendingDelta_.fetch_add(deferred, std::memory_order_acq_rel);
            throw;
        }
    }

    // Deliver this call's own delta. Fatal Java failures retain their cause;
    // only ThrowOnOomMemoryTarget.OutOfMemoryException is an advisory denial.
    if (size != 0) {
        const jlong granted = invokeReservation(env, size);
#ifdef OMNI_MEM_DEBUG
        // Reserve/unreserve delivery accounting (no exception => granted is final).
        if (size > 0) {
            g_lsnReserveCalls.fetch_add(1, std::memory_order_relaxed);
            g_lsnReserveReq.fetch_add(size, std::memory_order_relaxed);
            g_lsnReserveGranted.fetch_add(granted, std::memory_order_relaxed);
            if (granted < size) {   // partial grant: silent native/Spark divergence
                g_lsnPartialCount.fetch_add(1, std::memory_order_relaxed);
                g_lsnPartialGap.fetch_add(size - granted, std::memory_order_relaxed);
            }
        } else {
            g_lsnUnreserveCalls.fetch_add(1, std::memory_order_relaxed);
            g_lsnUnreserveReq.fetch_add(-size, std::memory_order_relaxed);
            g_lsnUnreserveDone.fetch_add(-granted, std::memory_order_relaxed);   // granted<0 for unreserve
        }
#endif
        applyGranted(granted);
    }
}

} // namespace gluten
