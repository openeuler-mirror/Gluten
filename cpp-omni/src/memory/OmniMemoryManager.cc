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

#include "memory/OmniPoolAllocationListener.h"
#include "OmniMemoryManager.h"
#include "RuntimeMemoryHelper.h"
#include "jni/OmniBlockAllocationListener.h"
#include "jni/OmniSparkAllocationListener.h"
#include "memory/omni_task_telemetry.h"
#include "operator/reclaim_context.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstdio>
#include <exception>
#include <limits>
#include <stdexcept>
#include <utility>

#include "arrow/memory_pool.h"   // arrow::default_memory_pool()

// Implementation of the free-function downcast bridge
// declared in RuntimeMemoryHelper.h. Lives in this TU because OmniMemoryManager.cc
// already has the cpp/core per-file include path set, and crucially does NOT
// transitively pull Omni's compute/* headers (which would re-trigger the
// compute/ResultIterator.h shadow trap; see RuntimeMemoryHelper.h comment).
namespace omniruntime {
namespace {
// Do not use dynamic_cast<OmniMemoryManager*> here. On aarch64 that path
// enters libstdc++ __dynamic_cast and SIGSEGVs when the MemoryManager
// typeinfo seen by this TU does not match the object's vtable (gluten-core
// MemoryManager.cc is compiled into the plugin; OmniMemoryManager is a
// separate TU). A virtual kind() call only needs a valid vtable.
bool IsOmniMemoryManager(gluten::MemoryManager *mm)
{
    return mm != nullptr && mm->kind() == "omni";
}
} // namespace

mem::OmniTaskMemoryPool *ToOmniTaskPoolOrNull(gluten::MemoryManager *mm)
{
    if (!IsOmniMemoryManager(mm)) {
        return nullptr;
    }
    return static_cast<gluten::OmniMemoryManager *>(mm)->getTaskMemoryPool();
}

void BindOmniSparkListener(gluten::MemoryManager* mm, gluten::OmniSparkAllocationListener* listener)
{
    if (!IsOmniMemoryManager(mm)) {
        throw std::invalid_argument("Memory manager is not an OmniMemoryManager");
    }
    static_cast<gluten::OmniMemoryManager*>(mm)->setSparkListener(listener);
}

void SetOmniReservationBlockSize(gluten::MemoryManager* mm, int64_t blockSize)
{
    if (!IsOmniMemoryManager(mm)) {
        throw std::invalid_argument("Memory manager is not an OmniMemoryManager");
    }
    static_cast<gluten::OmniMemoryManager*>(mm)->setReservationBlockSize(blockSize);
}

void ReconcileExternalRepay(
    gluten::MemoryManager *mm, int64_t nativeFreed, int64_t repaid)
{
    if (!IsOmniMemoryManager(mm)) {
        throw std::invalid_argument("Memory manager is not an OmniMemoryManager");
    }
    static_cast<gluten::OmniMemoryManager *>(mm)->reconcileExternalRepay(nativeFreed, repaid);
}

int64_t SpillOmniMemory(gluten::MemoryManager *mm, int64_t size)
{
    if (!IsOmniMemoryManager(mm)) {
        throw std::invalid_argument("Memory manager is not an OmniMemoryManager");
    }
    return static_cast<gluten::OmniMemoryManager *>(mm)->spill(size);
}

void SetOmniTaskTelemetryContext(
    gluten::MemoryManager *mm,
    const std::string &runId,
    const std::string &queryId,
    const std::string &round,
    int64_t taskAttemptId)
{
    auto *pool = ToOmniTaskPoolOrNull(mm);
    if (pool != nullptr) {
        mem::SetOmniTaskPoolTelemetry(pool, runId, queryId, round, taskAttemptId);
    }
}
} // namespace omniruntime

namespace gluten {

OmniMemoryManager::OmniMemoryManager(const std::string& kind, std::unique_ptr<AllocationListener> listener)
    : MemoryManager(kind), listener_(std::move(listener))
{
    // Move reservation to and from Spark in whole blocks (8 MiB unless
    // SetOmniReservationBlockSize applies spark.gluten.memory.reservationBlockSize).
    blockWrap_ = std::make_unique<OmniBlockAllocationListener>(listener_.get());

    // Bridge the two independently owned interfaces without relying on vtable
    // layout. Member order keeps the adapter and block wrapper alive for teardown.
    poolListener_ = std::make_unique<OmniPoolAllocationListener>(*blockWrap_);
    pool_ = std::make_unique<omniruntime::mem::OmniTaskMemoryPool>(poolListener_.get());

    // arrowPool_: arrow::default_memory_pool() returns an Arrow-owned singleton;
    // we hold a non-owning raw ptr. This pool is not listened (no Spark visibility
    // into allocations through it) until a listenable ArrowMemoryPool can be used
    // (see the OmniMemoryManager.h header comment on the Arrow API mismatch).
    arrowPool_ = arrow::default_memory_pool();
}

OmniMemoryManager::~OmniMemoryManager()
{
#ifdef OMNI_MEM_DEBUG
    // Reconciliation triangle (per task, at teardown): the three byte counters in the
    // listener chain that should agree but structurally cannot —
    //   pool_peak     : requested bytes the per-task pool charged (pre-coalesce)
    //   block_peak    : tier-coalesced reservation the block layer held
    //   spark_granted : bytes Spark actually granted (post partial-grant)
    // block_peak - spark_granted = quantization tax; pool_peak vs spark = partial-grant/drift.
    // Plus the Arrow default pool: allocations Gluten routes through it are NOT listened
    // (no Spark visibility) — an unaccounted-by-design bucket, made explicit here.
    int64_t poolPeak  = (pool_ != nullptr)      ? pool_->peakBytes()           : 0;
    int64_t blockPeak = (blockWrap_ != nullptr) ? blockWrap_->peakBytes()      : 0;
    int64_t sparkPeak = (listener_ != nullptr)  ? listener_->peakBytes()       : 0;
    int64_t arrowCur  = (arrowPool_ != nullptr) ? arrowPool_->bytes_allocated(): 0;
    int64_t arrowMax  = (arrowPool_ != nullptr) ? arrowPool_->max_memory()     : 0;
    fprintf(stderr, "[MEMPROBE-TRIANGLE] pool_peak=%lld block_peak=%lld spark_granted_peak=%lld\n",
            (long long)poolPeak, (long long)blockPeak, (long long)sparkPeak);
    fprintf(stderr, "[MEMPROBE-ARROW] arrow_current=%lld arrow_max=%lld\n",
            (long long)arrowCur, (long long)arrowMax);
    fflush(stderr);
#endif
}

void OmniMemoryManager::setReservationBlockSize(int64_t blockSize)
{
    blockWrap_->setBlockSize(blockSize);
}

void OmniMemoryManager::setSparkListener(OmniSparkAllocationListener* listener)
{
    if (listener != listener_.get()) {
        throw std::invalid_argument("Spark listener must be owned by this memory manager");
    }
    sparkListener_ = listener;
}

void OmniMemoryManager::reconcileExternalRepay(int64_t nativeFreed, int64_t repaid)
{
    if (nativeFreed < 0 || repaid < 0 || repaid > nativeFreed) {
        throw std::invalid_argument("Invalid native/Spark repayment amounts");
    }
    // Lock order matches allocation: pool -> block. No JNI/Spark callback is
    // made here. All validations precede the Spark-ledger CAS; once it succeeds,
    // the block and pool commits are non-throwing with their locks still held.
    pool_->restoreUnrepaidReservation(nativeFreed - repaid, [&] {
        if (repaid > 0) {
            blockWrap_->reconcileExternalRepay(repaid, [&] {
                if (sparkListener_ != nullptr) {
                    sparkListener_->reconcileExternalRepay(repaid);
                }
            });
        }
    });
}

const MemoryUsageStats OmniMemoryManager::collectMemoryUsageStats() const
{
    MemoryUsageStats stats;
    // Flat per-task pool view. set_current/set_peak are the protobuf-generated
    // setters on the `current` (field 2) and `peak` (field 1) int64 fields of
    // gluten-core/memory.proto::MemoryUsageStats.
    stats.set_current(pool_->currentBytes());
    stats.set_peak(pool_->peakBytes());
    // Flat pool: no per-operator children are reported.
    return stats;
}

namespace {

// Process-wide monotonic counters make individual shrink records useful even
// when several task managers write to the same executor log.
std::atomic<uint64_t> shrinkRequests{0};
std::atomic<uint64_t> shrinkSuccesses{0};
std::atomic<uint64_t> shrinkZeroResults{0};
std::atomic<uint64_t> shrinkExceptions{0};
std::atomic<uint64_t> shrinkRequestedBytes{0};
std::atomic<uint64_t> shrinkSentinelRequests{0};
std::atomic<uint64_t> shrinkFreedBytes{0};

bool IsEnvironmentSwitchEnabled(const char *name)
{
    const char *value = std::getenv(name);
    return value != nullptr && value[0] != '\0' && !(value[0] == '0' && value[1] == '\0');
}

} // namespace

const int64_t OmniMemoryManager::shrink(int64_t)
{
    return 0;
}

const int64_t OmniMemoryManager::spill(int64_t size)
{
    // Keep the historical OMNI-SHRINK telemetry tag and field schema while
    // central benchmark parsers migrate to the explicit SPILL API.
    const bool verboseTelemetry = IsEnvironmentSwitchEnabled("OMNI_RECLAIM_VERBOSE");
    const bool successTelemetry = verboseTelemetry ||
        IsEnvironmentSwitchEnabled("OMNI_RECLAIM_VERBOSE_SUCCESS");
    const auto started = std::chrono::steady_clock::now();
    const uint64_t requestId = shrinkRequests.fetch_add(1, std::memory_order_relaxed) + 1;
    struct ReclaimTraceRestore {
        omniruntime::op::ReclaimTraceSummary saved;
        ~ReclaimTraceRestore() { omniruntime::op::reclaimTraceSummary = saved; }
    } traceRestore{omniruntime::op::CurrentReclaimTraceSummary()};
    omniruntime::op::reclaimTraceSummary = {};
    const bool validRequest = size > 0;
    // Spark uses Long.MAX_VALUE as "reclaim as much as possible". It is a
    // control sentinel, not a meaningful byte demand; repeatedly adding it to
    // the uint64 aggregate wraps requested_total and corrupts telemetry.
    const bool sentinelRequest = size == std::numeric_limits<int64_t>::max();
    if (sentinelRequest) {
        shrinkSentinelRequests.fetch_add(1, std::memory_order_relaxed);
    } else if (validRequest) {
        shrinkRequestedBytes.fetch_add(static_cast<uint64_t>(size), std::memory_order_relaxed);
    }

    try {
        const int64_t freed = validRequest && pool_ != nullptr ? pool_->reclaim(size) : 0;
        if (freed > 0) {
            shrinkSuccesses.fetch_add(1, std::memory_order_relaxed);
            shrinkFreedBytes.fetch_add(static_cast<uint64_t>(freed), std::memory_order_relaxed);
        } else {
            shrinkZeroResults.fetch_add(1, std::memory_order_relaxed);
        }
        const bool logCompletion = verboseTelemetry || (successTelemetry && freed > 0);
        if (logCompletion) {
            const auto reclaimTrace = omniruntime::op::CurrentReclaimTraceSummary();
            const int64_t elapsedUs = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - started).count();
            const int64_t ratioPpm = validRequest && freed > 0
                ? static_cast<int64_t>((static_cast<long double>(freed) * 1000000.0L) / size)
                : 0;
            fprintf(stderr,
            "[OMNI-SHRINK] event=complete id=%llu pool=%p class=%s outcome=%s requested=%lld freed=%lld "
            "ratio_ppm=%lld duration_us=%lld memory_request_id=%llu requestor_type=%s requestor=%p site=%s "
            "primary_donor_type=%s primary_donor=%p donor_estimated=%llu donor_pool_freed=%lld donors=%lld "
            "requests=%llu successes=%llu zeros=%llu exceptions=%llu "
            "requested_total=%llu sentinel_requests=%llu freed_total=%llu\n",
            (unsigned long long)requestId, static_cast<void *>(pool_.get()), validRequest ? "positive" : "non_positive",
            freed > 0 ? "success" : "zero", (long long)size, (long long)freed,
            (long long)ratioPpm, (long long)elapsedUs,
            (unsigned long long)reclaimTrace.requestId,
            reclaimTrace.requestorType == nullptr ? "unknown" : reclaimTrace.requestorType,
            reclaimTrace.requestor,
            reclaimTrace.site == nullptr ? "unknown" : reclaimTrace.site,
            reclaimTrace.primaryDonorType == nullptr ? "none" : reclaimTrace.primaryDonorType,
            reclaimTrace.primaryDonor,
            (unsigned long long)reclaimTrace.primaryDonorEstimated,
            (long long)reclaimTrace.primaryDonorPoolFreed,
            (long long)reclaimTrace.donors,
            (unsigned long long)shrinkRequests.load(std::memory_order_relaxed),
            (unsigned long long)shrinkSuccesses.load(std::memory_order_relaxed),
            (unsigned long long)shrinkZeroResults.load(std::memory_order_relaxed),
            (unsigned long long)shrinkExceptions.load(std::memory_order_relaxed),
            (unsigned long long)shrinkRequestedBytes.load(std::memory_order_relaxed),
            (unsigned long long)shrinkSentinelRequests.load(std::memory_order_relaxed),
            (unsigned long long)shrinkFreedBytes.load(std::memory_order_relaxed));
            fflush(stderr);
        }
        return freed;
    } catch (const std::exception& e) {
        const uint64_t exceptions = shrinkExceptions.fetch_add(1, std::memory_order_relaxed) + 1;
        const int64_t elapsedUs = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - started).count();
        fprintf(stderr,
            "[OMNI-SHRINK] event=exception id=%llu class=%s requested=%lld duration_us=%lld "
            "exceptions=%llu error=%s\n",
            (unsigned long long)requestId, validRequest ? "positive" : "non_positive",
            (long long)size, (long long)elapsedUs, (unsigned long long)exceptions, e.what());
        fflush(stderr);
        throw;
    } catch (...) {
        const uint64_t exceptions = shrinkExceptions.fetch_add(1, std::memory_order_relaxed) + 1;
        const int64_t elapsedUs = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - started).count();
        fprintf(stderr,
            "[OMNI-SHRINK] event=exception id=%llu class=%s requested=%lld duration_us=%lld "
            "exceptions=%llu error=unknown\n",
            (unsigned long long)requestId, validRequest ? "positive" : "non_positive",
            (long long)size, (long long)elapsedUs, (unsigned long long)exceptions);
        fflush(stderr);
        throw;
    }
}

namespace {

// Factory registration at .so load time. Matches gluten/cpp/core/memory/MemoryManager.cc:33
// signature exactly: registerFactory(kind, Factory, Releaser). Factory returns
// MemoryManager* (raw — the create() caller takes ownership). Releaser is the
// matching delete operation invoked by release().
struct OmniMemoryManagerRegistrar {
    OmniMemoryManagerRegistrar() {
        MemoryManager::registerFactory(
            /*kind=*/"omni",
            /*factory=*/[](const std::string& kind, std::unique_ptr<AllocationListener> listener) -> MemoryManager* {
                return new OmniMemoryManager(kind, std::move(listener));
            },
            /*releaser=*/[](MemoryManager* mm) {
                delete mm;
            });
    }
};

// Static initializer — runs once when libspark_columnar_plugin.so is loaded.
// The Java side calls MemoryManager::create("omni", listener) via the JNI
// wrapper, which dispatches through the factory map registered here.
static OmniMemoryManagerRegistrar registrar_;

} // namespace

} // namespace gluten
