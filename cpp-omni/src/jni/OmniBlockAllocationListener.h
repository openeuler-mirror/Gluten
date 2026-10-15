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
 * OmniBlockAllocationListener — Velox-style coalescing wrap around AllocationListener.
 *
 * Unlike gluten::BlockAllocationListener's fixed block-count representation,
 * the reservation ledger may
 * become non-block-aligned after an exact JVM-side repayment.
 * Reservation propagation mirrors Velox MemoryPool::quantizedSize(): 1 MiB
 * below 16 MiB, 4 MiB below 64 MiB, then 8 MiB. This keeps small pools cheap
 * while retaining coarse-grained JNI traffic for large pools.
 *
 * Fixes the upstream gluten::BlockAllocationListener bug at AllocationListener.h:74-88:
 * reservationBytes_ (line 95) is declared and read from currentBytes() (line 65) but NEVER
 * written from reserve(). Result: currentBytes() silently returns 0 forever. This class
 * adds the missing reservationBytes_ update after the
 * ceiling math. Additionally tightens the upstream class's unlocked currentBytes()/peakBytes()
 * reads by reading under the same mutex as the writer.
 *
 * This class lives in cpp-omni. The upstream BlockAllocationListener bug in
 * gluten/cpp/core/memory/AllocationListener.h remains; a future apache/gluten PR
 * may upstream the fix.
 */
#pragma once

#include <algorithm>
#include <functional>
#include <mutex>
#include <stdexcept>
#include "memory/AllocationListener.h"

namespace gluten {

class OmniBlockAllocationListener final : public AllocationListener {
 public:
    explicit OmniBlockAllocationListener(AllocationListener* delegated)
        : delegated_(delegated) {
        if (delegated_ == nullptr) {
            throw std::invalid_argument("OmniBlockAllocationListener: invalid delegate");
        }
    }

    OmniBlockAllocationListener(const OmniBlockAllocationListener&) = delete;
    OmniBlockAllocationListener& operator=(const OmniBlockAllocationListener&) = delete;
    OmniBlockAllocationListener(OmniBlockAllocationListener&&) = delete;
    OmniBlockAllocationListener& operator=(OmniBlockAllocationListener&&) = delete;
    ~OmniBlockAllocationListener() override = default;

    void allocationChanged(int64_t diff) override {
        if (diff == 0) return;
        const ReservationUpdate update = updateReservation(diff);
        if (update.granted == 0) return;
        try {
            delegated_->allocationChanged(update.granted);
        } catch (...) {
            finishReservation(update, false);
            throw;
        }
        finishReservation(update, true);
    }

    int64_t currentBytes() override {
        std::lock_guard<std::mutex> lock(mutex_);   // tightens upstream (unlocked read)
        return reservationBytes_;
    }

    int64_t peakBytes() override {
        std::lock_guard<std::mutex> lock(mutex_);   // tightens upstream (unlocked read)
        return peakBytes_;
    }

    // Spark-facing reservation granularity, Gluten's spark.gluten.memory.
    // reservationBlockSize. Must be set before the first reservation.
    void setBlockSize(int64_t blockSize);

    // Reconcile bytes already repaid directly by the JVM MemoryTarget. This is
    // deliberately ledger-only: calling the delegate here would credit Spark a
    // second time. Exact repayments may leave a non-block-aligned reservation.
    void reconcileExternalRepay(int64_t bytes);
    // Validate under the block lock, reconcile the downstream ledger, then
    // commit. Callback must be ledger-only and must not reenter this wrapper.
    void reconcileExternalRepay(int64_t bytes, const std::function<void()>& reconcile);

 private:
    struct ReservationUpdate {
        int64_t usedDelta;
        int64_t granted;
        int64_t peakCandidate;
    };

    ReservationUpdate updateReservation(int64_t diff);
    void finishReservation(const ReservationUpdate& update, bool succeeded);

    // Raw usage and the amount still reserved from Spark are distinct ledgers.
    AllocationListener* const delegated_;
    int64_t usedBytes_{0};
    int64_t peakBytes_{0};
    // Reservation moves to and from Spark in whole blocks, like Gluten's
    // ListenableArbitrator for Velox (memoryPoolTransferCapacity =
    // reservationBlockSize, 8 MiB default). One unused block is retained on
    // frees before repaying; free-to-zero repays everything.
    int64_t blockSize_{int64_t{8} << 20};
    int64_t reservationBytes_{0};    // actually written by reserve() here; upstream omits the write
    // In-flight delegate calls are provisional. Nested calls coalesce against
    // committed usage/reservation, so a free cannot repay an ungranted borrow.
    int64_t pendingUsedBytes_{0};
    int64_t pendingReservationBytes_{0};
    mutable std::mutex mutex_;
};

} // namespace gluten
