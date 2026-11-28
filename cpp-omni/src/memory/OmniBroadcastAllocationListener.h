/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Description: executor-wide broadcast storage reservation coalescing.
 */
#pragma once

#include <algorithm>
#include <limits>
#include <mutex>
#include <stdexcept>
#include "memory/AllocationListener.h"
#include "memory/reservation_denied.h"

namespace gluten {

// Shared by all entries in one broadcast memory domain. The delegate outlives
// this wrapper. Keep accounting serialized; this class reduces JNI frequency,
// but does not claim to remove the remaining global callback serialization.
class OmniBroadcastAllocationListener final : public AllocationListener {
public:
    explicit OmniBroadcastAllocationListener(AllocationListener& delegate) : delegate_(delegate) {}

    void allocationChanged(int64_t delta) override {
        if (delta == 0) return;
        std::lock_guard<std::mutex> guard(mutex_);
        if (delta > 0 && used_ > std::numeric_limits<int64_t>::max() - delta) {
            throw std::overflow_error("Broadcast allocation accounting overflow");
        }
        if (delta < 0 && (delta == std::numeric_limits<int64_t>::min() || used_ < -delta)) {
            throw std::underflow_error("Broadcast allocation accounting underflow");
        }
        const int64_t nextUsed = used_ + delta;
        int64_t target = reserved_;
        if (nextUsed > reserved_) {
            target = roundUp(nextUsed);
            try {
                delegate_.allocationChanged(target - reserved_);
            } catch (const omniruntime::mem::ReservationDenied&) {
                // The speculative quantum must not reject an allocation that
                // fits the remaining cache/storage capacity exactly.
                if (target == nextUsed) throw;
                target = nextUsed;
                delegate_.allocationChanged(target - reserved_);
            }
        } else if (delta < 0) {
            target = std::min(reserved_, roundUp(nextUsed));
            if (target < reserved_) delegate_.allocationChanged(target - reserved_);
        }
        // Delegate exceptions leave both ledgers unchanged.
        used_ = nextUsed;
        reserved_ = target;
        peak_ = std::max(peak_, reserved_);
    }

    int64_t currentBytes() override {
        std::lock_guard<std::mutex> guard(mutex_);
        return reserved_;
    }
    int64_t peakBytes() override {
        std::lock_guard<std::mutex> guard(mutex_);
        return peak_;
    }

private:
    static int64_t roundUp(int64_t bytes) {
        constexpr int64_t quantum = int64_t{1} << 20;
        constexpr int64_t max = std::numeric_limits<int64_t>::max();
        return bytes > max - (quantum - 1) ? max : ((bytes + quantum - 1) / quantum) * quantum;
    }
    AllocationListener& delegate_;
    std::mutex mutex_;
    int64_t used_{0};
    int64_t reserved_{0};
    int64_t peak_{0};
};

} // namespace gluten
