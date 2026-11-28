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
 */
#include <functional>
#include <type_traits>
#include "gtest/gtest.h"
#include "jni/OmniBlockAllocationListener.h"
#include "memory/omni_task_memory_pool.h"
#include "memory/OmniPoolAllocationListener.h"
#include "operator/spillable_operator.h"

namespace {
static_assert(!std::is_convertible_v<gluten::AllocationListener*, omniruntime::mem::PoolListener*>,
    "Gluten listener must cross the explicit Omni adapter");
static_assert(std::is_convertible_v<gluten::OmniPoolAllocationListener*, omniruntime::mem::PoolListener*>);

constexpr int64_t kMiB = int64_t{1} << 20;
class SparkLedger final : public gluten::AllocationListener {
public:
    int64_t billed = 0;
    std::function<void()> onBorrow;
    void allocationChanged(int64_t diff) override {
        if (diff > 0 && onBorrow) {
            auto callback = std::move(onBorrow);
            onBorrow = nullptr;
            callback();
        }
        if (billed + diff < 0) throw std::underflow_error("over-repayment");
        billed += diff;
    }
};
class PoolDonor final : public omniruntime::op::SpillableOperator {
public:
    explicit PoolDonor(omniruntime::mem::OmniTaskMemoryPool& pool) : pool_(pool) {}
    uint64_t reclaimableBytes() const override { return 4 * kMiB; }
    int64_t reclaim(int64_t) override { pool_.SubMemory(4 * kMiB); return 4 * kMiB; }
    bool isInNonReclaimableSection() const noexcept override { return false; }
    const char* reclaimableName() const noexcept override { return "test-donor"; }
    omniruntime::op::Operator* operatorBase() noexcept override { return nullptr; }
    omniruntime::mem::OmniTaskMemoryPool& pool_;
};
}

TEST(OmniPoolRepayment, ReclaimThenDenyReconcilesPoolBlockAndSpark) {
    for (int64_t repaid : {4 * kMiB, 2 * kMiB, kMiB / 2}) {
        SparkLedger spark;
        gluten::OmniBlockAllocationListener block(&spark);
        gluten::OmniPoolAllocationListener poolListenerBridge(block);
        omniruntime::mem::OmniTaskMemoryPool pool(&poolListenerBridge);
        pool.AddMemory(8 * kMiB);
        PoolDonor donor(pool);
        pool.registerSpillable(&donor);
        spark.onBorrow = [&] {
            const int64_t freed = pool.reclaim(4 * kMiB);
            EXPECT_EQ(freed, 4 * kMiB);
            spark.billed -= repaid;
            block.reconcileExternalRepay(repaid);
            pool.restoreUnrepaidReservation(freed - repaid);
            throw omniruntime::mem::ReservationDenied("Spark denied after reclaim");
        };
        EXPECT_THROW(pool.AddMemory(8 * kMiB), omniruntime::mem::ReservationDenied);
        EXPECT_EQ(pool.currentBytes(), 4 * kMiB);
        EXPECT_EQ(pool.reservedBytes(), 8 * kMiB - repaid);
        EXPECT_EQ(block.currentBytes(), spark.billed);
        EXPECT_EQ(spark.billed, 8 * kMiB - repaid);
        pool.SubMemory(4 * kMiB);
        pool.releaseUnusedReservation();
        EXPECT_EQ(pool.reservedBytes(), 0);
        EXPECT_EQ(block.currentBytes(), 0);
        EXPECT_EQ(spark.billed, 0);
        pool.unregisterSpillable(&donor);
    }
}
