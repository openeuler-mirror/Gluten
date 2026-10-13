/*
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

// Verify OmniMemoryPoolAdapter routes Arrow allocations through the per-task
// Omni allocator. This is the spill-accounting gate for Arrow shuffle work.

#include "gtest/gtest.h"
#include "memory/AllocationListener.h"
#include "memory/omni_task_memory_pool.h"
#include "memory/OmniPoolAllocationListener.h"
#include "shuffle/omni_arrow_memory_pool.h"

#include <arrow/buffer.h>
#include <arrow/result.h>

#include <algorithm>
#include <vector/vector_common.h>

namespace {
class TrackingAllocationListener final : public gluten::AllocationListener {
public:
    void allocationChanged(int64_t diff) override
    {
        current_ += diff;
        peak_ = std::max(peak_, current_);
    }

    int64_t currentBytes() override { return current_; }
    int64_t peakBytes() override { return peak_; }

private:
    int64_t current_{0};
    int64_t peak_{0};
};
} // namespace

// TC-MP-01 — AdapterCounterTracksAllocations
// Verify the adapter's own bytes_allocated / max_memory counters correctly
// track Arrow allocations routed through the task-bound Omni allocator.
TEST(OmniMemoryPoolAdapter, AdapterCounterTracksAllocations)
{
    TrackingAllocationListener listener;
    gluten::OmniPoolAllocationListener taskPoolListenerBridge(listener);
    omniruntime::mem::OmniTaskMemoryPool taskPool(&taskPoolListenerBridge);
    auto *omniAlloc = taskPool.getBoundAllocator();
    OmniMemoryPoolAdapter pool(omniAlloc);

    constexpr int64_t kSize = 4096;
    constexpr int64_t kAlignment = 64; // Arrow default buffer alignment

    EXPECT_EQ(pool.bytes_allocated(), 0);

    uint8_t *p = nullptr;
    ASSERT_TRUE(pool.Allocate(kSize, kAlignment, &p).ok());
    ASSERT_NE(p, nullptr);

    // Adapter counter must reflect the allocation
    EXPECT_GE(pool.bytes_allocated(), kSize);
    // Peak must be at least kSize
    EXPECT_GE(pool.max_memory(), kSize);

    pool.Free(p, kSize, kAlignment);
    // After free, counter must drop back
    EXPECT_LE(pool.bytes_allocated(), 0);
    // Peak must remain at least kSize (peak is not reduced on free)
    EXPECT_GE(pool.max_memory(), kSize);
    EXPECT_EQ(taskPool.currentBytes(), 0);
    EXPECT_EQ(listener.currentBytes(), 0);
}

// TC-MP-02 — LargeAllocationTracksTaskPoolLedger
// Exercise the high-level Arrow API and verify that both the task pool and its
// listener see the allocation immediately and reconcile it on release.
TEST(OmniMemoryPoolAdapter, LargeAllocationTracksTaskPoolLedger)
{
    TrackingAllocationListener listener;
    gluten::OmniPoolAllocationListener taskPoolListenerBridge(listener);
    omniruntime::mem::OmniTaskMemoryPool taskPool(&taskPoolListenerBridge);
    auto *omniAlloc = taskPool.getBoundAllocator();
    OmniMemoryPoolAdapter pool(omniAlloc);

    const int64_t before = taskPool.currentBytes();

    constexpr int64_t kSize = 2 * 1024 * 1024; // 2 MB
    auto bufResult = arrow::AllocateResizableBuffer(kSize, &pool);
    ASSERT_TRUE(bufResult.ok()) << bufResult.status().ToString();
    auto buf = std::move(*bufResult);
    ASSERT_NE(buf->data(), nullptr);

    // Verify the allocated memory is usable
    buf->mutable_data()[0] = 0xAB;
    EXPECT_EQ(buf->data()[0], 0xAB);

    const int64_t after = taskPool.currentBytes();
    EXPECT_GE(after, before + kSize) << "Arrow alloc must be accounted in the task pool";
    EXPECT_GE(listener.currentBytes(), kSize);

    // Release routes through the same bound allocator and reconciles both
    // the pool-local and Spark-facing listener ledgers.
    buf.reset();
    EXPECT_EQ(taskPool.currentBytes(), before);
    EXPECT_EQ(listener.currentBytes(), before);
}
