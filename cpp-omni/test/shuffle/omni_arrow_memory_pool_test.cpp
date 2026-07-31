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

// Task 2 (TC-MP-01/02): Verify OmniMemoryPoolAdapter routes Arrow allocations
// through the omni Allocator so that GetGlobalAccountedMemory() reflects every
// byte. This is the spill-accounting gate for all subsequent Arrow shuffle work.

#include "gtest/gtest.h"
#include "shuffle/omni_arrow_memory_pool.h"

#include <arrow/buffer.h>
#include <arrow/result.h>

#include <vector/vector_common.h>

using omniruntime::mem::Allocator;
using omniruntime::mem::MemoryManager;
using omniruntime::mem::ThreadMemoryManager;

// TC-MP-01 — AdapterCounterTracksAllocations
// Verify the adapter's own bytes_allocated / max_memory counters correctly
// track Arrow allocations routed through the omni Allocator.  This is a unit
// test of the adapter's bookkeeping and does not depend on the 1 MB global
// flush threshold of ThreadMemoryManager.
TEST(OmniMemoryPoolAdapter, AdapterCounterTracksAllocations)
{
    auto *omniAlloc = Allocator::GetAllocator();
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
}

// TC-MP-02 — LargeAllocationFlushesToGlobalLedger
// Verify that Arrow allocations through the adapter eventually appear in the
// global memory ledger (GetGlobalAccountedMemory), which is what the shuffle
// spill threshold check reads.  The omni ThreadMemoryManager batches per-thread
// accounting with a 1 MB flush threshold, so we allocate >1 MB to force a flush
// and use arrow::AllocateResizableBuffer to exercise the high-level Arrow API
// path that production shuffle code will use.
TEST(OmniMemoryPoolAdapter, LargeAllocationFlushesToGlobalLedger)
{
    // Reset thread-local and global memory state for a deterministic baseline.
    ThreadMemoryManager::GetThreadMemoryManager()->Clear();

    auto *omniAlloc = Allocator::GetAllocator();
    OmniMemoryPoolAdapter pool(omniAlloc);

    int64_t before = MemoryManager::GetGlobalAccountedMemory();

    // Allocate 2 MB — exceeds the 1 MB untrackedMemoryThreshold so the batched
    // AddMemory propagates to the global MemoryManager.
    constexpr int64_t kSize = 2 * 1024 * 1024; // 2 MB
    auto bufResult = arrow::AllocateResizableBuffer(kSize, &pool);
    ASSERT_TRUE(bufResult.ok()) << bufResult.status().ToString();
    auto buf = std::move(*bufResult);
    ASSERT_NE(buf->data(), nullptr);

    // Verify the allocated memory is usable
    buf->mutable_data()[0] = 0xAB;
    EXPECT_EQ(buf->data()[0], 0xAB);

    int64_t after = MemoryManager::GetGlobalAccountedMemory();
    EXPECT_GE(after, before + kSize) << "Arrow alloc must be accounted in global memory";

    // Release the buffer — destructor calls pool->Free which routes through
    // Allocator::Free → ThreadMemoryManager::ReclaimMemory → global ledger.
    buf.reset();
    int64_t freed = MemoryManager::GetGlobalAccountedMemory();
    EXPECT_LE(freed, after);
}
