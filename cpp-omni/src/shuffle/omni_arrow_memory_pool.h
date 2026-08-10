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

#ifndef CPP_OMNI_ARROW_MEMORY_POOL_H
#define CPP_OMNI_ARROW_MEMORY_POOL_H

#include <arrow/memory_pool.h>

#include <atomic>
#include <string>

#include <vector/vector_common.h>

// OmniMemoryPoolAdapter bridges arrow::MemoryPool to the omniruntime Allocator.
//
// Every Arrow allocation (e.g. arrow::AllocateResizableBuffer, BufferBuilder)
// that is constructed with this pool is routed through
// omniruntime::mem::Allocator::Alloc/Free, which in turn reports to
// MemoryManager::GetGlobalAccountedMemory().  This guarantees the shuffle spill
// threshold check — which reads GetGlobalAccountedMemory() — does not miss any
// Arrow buffer and therefore avoids silent OOM.
//
// Design reference: 260629-Shuffle-arrow-design-detail.md §13.1.
class OmniMemoryPoolAdapter : public arrow::MemoryPool {
public:
    // Construct an adapter that forwards to the given omni Allocator.
    // The caller retains ownership of \p alloc (typically
    // Allocator::GetAllocator() or SplitOptions::allocator).
    explicit OmniMemoryPoolAdapter(omniruntime::mem::Allocator *alloc) : alloc_(alloc) {}

    // --- arrow::MemoryPool overrides (Arrow 11 three-parameter signatures) ---
    arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t **out) override;
    arrow::Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment, uint8_t **ptr) override;
    void Free(uint8_t *buffer, int64_t size, int64_t alignment) override;

    int64_t bytes_allocated() const override;
    int64_t max_memory() const override;
    std::string backend_name() const override;

private:
    omniruntime::mem::Allocator *alloc_;
    std::atomic<int64_t> bytesAllocated_{0};
    std::atomic<int64_t> peak_{0};
};

#endif // CPP_OMNI_ARROW_MEMORY_POOL_H
