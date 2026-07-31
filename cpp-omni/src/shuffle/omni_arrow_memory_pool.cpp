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

#include "shuffle/omni_arrow_memory_pool.h"

#include <algorithm>
#include <cstring>

#include "common/debug.h"

// OmniMemoryPoolAdapter implementation.
//
// Each Arrow Allocate/Reallocate/Free is forwarded to the omniruntime Allocator
// which transparently reports to the global memory ledger
// (MemoryManager::GetGlobalAccountedMemory).  The adapter additionally keeps
// a local bytes_allocated counter (for arrow::MemoryPool::bytes_allocated) and
// a peak tracker (for max_memory).

arrow::Status OmniMemoryPoolAdapter::Allocate(int64_t size, int64_t /*alignment*/, uint8_t **out)
{
    void *p = alloc_->Alloc(size); // → automatically accounted in GetGlobalAccountedMemory()
    if (p == nullptr) {
        LogsError("OmniMemoryPoolAdapter Allocate failed: size=%lld bytesAllocated=%lld peak=%lld",
                  static_cast<long long>(size),
                  static_cast<long long>(bytesAllocated_.load()),
                  static_cast<long long>(peak_.load()));
        return arrow::Status::OutOfMemory("OmniMemoryPoolAdapter alloc failed, size: ", size);
    }
    *out = static_cast<uint8_t *>(p);
    bytesAllocated_ += size;
    int64_t cur = bytesAllocated_.load();
    peak_ = std::max(peak_.load(), cur);
    return arrow::Status::OK();
}

arrow::Status OmniMemoryPoolAdapter::Reallocate(int64_t old_size, int64_t new_size, int64_t /*alignment*/, uint8_t **ptr)
{
    void *np = alloc_->Alloc(new_size);
    if (np == nullptr) {
        LogsError("OmniMemoryPoolAdapter Reallocate failed: old_size=%lld new_size=%lld bytesAllocated=%lld peak=%lld",
                  static_cast<long long>(old_size), static_cast<long long>(new_size),
                  static_cast<long long>(bytesAllocated_.load()),
                  static_cast<long long>(peak_.load()));
        return arrow::Status::OutOfMemory("OmniMemoryPoolAdapter realloc failed, new_size: ", new_size);
    }
    if (*ptr != nullptr) {
        memcpy(np, *ptr, std::min(old_size, new_size)); // ResizableBuffer growth triggers this path
        alloc_->Free(*ptr, old_size);
    }
    *ptr = static_cast<uint8_t *>(np);
    bytesAllocated_ += (new_size - old_size);
    int64_t cur = bytesAllocated_.load();
    peak_ = std::max(peak_.load(), cur);
    return arrow::Status::OK();
}

void OmniMemoryPoolAdapter::Free(uint8_t *buffer, int64_t size, int64_t /*alignment*/)
{
    alloc_->Free(buffer, size);
    bytesAllocated_ -= size;
}

int64_t OmniMemoryPoolAdapter::bytes_allocated() const
{
    return bytesAllocated_.load();
}

int64_t OmniMemoryPoolAdapter::max_memory() const
{
    return peak_.load();
}

std::string OmniMemoryPoolAdapter::backend_name() const
{
    return "omni-allocator";
}
