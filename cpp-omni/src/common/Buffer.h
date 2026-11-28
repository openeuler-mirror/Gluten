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

#ifndef CPP_BUFFER_H
#define CPP_BUFFER_H

#include <cstring>
#include <vector>
#include <chrono>
#include <memory>
#include <list>
#include <vector/vector_common.h>

class Buffer {
public:
    // Legacy 3-arg ctor kept for
    // backwards-compat at the ~5 call sites that don't yet thread an Allocator.
    // When boundAllocator_==nullptr the dtor falls back to a function-local
    // nullBridge (legacy behaviour, accounting skipped on Free but mem freed).
    Buffer(uint8_t* data, int64_t size, int64_t capacity, bool isOmniAllocated = true)
            : data_(data),
            size_(size),
            capacity_(capacity),
            allocatedByOmni(isOmniAllocated),
            boundAllocator_(nullptr) {
    }

    // Preferred ctor — stores the
    // Allocator* that produced data_ so the dtor's Free routes symmetrically
    // to the same allocator. Caller (splitter.cpp:146 / :1085 / :1575) passes
    // options_.allocator (now per-task-pool-bound after the Splitter ctor
    // re-bind in splitter.cpp:2400-2425). This closes the AddMemory/SubMemory
    // asymmetry that left ~GB shuffle bytes unaccounted under the per-task
    // pool's listener (invisible to Spark TMM).
    Buffer(uint8_t* data, int64_t size, int64_t capacity,
           omniruntime::mem::Allocator* boundAllocator, bool isOmniAllocated = true)
            : data_(data),
            size_(size),
            capacity_(capacity),
            allocatedByOmni(isOmniAllocated),
            boundAllocator_(boundAllocator) {
    }

    ~Buffer() {
        if (allocatedByOmni && not releaseFlag) {
            // When boundAllocator_
            // is supplied, route Free through it for symmetric AddMemory/SubMemory
            // listener events on the per-task pool. nullptr → legacy nullBridge
            // (call sites that haven't been threaded yet, e.g. external-data
            // Buffer with isOmniAllocated=false ironically routes the other branch).
            if (boundAllocator_ != nullptr) {
                boundAllocator_->Free(data_, capacity_);
            } else {
                static omniruntime::mem::Allocator nullBridge{nullptr};
                nullBridge.Free(data_, capacity_);
            }
        }
    }

    void SetReleaseFlag() {
        releaseFlag = true;
    }

public:
    uint8_t * data_;
    int64_t size_;
    int64_t capacity_;
    bool allocatedByOmni = true;
    bool releaseFlag = false;
    // Non-owning. Lifetime invariant: the
    // Allocator (and the OmniTaskMemoryPool it wraps) must outlive every
    // Buffer it produced. Production: Splitter owns options_.allocator
    // (per-task pool), and the Splitter outlives its partition Buffers.
    omniruntime::mem::Allocator* boundAllocator_ = nullptr;
};

#endif //CPP_BUFFER_H