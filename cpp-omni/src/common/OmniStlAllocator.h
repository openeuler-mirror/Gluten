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

#ifndef SPARK_THESTRAL_PLUGIN_OMNISTLALLOCATOR_H
#define SPARK_THESTRAL_PLUGIN_OMNISTLALLOCATOR_H

#include <cstddef>
#include <limits>
#include <new>
#include <type_traits>

// Forward-declare omni's per-task pool-bound Allocator. The real Alloc/Free
// calls are routed through the two extern thunks below (defined in
// shuffle/splitter.cpp, which already holds the full omniruntime::mem::Allocator
// definition). Keeping the heavy memory/allocator.h closure out of this header
// lets it be pulled into BinaryLocation.h / common.h cheaply and avoids the
// compute/ResultIterator.h include-shadow trap documented in src/CMakeLists.txt.
namespace omniruntime { namespace mem { class Allocator; } }

// Route a raw byte allocation/free through a pool-bound omniruntime::mem::Allocator
// so the bytes are charged to the per-task OmniTaskMemoryPool. A nullptr allocator
// falls back to std::malloc/std::free (test / no-pool path). Defined in splitter.cpp.
void *OmniStlAllocBytes(omniruntime::mem::Allocator *alloc, std::size_t bytes);
void OmniStlFreeBytes(omniruntime::mem::Allocator *alloc, void *ptr, std::size_t bytes) noexcept;

// Minimal C++17 stateful STL allocator that charges every allocation to the
// per-task pool behind `alloc_`. Used for the shuffle vc_list (the VCLocation
// index storage), which is the #1 pool-bypassing allocation in the splitter
// (~122 GB cumulative across SF1, measured). The allocator pointer travels with
// the container on copy/move/swap so each buffer is freed through the same pool
// it was allocated from.
template <typename T>
class OmniStlAllocator {
public:
    using value_type = T;
    using pointer = T *;
    using const_pointer = const T *;
    using reference = T &;
    using const_reference = const T &;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;
    template <typename U> struct rebind { using other = OmniStlAllocator<U>; };

    using propagate_on_container_copy_assignment = std::true_type;
    using propagate_on_container_move_assignment = std::true_type;
    using propagate_on_container_swap = std::true_type;

    OmniStlAllocator() noexcept : alloc_(nullptr) {}
    explicit OmniStlAllocator(omniruntime::mem::Allocator *alloc) noexcept : alloc_(alloc) {}
    template <typename U>
    OmniStlAllocator(const OmniStlAllocator<U> &other) noexcept : alloc_(other.alloc_) {}

    T *allocate(std::size_t n)
    {
        if (n > std::numeric_limits<std::size_t>::max() / sizeof(T)) {
            throw std::bad_array_new_length();
        }
        return static_cast<T *>(OmniStlAllocBytes(alloc_, n * sizeof(T)));
    }

    void deallocate(T *ptr, std::size_t n) noexcept
    {
        OmniStlFreeBytes(alloc_, ptr, n * sizeof(T));
    }

    template <typename U>
    bool operator==(const OmniStlAllocator<U> &other) const noexcept { return alloc_ == other.alloc_; }
    template <typename U>
    bool operator!=(const OmniStlAllocator<U> &other) const noexcept { return alloc_ != other.alloc_; }

    // Public so the converting constructor can read across instantiations.
    omniruntime::mem::Allocator *alloc_;
};

#endif // SPARK_THESTRAL_PLUGIN_OMNISTLALLOCATOR_H
