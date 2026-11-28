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

#ifndef SPARK_THESTRAL_PLUGIN_BINARYLOCATION_H
#define SPARK_THESTRAL_PLUGIN_BINARYLOCATION_H
#include <cstdint>
#include <vector>
#include "OmniStlAllocator.h"
class VCLocation {
public:
    VCLocation(uint64_t vc_addr, uint32_t vc_len, bool isnull)
            : vc_len(vc_len), vc_addr(vc_addr), is_null(isnull) {
            }
    ~VCLocation() {
    }
    uint32_t get_vc_len() {
        return vc_len;
    }

    uint64_t get_vc_addr() {
        return vc_addr;
    }

    bool get_is_null() {
        return is_null;
    }

public:
    uint32_t vc_len;
    uint64_t vc_addr;
    bool is_null;
};

// vc_list holds POD VCLocation index entries (pointer + length + null flag) for
// one shuffle batch. It is the single largest pool-bypassing allocation in the
// splitter (measured ~122 GB cumulative across SF1). Backing it with
// OmniStlAllocator charges its storage to the per-task pool. The allocator is
// supplied at construction from the splitter's pool-bound options_.allocator
// (Splitter rebinds that to taskMemoryPool_->getBoundAllocator()); a nullptr
// allocator (test / no-pool path) falls back to std::malloc.
using VCLocationList = std::vector<VCLocation, OmniStlAllocator<VCLocation>>;

class VCBatchInfo {
public:
    explicit VCBatchInfo(uint32_t vcb_capacity, omniruntime::mem::Allocator *alloc = nullptr)
            : vcb_capacity(vcb_capacity),
              vcb_total_len(0),
              vc_list(OmniStlAllocator<VCLocation>(alloc)) {
        // No eager reserve(vcb_capacity). Now that vc_list is pool-charged, an
        // eager reserve of spill_batch_row_num (=10000 => 240 KB) per batch
        // charges the per-task pool for capacity that demand-paging never makes
        // resident: AddMemory counts the full reservation but untouched pages
        // never hit RSS. Measured on SF1 this over-charged the pool by GBs on
        // shuffle-heavy queries (q4: +4976 MB charged vs +254 MB resident =>
        // accounting_pct shot to 337%). Letting vc_list grow geometrically to
        // the actual fill keeps the charge within ~1-2x of resident.
    }

    // Keep move operations explicit: this transfers the allocator-aware
    // vc_list buffer without copying it, while the default destructor releases
    // the storage through the allocator carried by the vector.
    VCBatchInfo(VCBatchInfo&& other) noexcept
        : vc_list(std::move(other.vc_list)),
          vcb_capacity(other.vcb_capacity),
          vcb_total_len(other.vcb_total_len),
          hasNullFlag(other.hasNullFlag) {
        other.vcb_capacity = 0;
        other.vcb_total_len = 0;
    }

    VCBatchInfo& operator=(VCBatchInfo&& other) noexcept {
        if (this != &other) {
            vc_list = std::move(other.vc_list);
            vcb_capacity = other.vcb_capacity;
            vcb_total_len = other.vcb_total_len;
            hasNullFlag = other.hasNullFlag;
            other.vcb_capacity = 0;
            other.vcb_total_len = 0;
        }
        return *this;
    }

    ~VCBatchInfo() = default;

    uint32_t getVcbCapacity() {
        return vcb_capacity;
    }

    uint32_t getVcbTotalLen() {
        return vcb_total_len;
    }

    VCLocationList &getVcList() {
        return vc_list;
    }

    const VCLocationList &getVcList() const {
        return vc_list;
    }

    bool hasNull() const {
        return hasNullFlag;
    }

    void SetNullFlag(bool hasNull) {
        hasNullFlag = hasNull;
    }

public:
    uint32_t vcb_capacity;
    uint32_t vcb_total_len;
    VCLocationList vc_list;
    bool hasNullFlag = false;

};
#endif //SPARK_THESTRAL_PLUGIN_BINARYLOCATION_H
