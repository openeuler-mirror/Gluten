/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Description: explicit bridge between the Omni and Gluten listener contracts.
 */
#pragma once

#include "memory/AllocationListener.h"
#include "memory/pool_listener.h"

namespace gluten {

// Both interfaces are included in this translation unit. Any incompatible
// change to either contract is therefore a compile error, not a vtable guess.
// The delegate must outlive this adapter, which must outlive its Omni pool.
class OmniPoolAllocationListener final : public omniruntime::mem::PoolListener {
public:
    explicit OmniPoolAllocationListener(AllocationListener& delegate) : delegate_(delegate) {}
    void allocationChanged(int64_t diff) override { delegate_.allocationChanged(diff); }
    int64_t currentBytes() override { return delegate_.currentBytes(); }
    int64_t peakBytes() override { return delegate_.peakBytes(); }
private:
    AllocationListener& delegate_;
};

} // namespace gluten
