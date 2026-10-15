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
 * ---
 *
 * OmniMemoryManager — gluten::MemoryManager adapter for the Omni backend.
 *
 * Owns the allocation listener, its block-coalescing wrapper, and the task pool.
 * The pool borrows poolListener_; it delegates to blockWrap_, which borrows listener_. Member order keeps
 * these dependencies alive during pool teardown. arrowPool_ is a borrowed
 * Arrow default-pool singleton and is not tracked by the Spark listener.
 *
 * JNI nmmHandle carries a gluten::MemoryManager*. RuntimeMemoryHelper checks
 * its backend kind before accessing the Omni task pool.
 *
 * Factory registration (kind="omni") happens in OmniMemoryManager.cc via static
 * initializer at .so load time.
 */

#pragma once

#include <memory>
#include <string>

// Cross-repo includes — resolution comes from per-file INCLUDE_DIRECTORIES
// set on OmniMemoryManager.cc in gluten/cpp-omni/src/CMakeLists.txt:
//   - gluten-core: ${GLUTEN_HOME}/cpp/core/
//   - omni-core:   ${CENTRAL_ROOT}/omni/core/src/
// (Header propagation: this .h is consumed via OmniMemoryManager.cc only, so
// the per-file include paths apply transitively to it during compilation.)
//
// NB: gluten/cpp/core/memory/ArrowMemoryPool.h is NOT included here — it was
// written against a newer arrow API (override of total_bytes_allocated /
// num_allocations) than the arrow shipped in the omnioperator-euler:v2 image,
// so direct inclusion fails to compile. We use the arrow-installed
// default_memory_pool() as a non-owning pointer until the arrow-version mismatch
// is resolved (an arrow upgrade or a cpp-omni-local adapter).
#include "memory/AllocationListener.h"
#include "memory/MemoryManager.h"
#include "arrow/memory_pool.h"   // arrow::default_memory_pool()

#include "memory/omni_task_memory_pool.h"

namespace gluten {

class OmniBlockAllocationListener;
class OmniPoolAllocationListener;
class OmniSparkAllocationListener;

class OmniMemoryManager final : public MemoryManager {
public:
    OmniMemoryManager(const std::string& kind, std::unique_ptr<AllocationListener> listener);
    // Defaulted behaviour in release; under OMNI_MEM_DEBUG the body emits the per-task
    // reconciliation triangle + Arrow-pool bucket (definition in OmniMemoryManager.cc).
    ~OmniMemoryManager() override;

    OmniMemoryManager(const OmniMemoryManager&) = delete;
    OmniMemoryManager(OmniMemoryManager&&) = delete;
    OmniMemoryManager& operator=(const OmniMemoryManager&) = delete;
    OmniMemoryManager& operator=(OmniMemoryManager&&) = delete;

    // gluten::MemoryManager interface — 4 virtuals (dtor defaulted).
    arrow::MemoryPool* getArrowMemoryPool() override { return arrowPool_; }
    const MemoryUsageStats collectMemoryUsageStats() const override;
    // Omni has no SHRINK phase: only explicit SPILL may reclaim task allocations.
    const int64_t shrink(int64_t) override;
    // Synchronously reclaims spillable allocations registered with the task pool.
    const int64_t spill(int64_t size);
    // Velox uses hold() to defer pool teardown until cache eviction completes.
    // Omni has no equivalent; explicit no-op.
    void hold() override {}

    void reconcileExternalRepay(int64_t nativeFreed, int64_t repaid);
    void setSparkListener(OmniSparkAllocationListener* listener);
    // Spark-facing reservation block; call before the first allocation.
    void setReservationBlockSize(int64_t blockSize);

    // Omni-specific accessors — SparkJniWrapper.cpp reaches through these to
    // find the per-task pool from the jlong nmmHandle.
    omniruntime::mem::OmniTaskMemoryPool* getTaskMemoryPool() const { return pool_.get(); }
    AllocationListener* getListener() const { return listener_.get(); }

private:
    // Field order: listener_ → blockWrap_ → poolListener_ → pool_ → arrowPool_.
    // Destruction reverse: arrowPool_ → pool_ → poolListener_ → blockWrap_ → listener_.
    // pool_ borrows poolListener_; poolListener_ borrows blockWrap_; blockWrap_ holds raw listener_.get().
    // The pool's residual drain routes through blockWrap_'s
    // allocationChanged(-residual) which delegates to listener_'s up-call.
    // blockWrap_ MUST outlive pool_; listener_ MUST outlive blockWrap_.
    std::unique_ptr<AllocationListener> listener_;                       // OWNS (matches VeloxMemoryManager.h:80)
    std::unique_ptr<OmniBlockAllocationListener> blockWrap_;             // whole-block Spark reservation wrapper
    std::unique_ptr<OmniPoolAllocationListener> poolListener_;
    std::unique_ptr<omniruntime::mem::OmniTaskMemoryPool> pool_;          // OWNS — created with poolListener_.get()
    OmniSparkAllocationListener* sparkListener_{nullptr}; // non-owning alias of listener_
    arrow::MemoryPool* arrowPool_{nullptr};                              // NON-OWNING — points at arrow::default_memory_pool()
                                                                          // singleton (Arrow owns it); see the header comment
                                                                          // about gluten core's ArrowMemoryPool API mismatch.
};

} // namespace gluten
