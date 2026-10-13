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
 * Free-function bridge for downcasting a
 * gluten::MemoryManager* to its underlying OmniTaskMemoryPool*.
 *
 * Why a free function in its own tiny header:
 *
 * The dynamic_cast<gluten::OmniMemoryManager*> needs gluten/cpp/core headers
 * (for the base type) and the implementation must live in a TU that has
 * `${GLUTEN_HOME}/cpp/core` on its include path. But applying that path to
 * `compute/Runtime.cpp` (or to a TU that #includes Runtime.h) shadows
 * OmniOperator's compute/ResultIterator.h chain transitively pulled in by
 * WholeStageResultIterator.h → compute/task.h → plannode/RowVectorStream.h
 * (which uses unqualified `ResultIterator`). Result: 30+ compile errors.
 *
 * Resolution: implementation lives inside OmniMemoryManager.cc (which already
 * has the cpp/core per-file include and crucially does NOT pull any of
 * Omni's compute headers). Consumers (Runtime.cpp) include only this tiny
 * header — two forward decls and one function decl — pulling no shadow chain.
 */

#pragma once

#include <cstdint>
#include <string>

namespace gluten {
class MemoryManager;
class OmniSparkAllocationListener;
}
namespace omniruntime::mem {
class OmniTaskMemoryPool;
}

namespace omniruntime {

// Returns the OmniTaskMemoryPool* embedded in `mm` if `mm` is dynamically a
// gluten::OmniMemoryManager. Otherwise returns nullptr (null input, internal
// backend, mock, etc.). The returned pool is owned by `mm`; do not delete.
mem::OmniTaskMemoryPool *ToOmniTaskPoolOrNull(gluten::MemoryManager *mm);

// Bind the manager-owned Spark listener for ledger-only external repayments.
void BindOmniSparkListener(gluten::MemoryManager* mm, gluten::OmniSparkAllocationListener* listener);

// Adjust both listeners after the JVM has already repaid Spark and restore
// any unrepaid part of the native shrink offer to the pool reservation marker.
void ReconcileExternalRepay(
    gluten::MemoryManager *mm, int64_t nativeFreed, int64_t repaid);

// Dispatch explicit Omni SPILL without exposing Omni's compute includes to JNI.
int64_t SpillOmniMemory(gluten::MemoryManager *mm, int64_t size);

// Attach the Spark task/query identity to the per-task pool so native stderr
// can be attributed without relying on driver-side time windows.
void SetOmniTaskTelemetryContext(
    gluten::MemoryManager *mm,
    const std::string &runId,
    const std::string &queryId,
    const std::string &round,
    int64_t taskAttemptId);

} // namespace omniruntime
