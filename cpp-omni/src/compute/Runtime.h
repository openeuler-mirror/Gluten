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
 */

#pragma once

#include "WholeStageResultIterator.h"
#include "util/config/ConfigBase.h"
#include "OmniPlanConverter.h"
// Free-function downcast bridge — see header for shadow-trap rationale.
#include "memory/RuntimeMemoryHelper.h"

// Forward decl of gluten::MemoryManager so Runtime.h does NOT pull
// gluten/cpp/core/memory/MemoryManager.h (would trigger compute/ResultIterator.h
// shadow chain in every consumer TU that has cpp/core on its include path).
namespace gluten {
class MemoryManager;
}

namespace omniruntime {
class Runtime {
public:
    // Ctor now takes the per-task gluten::MemoryManager*
    // decoded from the JNI nmmHandle in SparkJniWrapper.cpp. The pointer is
    // non-owning — Java-side TaskResources owns the OmniNativeMemoryManager
    // lifecycle (priority=0, released after Runtime priority=20).
    explicit Runtime(std::string kind, gluten::MemoryManager *memoryManager,
                     const std::unordered_map<std::string, std::string> &confMap);

    void ParsePlan(const uint8_t *data, int32_t size, std::optional<std::string> dumpFile);

    void ParseSplitInfo(const uint8_t *data, int32_t size, std::optional<std::string> dumpFile);

    std::unique_ptr<ResultIterator> CreateResultIterator(const std::string &spillDir,
        const std::vector<std::shared_ptr<ResultIterator>> &inputs = {},
        const std::unordered_map<std::string, std::string> &sessionConf = {});

    std::string PlanString(bool details, const std::unordered_map<std::string, std::string> &sessionConf);

    void DumpConf(const std::string &path);

    std::shared_ptr<const PlanNode> GetOmniPlan()
    {
        return omniPlan_;
    }

    const std::unordered_map<std::string, std::string> &GetConfMap()
    {
        return confMap_;
    }
    void setLocalFiles(std::vector<::substrait::ReadRel_LocalFiles> localFiles)
    {
        localFiles_ = localFiles;
    }

    // Non-owning accessor for the gluten
    // memory manager threaded through from the JNI layer. Callers downcast to
    // gluten::OmniMemoryManager* when they need the omni-specific per-task pool.
    gluten::MemoryManager *GetMemoryManager() const { return memoryManager_; }

    // Returns the per-task OmniTaskMemoryPool* if memoryManager_ is a
    // gluten::OmniMemoryManager, nullptr otherwise. Delegates to the
    // free-function bridge defined in OmniMemoryManager.cc — see
    // memory/RuntimeMemoryHelper.h for the shadow-trap rationale.
    omniruntime::mem::OmniTaskMemoryPool *GetTaskMemoryPool() const
    {
        return omniruntime::ToOmniTaskPoolOrNull(memoryManager_);
    }

private:
    std::string kind_;
    // Non-owning back-pointer; Java-side TaskResources owns the
    // OmniNativeMemoryManager lifecycle.
    gluten::MemoryManager *memoryManager_ = nullptr;
    std::unordered_map<std::string, std::string> confMap_;
    std::shared_ptr<const PlanNode> omniPlan_;
    std::shared_ptr<config::ConfigBase> omniCfg_;
    ::substrait::Plan substraitPlan_;
    std::vector<::substrait::ReadRel_LocalFiles> localFiles_;
};
}
