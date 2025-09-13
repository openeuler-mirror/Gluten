/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include <utility>
#include "compute/ProtobufUtils.h"
#include "Runtime.h"

namespace omniruntime {
Runtime::Runtime(std::string kind, const std::unordered_map<std::string, std::string> &confMap)
    : kind_(std::move(kind)), confMap_(confMap)
{
    // Refresh session config.
    omniCfg_ = std::make_shared<config::ConfigBase>(std::unordered_map<std::string, std::string>(confMap_));
}

void Runtime::ParsePlan(const uint8_t *data, int32_t size, std::optional<std::string> dumpFile)
{
    OMNI_CHECK(ParseProtobuf(data, size, &substraitPlan_) == true, "Parse substrait plan failed");
}

std::unique_ptr<ResultIterator> Runtime::CreateResultIterator(const std::string &spillDir,
    const std::vector<std::shared_ptr<ResultIterator>> &inputs,
    const std::unordered_map<std::string, std::string> &sessionConf)
{
    OmniPlanConverter omniPlanConverter(inputs, GetMemoryPool(), sessionConf);
    omniPlan_ = omniPlanConverter.ToOmniPlan(substraitPlan_, std::move(localFiles_));

    // Scan node can be required.
    std::vector<PlanNodeId> scanIds;
    std::vector<PlanNodeId> streamIds;

    auto wholeStageIter = std::make_unique<WholeStageResultIterator>(MemoryManager::GetGlobalMemoryManager(), omniPlan_,
        scanIds, streamIds, spillDir, confMap_);
    return std::move(std::make_unique<ResultIterator>(std::move(wholeStageIter)));
}
}
