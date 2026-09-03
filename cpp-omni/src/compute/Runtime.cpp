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
    auto effectiveConf = confMap_;
    for (const auto &entry : sessionConf) {
        effectiveConf[entry.first] = entry.second;
    }
    OmniPlanConverter omniPlanConverter(inputs, GetMemoryPool(), effectiveConf);
    omniPlan_ = omniPlanConverter.ToOmniPlan(substraitPlan_, std::move(localFiles_));

    // Scan node can be required.
    std::vector<PlanNodeId> scanIds;
    std::vector<PlanNodeId> streamIds;
    std::vector<std::shared_ptr<SplitInfo>> scanInfos;

    const std::unordered_map<omniruntime::PlanNodeId, std::shared_ptr<SplitInfo>>& splitInfoMap
        = omniPlanConverter.splitInfos();
    for (const auto& pair : splitInfoMap) {
        omniruntime::PlanNodeId nodeId = pair.first;
        const std::shared_ptr<SplitInfo>& splitInfoPtr = pair.second;
        if (!splitInfoPtr) {
            continue;
        }
        if (splitInfoPtr->isStream) {
            streamIds.emplace_back(nodeId);
        } else {
            scanInfos.emplace_back(splitInfoPtr);
            scanIds.emplace_back(nodeId);
        }
    }

    auto wholeStageIter = std::make_unique<WholeStageResultIterator>(MemoryManager::GetGlobalMemoryManager(), omniPlan_,
        scanIds, streamIds, spillDir, effectiveConf, scanInfos);
    return std::move(std::make_unique<ResultIterator>(std::move(wholeStageIter)));
}
}
