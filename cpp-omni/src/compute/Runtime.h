/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#pragma once

#include "WholeStageResultIterator.h"
#include "util/config/ConfigBase.h"
#include "OmniPlanConverter.h"

namespace omniruntime {
class Runtime {
public:
    explicit Runtime(std::string kind, const std::unordered_map<std::string, std::string> &confMap);

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

private:
    std::string kind_;
    std::unordered_map<std::string, std::string> confMap_;
    std::shared_ptr<const PlanNode> omniPlan_;
    std::shared_ptr<config::ConfigBase> omniCfg_;
    ::substrait::Plan substraitPlan_;
    std::vector<::substrait::ReadRel_LocalFiles> localFiles_;
};
}
