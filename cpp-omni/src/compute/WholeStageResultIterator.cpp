/**
 * Copyright (C) 2025-2025. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "WholeStageResultIterator.h"
#include "compute/reason.h"
#include "util/config/QueryConfig.h"
#include "config/OmniConfig.h"

namespace omniruntime {
std::string BoolToString(const bool value)
{
    return value ? "true" : "false";
}

WholeStageResultIterator::WholeStageResultIterator(MemoryManager *memoryManager,
    const std::shared_ptr<const PlanNode> &planNode, const std::vector<PlanNodeId> &scanNodeIds,
    const std::vector<PlanNodeId> &streamIds, const std::string &spillDir,
    const std::unordered_map<std::string, std::string> &confMap)
    : memoryManager_(memoryManager), omniPlan_(planNode),
    omniCfg_(std::make_shared<config::ConfigBase>(std::unordered_map<std::string, std::string>(confMap))),
    scanNodeIds_(scanNodeIds), streamIds_(streamIds)
{
    // Create task instance.
    config::QueryConfig queryConfig(GetQueryContextConf(spillDir));
    std::unordered_set<PlanNodeId> emptySet;
    PlanFragment planFragment{planNode, ExecutionStrategy::K_UNGROUPED, 1, emptySet};
    task_ = std::make_shared<OmniTask>(planFragment, std::move(queryConfig));
}

VectorBatch *WholeStageResultIterator::Next()
{
    VectorBatch *vectorBatch = nullptr;
    while (true) {
        auto future = OmniFuture::makeEmpty();
        auto out = task_->Next(&future);
        if (!future.valid()) {
            // Not need to wait. Break.
            vectorBatch = out;
            break;
        }
        // Omni suggested to wait. This might be because another thread (e.g., background io thread) is spilling the task.
        OMNI_CHECK(out == nullptr, "Expected to wait but still got non-null output from Omni task");
        future.wait();
    }
    if (vectorBatch == nullptr) {
        return nullptr;
    }
    uint64_t numRows = vectorBatch->GetRowCount();
    if (numRows == 0) {
        return nullptr;
    }
    return vectorBatch;
}

std::unordered_map<std::string, std::string> WholeStageResultIterator::GetQueryContextConf(
    const std::string &spillDir) const
{
    std::unordered_map<std::string, std::string> configs = {};

    try {
        configs[config::QueryConfig::KSpillDir] = spillDir;
        if (spillStrategy_ == "none") {
            configs[config::QueryConfig::kSpillEnabled] = "false";
        } else {
            configs[config::QueryConfig::kSpillEnabled] = "true";
        }
        configs[config::QueryConfig::kAggregationSpillEnabled] = std::to_string(
            omniCfg_->Get<bool>(kAggregationSpillEnabled, true));
        configs[config::QueryConfig::kJoinSpillEnabled] = BoolToString(omniCfg_->Get<bool>(kJoinSpillEnabled, true));
        configs[config::QueryConfig::kOrderBySpillEnabled] = BoolToString(omniCfg_->Get<bool>(kOrderBySpillEnabled,
            true));
        configs[config::QueryConfig::kMaxSpillLevel] = std::to_string(omniCfg_->Get<int32_t>(kMaxSpillLevel, 4));
        configs[config::QueryConfig::kMaxSpillFileSize] = std::to_string(
            omniCfg_->Get<uint64_t>(kMaxSpillFileSize, 1L * 1024 * 1024 * 1024));
        configs[config::QueryConfig::kMaxSpillRunRows] = std::to_string(
            omniCfg_->Get<uint64_t>(kMaxSpillRunRows, 3L * 1024 * 1024));
        configs[config::QueryConfig::kMaxSpillBytes] = std::to_string(omniCfg_->Get<uint64_t>(kMaxSpillBytes,
            107374182400LL));
        configs[config::QueryConfig::kSpillStartPartitionBit] = std::to_string(
            omniCfg_->Get<uint8_t>(kSpillStartPartitionBit, 29));
        configs[config::QueryConfig::kSpillNumPartitionBits] = std::to_string(
            omniCfg_->Get<uint8_t>(kSpillPartitionBits, 3));
        configs[config::QueryConfig::kSpillableReservationGrowthPct] = std::to_string(
            omniCfg_->Get<uint8_t>(kSpillableReservationGrowthPct, 25));
        configs[config::QueryConfig::kSpillPrefixSortEnabled] = omniCfg_->Get<std::string>(kSpillPrefixSortEnabled,
            "false");
        if (omniCfg_->Get<bool>(kSparkShuffleSpillCompress, true)) {
            configs[config::QueryConfig::kSpillCompressionKind] = omniCfg_->Get<std::string>(kSpillCompressionKind,
                omniCfg_->Get<std::string>(kCompressionKind, "lz4"));
        } else {
            configs[config::QueryConfig::kSpillCompressionKind] = "none";
        }
    } catch (const std::invalid_argument &err) {
        const std::string errDetails = err.what();
        throw std::runtime_error("Invalid conf arg: " + errDetails);
    }
    return configs;
}
}
