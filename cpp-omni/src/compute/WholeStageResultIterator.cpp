/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "WholeStageResultIterator.h"
#include "compute/reason.h"
#include "util/config/QueryConfig.h"
#include "config/OmniConfig.h"
#include "compute/plannode_stats.h"
#include "Runtime.h"

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
    getOrderedNodeIds(omniPlan_, orderedNodeIds_);
}

void WholeStageResultIterator::getOrderedNodeIds(const std::shared_ptr<const PlanNode>& planNode,
                                                 std::vector<PlanNodeId>& nodeIds)
{
    const auto& sourceNodes = planNode->Sources();
    for (const auto& sourceNode : sourceNodes) {
        // Post-order traversal.
        getOrderedNodeIds(sourceNode, nodeIds);
    }
    nodeIds.emplace_back(planNode->Id());
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
        // Omni suggested to wait.
        // This might be because another thread (e.g., background io thread) is spilling the task.
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
        configs[config::QueryConfig::kAggregationSpillEnabled] = BoolToString(
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
        configs[config::QueryConfig::KSpillHashAggRowThreshold] = std::to_string(
            omniCfg_->Get<int32_t>(KSpillHashAggRowThreshold, INT32_MAX));
        configs[config::QueryConfig::KSpillSortRowThreshold] = std::to_string(
            omniCfg_->Get<int32_t>(KSpillSortRowThreshold, INT32_MAX));
        configs[config::QueryConfig::KColumnarSpillMemThreshold] = std::to_string(
            omniCfg_->Get<uint64_t>(KColumnarSpillMemThreshold, 90));
        configs[config::QueryConfig::KColumnarSpillWriteBufferSize] = std::to_string(
            omniCfg_->Get<uint64_t>(KColumnarSpillWriteBufferSize, 4121440L));
        configs[config::QueryConfig::KColumnarSpillDirDiskReserveSize] = std::to_string(
            omniCfg_->Get<uint64_t>(KColumnarSpillDirDiskReserveSize, 10737418240L));
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

void WholeStageResultIterator::CollectMetrics()
{
    if (metrics_) {
        // The metrics has already been created.
        LogsWarn("The metrics has already been created.");
        return;
    }
    const auto& taskStats = task_->GetTaskStats();
    if (taskStats.executionStartTimeMs == 0) {
        LogsWarn("Skip collect task metrics since task did not call next().");
        return;
    }
    auto planStats = omniruntime::compute::ToPlanStats(taskStats);
    int statsNum = 0;
    for (size_t idx = 0; idx < orderedNodeIds_.size(); idx++) {
        const auto& nodeId = orderedNodeIds_[idx];
        if (planStats.find(nodeId) == planStats.end()) {
            if (omittedNodeIds_.find(nodeId) == omittedNodeIds_.end()) {
                LogsWarn("Not found node id: %d", nodeId);
                throw std::runtime_error("Node id cannot be found in plan status.");
            }
            // Special handing for Filter over Project case. Filter metrics areomitted.
            statsNum += 1;
            continue;
        }
        statsNum += planStats.at(nodeId).operatorStats.size();
    }
    LogsDebug("planStats size: %d, statsNum is %d.", planStats.size(), statsNum);
    metrics_ = std::make_unique<omniruntime::OmniMetrics>(statsNum);
    int metricIndex = 0;
    for (size_t idx = 0; idx < orderedNodeIds_.size(); idx++) {
        const auto& nodeId = orderedNodeIds_[idx];
        if (planStats.find(nodeId) == planStats.end()) {
            metrics_->get(omniruntime::OmniMetrics::kOutputRows)[metricIndex] = 0;
            metrics_->get(omniruntime::OmniMetrics::kNumOutputVecBatches)[metricIndex] = 0;
            metrics_->get(omniruntime::OmniMetrics::kOutputBytes)[metricIndex] = 0;
            metrics_->get(omniruntime::OmniMetrics::kNumInputVecBatches)[metricIndex] = 0;
            metricIndex += 1;
            LogsWarn("no nodeId %d in planState and continue.", nodeId);
            continue;
        }
        const auto& stats = planStats.at(nodeId);
        buildMetricsForNative(stats, metricIndex);
        metricIndex += 1;
    }
}

void WholeStageResultIterator::buildMetricsForNative(
    const omniruntime::compute::PlanNodeStats& stats, int metricIndex)
{
    for (const auto& entry : stats.operatorStats) {
        const auto& second = entry.second;
        metrics_->get(omniruntime::OmniMetrics::kInputRows)[metricIndex] = second->inputRows;
        metrics_->get(omniruntime::OmniMetrics::kNumInputVecBatches)[metricIndex] = second->numInputVecBatches;
        metrics_->get(omniruntime::OmniMetrics::kInputBytes)[metricIndex] = second->inputBytes;

        metrics_->get(omniruntime::OmniMetrics::kRawInputRows)[metricIndex] = second->rawInputRows;
        metrics_->get(omniruntime::OmniMetrics::kRawInputBytes)[metricIndex] = second->rawInputBytes;

        metrics_->get(omniruntime::OmniMetrics::kOutputRows)[metricIndex] = second->outputRows;
        metrics_->get(omniruntime::OmniMetrics::kNumOutputVecBatches)[metricIndex] = second->numOutputVecBatches;
        metrics_->get(omniruntime::OmniMetrics::kOutputBytes)[metricIndex] = second->outputBytes;

        metrics_->get(omniruntime::OmniMetrics::kSpilledBytes)[metricIndex] = second->spilledBytes;
        metrics_->get(omniruntime::OmniMetrics::kSpilledRows)[metricIndex] = second->spilledRows;
        metrics_->get(omniruntime::OmniMetrics::kSpilledPartitions)[metricIndex] = second->spilledPartitions;
        metrics_->get(omniruntime::OmniMetrics::kCpuCount)[metricIndex] = second->cpuWallTiming.count;
        metrics_->get(omniruntime::OmniMetrics::kSpilledFiles)[metricIndex] = second->spilledFiles;
        metrics_->get(omniruntime::OmniMetrics::kAddInputTime)[metricIndex] = second->addInputTime.cpuNanos;
        metrics_->get(omniruntime::OmniMetrics::kGetOutputTime)[metricIndex] = second->getOutputTime.cpuNanos;
        metrics_->get(omniruntime::OmniMetrics::kAddInputCpuCount)[metricIndex] = second->addInputTime.count;
        metrics_->get(omniruntime::OmniMetrics::kGetOutputCpuCount)[metricIndex] = second->getOutputTime.count;

        metrics_->get(omniruntime::OmniMetrics::kBuildInputRows)[metricIndex] = second->buildInputRows;
        metrics_->get(omniruntime::OmniMetrics::kBuildNumInputVecBatches)[metricIndex] =
            second->buildNumInputVecBatches;
        metrics_->get(omniruntime::OmniMetrics::kBuildAddInputTime)[metricIndex] = second->buildAddInputTime.cpuNanos;
        metrics_->get(omniruntime::OmniMetrics::kBuildGetOutputTime)[metricIndex] = second->buildGetOutputTime.cpuNanos;

        metrics_->get(omniruntime::OmniMetrics::kLookupInputRows)[metricIndex] = second->lookupInputRows;
        metrics_->get(omniruntime::OmniMetrics::kLookupNumInputVecBatches)[metricIndex] =
            second->lookupNumInputVecBatches;
        metrics_->get(omniruntime::OmniMetrics::kLookupOutputRows)[metricIndex] = second->lookupOutputRows;
        metrics_->get(omniruntime::OmniMetrics::kLookupNumOutputVecBatches)[metricIndex] =
            second->lookupNumOutputVecBatches;
        metrics_->get(omniruntime::OmniMetrics::kLookupAddInputTime)[metricIndex] =
            second->lookupAddInputTime.cpuNanos;
        metrics_->get(omniruntime::OmniMetrics::kLookupGetOutputTime)[metricIndex] =
            second->lookupGetOutputTime.cpuNanos;
    }
}
}
