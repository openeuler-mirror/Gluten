/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "WholeStageResultIterator.h"
#include "compute/reason.h"
#include "util/config/QueryConfig.h"
#include "config/OmniConfig.h"
#include "compute/plannode_stats.h"
#include "Runtime.h"
#include <nlohmann/json.hpp>

namespace omniruntime {
std::string BoolToString(const bool value)
{
    return value ? "true" : "false";
}

std::string IcebergDeleteContentToString(IcebergDeleteContent content)
{
    switch (content) {
        case IcebergDeleteContent::kPositionDeletes:
            return "position";
        case IcebergDeleteContent::kEqualityDeletes:
            return "equality";
        default:
            return "data";
    }
}

std::string FileFormatToString(FileFormat format)
{
    switch (format) {
        case FileFormat::PARQUET:
            return "parquet";
        case FileFormat::ORC:
            return "orc";
        default:
            return "unknown";
    }
}

std::unordered_map<std::string, std::string> BuildIcebergCustomSplitInfo(
    const std::shared_ptr<SplitInfo>& scanInfo,
    int idx)
{
    auto icebergSplitInfo = std::dynamic_pointer_cast<IcebergSplitInfo>(scanInfo);
    if (icebergSplitInfo == nullptr) {
        return {};
    }
    std::unordered_map<std::string, std::string> customSplitInfo = {
        {"table_format", "iceberg"}
    };
    if (idx >= icebergSplitInfo->deleteFilesVec.size()) {
        return customSplitInfo;
    }
    const auto& deleteFiles = icebergSplitInfo->deleteFilesVec[idx];
    if (deleteFiles.empty()) {
        return customSplitInfo;
    }

    nlohmann::json deleteFilesJson = nlohmann::json::array();
    for (const auto& deleteFile : deleteFiles) {
        deleteFilesJson.push_back({
            {"content", IcebergDeleteContentToString(deleteFile.content)},
            {"format", FileFormatToString(deleteFile.format)},
            {"path", deleteFile.path},
            {"recordCount", deleteFile.recordCount},
            {"fileSize", deleteFile.fileSize}
        });
    }
    customSplitInfo["iceberg_delete_files"] = deleteFilesJson.dump();
    return customSplitInfo;
}

WholeStageResultIterator::WholeStageResultIterator(MemoryManager *memoryManager,
    const std::shared_ptr<const PlanNode> &planNode, const std::vector<PlanNodeId> &scanNodeIds,
    const std::vector<PlanNodeId> &streamIds, const std::string &spillDir,
    const std::unordered_map<std::string, std::string> &confMap,
    const std::vector<std::shared_ptr<SplitInfo>>& scanSplitInfos,
    int32_t partitionId)
    : memoryManager_(memoryManager), omniPlan_(planNode),
    omniCfg_(std::make_shared<config::ConfigBase>(std::unordered_map<std::string, std::string>(confMap))),
    scanNodeIds_(scanNodeIds), streamIds_(streamIds), scanInfos_(scanSplitInfos), partitionId_(partitionId)
{
    // Create task instance.
    config::QueryConfig queryConfig(GetQueryContextConf(spillDir));
    std::unordered_set<PlanNodeId> emptySet;
    PlanFragment planFragment{planNode, ExecutionStrategy::K_UNGROUPED, 1, emptySet};
    task_ = std::make_shared<OmniTask>(planFragment, std::move(queryConfig));
    getOrderedNodeIds(omniPlan_, orderedNodeIds_);

    if (!omniruntime::connector::getConnector(kHiveConnectorId())) {
        omniruntime::connector::registerConnector(std::make_shared<HiveConnector>(kHiveConnectorId(), omniCfg_));
    }
    splits_.reserve(scanInfos_.size());
    if (scanNodeIds_.size() != scanInfos_.size()) {
        throw std::runtime_error("Invalid scan information.");
    }
    for (const auto& scanInfo : scanInfos_) {
        const auto& paths = scanInfo->paths;
        const auto& starts = scanInfo->starts;
        const auto& lengths = scanInfo->lengths;
        const auto& properties = scanInfo->properties;
        const auto& format = scanInfo->format;
        const auto& partitionColumns = scanInfo->partitionColumns;
        const auto& metadataColumns = scanInfo->metadataColumns;
        std::vector<std::shared_ptr<ConnectorSplit>> connectorSplits;
        connectorSplits.reserve(paths.size());
        for (int idx = 0; idx < paths.size(); idx++) {
            auto partitionColumn = partitionColumns[idx];
            auto metadataColumn = metadataColumns[idx];
            std::unordered_map<std::string, std::optional<std::string>> partitionKeys;
            ConstructPartitionColumns(partitionKeys, partitionColumn);
            std::shared_ptr<ConnectorSplit> split;
            split = std::make_shared<HiveConnectorSplit>(
                kHiveConnectorId(),
                paths[idx],
                format,
                starts[idx],
                lengths[idx],
                partitionKeys,
                BuildIcebergCustomSplitInfo(scanInfo, idx),
                nullptr,
                std::unordered_map<std::string, std::string>(),
                std::unordered_map<std::string, std::string>(),
                0,
                true,
                std::unordered_map<std::string, std::string>(),
                properties[idx]);
            connectorSplits.emplace_back(split);
        }
        std::vector<Split> scanSplits;
        scanSplits.reserve(connectorSplits.size());
        for (const auto& connectorSplit : connectorSplits) {
            auto splitCopy = connectorSplit;
            int32_t groupId = -1;
            scanSplits.emplace_back(Split(std::move(splitCopy), groupId));
        }
        splits_.emplace_back(scanSplits);
    }
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

void WholeStageResultIterator::ConstructPartitionColumns(
    std::unordered_map<std::string, std::optional<std::string>>& partitionKeys,
    const std::unordered_map<std::string, std::string>& map)
{
    for (const auto& partitionColumn : map) {
        auto key = partitionColumn.first;
        const auto value = partitionColumn.second;
        if (IsNullPartitionMarker(value)) {
            partitionKeys[key] = std::nullopt;
        } else {
            partitionKeys[key] = value;
        }
    }
}

void WholeStageResultIterator::TryAddSplitsToTask()
{
    if (noMoreSplits_) {
        return;
    }
    for (int idx = 0; idx < scanNodeIds_.size(); idx++) {
        for (auto& split : splits_[idx]) {
            task_->addSplit(scanNodeIds_[idx], std::move(split));
        }
        task_->noMoreSplits(scanNodeIds_[idx]);
    }
    noMoreSplits_ = true;
}


VectorBatch *WholeStageResultIterator::Next()
{
    TryAddSplitsToTask();
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

    configs[config::QueryConfig::kSparkPartitionId] = std::to_string(partitionId_);

    try {
        if (omniCfg_->ValueExists(kDefaultSessionTimezone)) {
            configs[config::QueryConfig::kSessionTimezone] = omniCfg_->Get<std::string>(kDefaultSessionTimezone, "");
        }
        if (omniCfg_->ValueExists(kSessionTimezone)) {
            configs[config::QueryConfig::kSessionTimezone] = omniCfg_->Get<std::string>(kSessionTimezone, "");
        }
        // Adjust timestamp according to the above configured session timezone.
        configs[config::QueryConfig::kAdjustTimestampToTimezone] = "true";
        configs[config::QueryConfig::KSpillDir] = spillDir;
        if (spillStrategy_ == "none") {
            configs[config::QueryConfig::kSpillEnabled] = "false";
        } else {
            configs[config::QueryConfig::kSpillEnabled] = "true";
        }
        configs[config::QueryConfig::kAggregationSpillEnabled] = BoolToString(
            omniCfg_->Get<bool>(kAggregationSpillEnabled, true));
        configs[config::QueryConfig::kMemFraction] = std::to_string(omniCfg_->Get<int32_t>(kMemFraction, 90));
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
        configs[config::QueryConfig::KColumnarSpillEnableCompress] = BoolToString(
            omniCfg_->Get<bool>(KColumnarSpillEnableCompress, false));
        configs[config::QueryConfig::KEnableAdaptivePartialAggregation] = omniCfg_->Get<std::string>(
            KEnableAdaptivePartialAggregation, "true");
        configs[config::QueryConfig::KAdaptivePartialAggregationMinRows] = std::to_string(
            omniCfg_->Get<int32_t>(KAdaptivePartialAggregationMinRows, 500000));
        configs[config::QueryConfig::KAdaptivePartialAggregationRatio] = std::to_string(
            omniCfg_->Get<double>(KAdaptivePartialAggregationRatio, 0.8));
        configs[config::QueryConfig::KPreferVectorizationExpression] = BoolToString(
            omniCfg_->Get<bool>(KPreferVectorizationExpression, true));
        configs[config::QueryConfig::KHashAggNormalizedKeyEnabled] = BoolToString(
            omniCfg_->Get<bool>(kHashAggNormalizedKeyEnabled, false));
        configs[config::QueryConfig::KMaxBatchSize] = std::to_string(
            omniCfg_->Get<uint64_t>(kSparkBatchSize, 4096));
        configs[config::QueryConfig::kHdfsReadMode] = std::to_string(
            omniCfg_->Get<int32_t>(kHdfsReadMode, 0));
        configs[config::QueryConfig::KEnableMixedStorage] = BoolToString(
            omniCfg_->Get<bool>(KEnableMixedStorage, false));
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
    }
}

void WholeStageResultIterator::buildMetricsForNative(
    const omniruntime::compute::PlanNodeStats& stats, int& metricIndex)
{
    for (const auto& entry : stats.operatorStats) {
        const auto& second = entry.second;
        metrics_->get(omniruntime::OmniMetrics::kInputRows)[metricIndex] = second->inputRows;
        metrics_->get(omniruntime::OmniMetrics::kNumInputVecBatches)[metricIndex] = second->numInputVecBatches;
        metrics_->get(omniruntime::OmniMetrics::kInputBytes)[metricIndex] = second->inputBytes;

        metrics_->get(omniruntime::OmniMetrics::kRawInputRows)[metricIndex] = second->rawInputRows;
        metrics_->get(omniruntime::OmniMetrics::kRawInputBytes)[metricIndex] = 0;

        metrics_->get(omniruntime::OmniMetrics::kOutputRows)[metricIndex] = second->outputRows;
        metrics_->get(omniruntime::OmniMetrics::kNumOutputVecBatches)[metricIndex] = second->numOutputVecBatches;
        metrics_->get(omniruntime::OmniMetrics::kOutputBytes)[metricIndex] = second->outputBytes;

        metrics_->get(omniruntime::OmniMetrics::kSpilledBytes)[metricIndex] = second->spilledBytes;
        metrics_->get(omniruntime::OmniMetrics::kSpilledRows)[metricIndex] = second->spilledRows;
        metrics_->get(omniruntime::OmniMetrics::kSpilledPartitions)[metricIndex] = second->spilledPartitions;
        metrics_->get(omniruntime::OmniMetrics::kCpuCount)[metricIndex] = second->cpuWallTiming.count;
        metrics_->get(omniruntime::OmniMetrics::kSpilledFiles)[metricIndex] = second->spilledFiles;
        metrics_->get(omniruntime::OmniMetrics::kScanTime)[metricIndex] =
            static_cast<long>(second->totalScanWallNanos);
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
        metricIndex += 1;
    }
}
}
