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

#pragma once

#include <optional>
#include <memory>
#include "compute/task.h"
#include "compute/plannode_stats.h"
#include "common/debug.h"
#include "plannode/planFragment.h"
#include "operator/config/operator_config.h"
#include "memory/memory_manager.h"
#include "metrics/omni_metrics.h"
#include "compute/ColumnarBatchIterator.h"
#include "util/config/ConfigBase.h"

namespace omniruntime {
class WholeStageResultIterator : public ColumnarBatchIterator {
public:
    WholeStageResultIterator(MemoryManager *memoryManager, const std::shared_ptr<const PlanNode> &planNode,
        const std::vector<PlanNodeId> &scanNodeIds, const std::vector<PlanNodeId> &streamIds,
        const std::string& spillDir, const std::unordered_map<std::string, std::string> &confMap);

    ~WholeStageResultIterator() override = default;

    VectorBatch *Next() override;

    const OmniTask *task() const
    {
        return task_.get();
    }

    const PlanNode *veloxPlan() const
    {
        return omniPlan_.get();
    }

    omniruntime::OmniMetrics* getMetrics(int64_t exportNanos)
    {
        LogsDebug("get Metrics and exportNanos = %d", exportNanos);
        CollectMetrics();
        if (metrics_) {
            metrics_->omniToArrow = exportNanos;
        }
        return metrics_.get();
    }

private:
    /// Get the Spark confs to Velox query context.
    std::unordered_map<std::string, std::string> GetQueryContextConf(const std::string &spillDir) const;

    MemoryManager *memoryManager_;

    /// Config, task and plan.
    OperatorConfig operatorConfig;
    std::shared_ptr<OmniTask> task_;
    std::shared_ptr<const PlanNode> omniPlan_;
    std::shared_ptr<config::ConfigBase> omniCfg_;
    /// Spill.
    std::string spillStrategy_;

    /// All the children plan node ids with postorder traversal.
    std::vector<PlanNodeId> orderedNodeIds_;

    /// Node ids should be omitted in metrics.
    std::unordered_set<PlanNodeId> omittedNodeIds_;
    std::vector<PlanNodeId> scanNodeIds_;
    std::vector<PlanNodeId> streamIds_;
    bool noMoreSplits_ = false;
    std::unique_ptr<omniruntime::OmniMetrics> metrics_{};
    /// Get all the children plan node ids with postorder traversal.
    void getOrderedNodeIds(
        const std::shared_ptr<const omniruntime::PlanNode> &,
        std::vector<omniruntime::PlanNodeId> &nodeIds);

    void buildMetricsForNative(const struct omniruntime::compute::PlanNodeStats& stats,
        int metricIndex);
    /// Collect omni metrics.
    void CollectMetrics();
};
}
