/*
* Copyright (C) 2025-2025. Huawei Technologies Co., Ltd. All rights reserved.
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

#include "compute/ResultIterator.h"
#include "memory/memory_pool.h"
#include "substrait/SubstraitToOmniPlan.h"
#include "substrait/plan.pb.h"
#include "plannode/planNode.h"

namespace omniruntime
{
// This class is used to convert the Substrait plan into Omni plan.
class OmniPlanConverter {
public:
    explicit OmniPlanConverter(const std::vector<std::shared_ptr<ResultIterator>> &inputIters,
        mem::MemoryPool *OmniPool, const std::unordered_map<std::string, std::string> &confMap,
        const std::optional<std::string> writeFilesTempPath = std::nullopt, bool validationMode = false);

    std::shared_ptr<const PlanNode> ToOmniPlan(const ::substrait::Plan &substraitPlan,
        std::vector<::substrait::ReadRel_LocalFiles> localFiles);

    //     const std::unordered_map<omniruntime::PlanNodeId, std::shared_ptr<SplitInfo>> &splitInfos()
    //     {
    //         return substraitOmniPlanConverter_.splitInfos();
    //     }

private:
    std::string nextPlanNodeId();

    int planNodeId_ = 0;

    bool validationMode_;

    SubstraitToOmniPlanConverter substraitOmniPlanConverter_;
};
} // namespace gluten
