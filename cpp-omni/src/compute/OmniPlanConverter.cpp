//
// Created by root on 4/29/25.
//

#include "OmniPlanConverter.h"

namespace omniruntime
{
OmniPlanConverter::OmniPlanConverter(const std::vector<std::shared_ptr<ResultIterator>> &inputIters,
    mem::MemoryPool *OmniPool, const std::unordered_map<std::string, std::string> &confMap,
    const std::optional<std::string> writeFilesTempPath, bool validationMode)
    : validationMode_(validationMode),
      substraitOmniPlanConverter_(confMap, writeFilesTempPath, validationMode)
{
    substraitOmniPlanConverter_.setInputIters(std::move(inputIters));
}

std::shared_ptr<const PlanNode> OmniPlanConverter::ToOmniPlan(const ::substrait::Plan &substraitPlan,
    std::vector<::substrait::ReadRel_LocalFiles> localFiles)
{
    auto OmniPlan = substraitOmniPlanConverter_.ToOmniPlan(substraitPlan);
    return OmniPlan;
}
}
