//
// Created by root on 4/27/25.
//

#include "WholeStageResultIterator.h"
#include "compute/reason.h"

namespace omniruntime
{
WholeStageResultIterator::WholeStageResultIterator(
    MemoryManager *memoryManager,
    const std::shared_ptr<const PlanNode> &planNode,
    const std::vector<PlanNodeId> &scanNodeIds,
    const std::vector<PlanNodeId> &streamIds,
    const std::string spillDir,
    const std::unordered_map<std::string, std::string> &confMap)
    : memoryManager_(memoryManager),
      omniPlan_(planNode),
      scanNodeIds_(scanNodeIds),
      streamIds_(streamIds)
{
    // Create task instance.
    std::unordered_set<PlanNodeId> emptySet;
    PlanFragment planFragment{planNode, ExecutionStrategy::K_UNGROUPED, 1, emptySet};
    task_ = std::make_shared<OmniTask>(planFragment, OperatorConfig());
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
}
