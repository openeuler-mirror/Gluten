/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: print expression tree methods
 */

#pragma once

#include <optional>
#include <string>
#include <tuple>
#include "SubstraitParser.h"
#include "SubstraitToOmniExpr.h"
#include "compute/ResultIterator.h"
#include "plannode/RowVectorStream.h"
#include "plannode/planNode.h"
#include "operator/window/window_frame.h"

namespace omniruntime {
/// This class is used to convert the Substrait plan into Omni plan.
class SubstraitToOmniPlanConverter {
public:
    SubstraitToOmniPlanConverter(const std::unordered_map<std::string, std::string> &confMap = {},
        const std::optional<std::string> writeFilesTempPath = std::nullopt, bool validationMode = false)
        : confMap(confMap), writeFilesTempPath(writeFilesTempPath), validationMode(validationMode)
    {}

    /// Used to convert Substrait WriteRel into Omni PlanNode.
    PlanNodePtr ToOmniPlan(const ::substrait::WriteRel &writeRel);

    /// Used to convert Substrait ExpandRel into Omni PlanNode.
    PlanNodePtr ToOmniPlan(const ::substrait::ExpandRel &expandRel);

    /// Used to convert Substrait GenerateRel into Omni PlanNode.
    PlanNodePtr ToOmniPlan(const ::substrait::GenerateRel &generateRel);

    /// Used to convert Substrait WindowRel into Omni PlanNode.
    PlanNodePtr ToOmniPlan(const ::substrait::WindowRel &windowRel);

    /// Used to convert Substrait WindowGroupLimitRel into Omni PlanNode.
    PlanNodePtr ToOmniPlan(const ::substrait::WindowGroupLimitRel &windowGroupLimitRel);

    /// Used to convert Substrait SetRel into Omni PlanNode.
    PlanNodePtr ToOmniPlan(const ::substrait::SetRel &setRel);

    /// Used to convert Substrait JoinRel into Omni PlanNode.
    PlanNodePtr ToOmniPlan(const ::substrait::JoinRel &joinRel);

    /// Used to convert Substrait CrossRel into Omni PlanNode.
    PlanNodePtr ToOmniPlan(const ::substrait::CrossRel &crossRel);

    /// Used to convert Substrait AggregateRel into Omni PlanNode.
    PlanNodePtr ToOmniPlan(const ::substrait::AggregateRel &aggRel);

    /// Convert Substrait ProjectRel into Omni PlanNode.
    PlanNodePtr ToOmniPlan(const ::substrait::ProjectRel &projectRel);

    /// Convert Substrait FilterRel into Omni PlanNode.
    PlanNodePtr ToOmniPlan(const ::substrait::FilterRel &filterRel);

    /// Convert Substrait FetchRel into Omni LimitNode.
    PlanNodePtr ToOmniPlan(const ::substrait::FetchRel &fetchRel);

    /// Convert Substrait TopNRel into Omni TopNNode.
    PlanNodePtr ToOmniPlan(const ::substrait::TopNRel &topNRel);

    /// Convert Substrait ReadRel into Omni Values Node.
    PlanNodePtr ToOmniPlan(const ::substrait::ReadRel &readRel, const DataTypesPtr &type);

    /// Convert Substrait SortRel into Omni OrderByNode.
    PlanNodePtr ToOmniPlan(const ::substrait::SortRel &sortRel);

    /// Convert Substrait ReadRel into Omni PlanNode.
    /// Index: the index of the partition this item belongs to.
    /// Starts: the start positions in byte to read from the items.
    /// Lengths: the lengths in byte to read from the items.
    /// FileProperties: the file sizes and modification times of the files to be scanned.
    PlanNodePtr ToOmniPlan(const ::substrait::ReadRel &sRead);

    PlanNodePtr ConstructValueStreamNode(const ::substrait::ReadRel &sRead, int32_t streamIdx);

    /// Used to convert Substrait Rel into Omni PlanNode.
    PlanNodePtr ToOmniPlan(const ::substrait::Rel &sRel);

    /// Used to convert Substrait RelRoot into Omni PlanNode.
    PlanNodePtr ToOmniPlan(const ::substrait::RelRoot &sRoot);

    /// Used to convert Substrait Plan into Omni PlanNode.
    PlanNodePtr ToOmniPlan(const ::substrait::Plan &substraitPlan);

    // return the raw ptr of ExprConverter
    SubstraitOmniExprConverter *GetExprConverter() { return this->exprConverter.get(); }

    /// Used to construct the function map between the index
    /// and the Substrait function name. Initialize the expression
    /// converter based on the constructed function map.
    void ConstructFunctionMap(const ::substrait::Plan &substraitPlan);

    /// Will return the function map used by this plan converter.
    const std::unordered_map<uint64_t, std::string> &GetFunctionMap() const { return this->functionMap; }

    /// Used to insert certain plan node as input. The plan node
    /// id will start from the setted one.
    void InsertInputNode(uint64_t inputIdx, const std::shared_ptr<const PlanNode> &inputNode, int planNodeId)
    {
        this->inputNodesMap[inputIdx] = inputNode;
        this->planNodeId = planNodeId;
    }

    void setInputIters(std::vector<std::shared_ptr<ResultIterator>> inputIters)
    {
        this->inputIters = std::move(inputIters);
    }

    /// Used to check if ReadRel specifies an input of stream.
    /// If yes, the index of input stream will be returned.
    /// If not, -1 will be returned.
    int32_t GetStreamIndex(const ::substrait::ReadRel &sRel);

    /// Used to find the function specification in the constructed function map.
    std::string FindFuncSpec(uint64_t id);

    /// Extract join keys from joinExpression.
    /// joinExpression is a boolean condition that describes whether each record
    /// from the left set “match” the record from the right set. The condition
    /// must only include the following operations: AND, ==, field references.
    /// Field references correspond to the direct output order of the data.
    void ExtractJoinKeys(const ::substrait::Expression &joinExpression,
        std::vector<const ::substrait::Expression::FieldReference *> &leftExprs,
        std::vector<const ::substrait::Expression::FieldReference *> &rightExprs);

    // /// Get aggregation step from AggregateRel.
    // /// If returned Partial, it means the aggregate generated can leveraging flushing and abandoning like
    // /// what streaming pre-aggregation can do in MPP databases.
    // AggregationNode::Step toAggregationStep(const ::substrait::AggregateRel& sAgg);

    /// Get aggregation function step for AggregateFunction.
    /// The returned step value will be used to decide which Omni aggregate function or companion function
    /// is used for the actual data processing.
    static AggregationNode::Step ToAggregationFunctionStep(const ::substrait::AggregateFunction &sAggFuc);

    //
    // /// We use companion functions if the aggregate is not single.
    // std::string toAggregationFunctionName(const std::string& baseName, const AggregationNode::Step& step);

    /// Helper Function to convert Substrait sortField to Omni sortingKeys and
    /// sortingOrders.
    /// Note that, this method would deduplicate the sorting keys which have the same field name.
    std::tuple<std::vector<int32_t>, std::vector<int32_t>, std::vector<int32_t>> ProcessSortField(
        const ::google::protobuf::RepeatedPtrField<::substrait::SortField> &sortField, const DataTypesPtr &inputType);

private:
    /// Integrate Substrait emit feature. Here a given 'substrait::RelCommon'
    /// is passed and check if emit is defined for this relation. Basically a
    /// ProjectNode is added on top of 'noEmitNode' to represent output order
    /// specified in 'relCommon::emit'. Return 'noEmitNode' as is
    /// if output order is 'kDriect'.
    PlanNodePtr ProcessEmit(const ::substrait::RelCommon &relCommon, const PlanNodePtr &noEmitNode);

    /// Check the Substrait type extension only has one unknown extension.
    static bool CheckTypeExtension(const ::substrait::Plan &substraitPlan);

    /// Returns unique ID to use for plan node. Produces sequential numbers
    /// starting from zero.
    std::string NextPlanNodeId();

    // /// Used to convert AggregateRel into Omni plan node.
    // /// The output of child node will be used as the input of Aggregation.
    // std::shared_ptr<const PlanNode> toOmniAgg(
    //     const ::substrait::AggregateRel& sAgg,
    //     const std::shared_ptr<const PlanNode>& childNode,
    //     const AggregationNode::Step& aggStep);

    /// Helper function to convert the input of Substrait Rel to Omni Node.
    template <typename T>
    PlanNodePtr ConvertSingleInput(T rel)
    {
        OMNI_CHECK(rel.has_input(), "Child Rel is expected here.");
        return ToOmniPlan(rel.input());
    }

    const WindowFrameInfo createWindowFrameInfo(
        const ::substrait::Expression_WindowFunction_Bound& lower_bound,
        const ::substrait::Expression_WindowFunction_Bound& upper_bound,
        const ::substrait::WindowType& type);

    /// The unique identification for each PlanNode.
    int planNodeId = 0;

    /// The map storing the relations between the function id and the function
    /// name. Will be constructed based on the Substrait representation.
    std::unordered_map<uint64_t, std::string> functionMap;

    // /// The map storing the split stats for each PlanNode.
    // std::unordered_map<PlanNodeId, std::shared_ptr<SplitInfo>> splitInfoMap_;
    //
    // std::function<PlanNodePtr(std::string, memory::MemoryPool*, int32_t, RowTypePtr)> valueStreamNodeFactory_;
    //
    std::vector<std::shared_ptr<ResultIterator>> inputIters;

    /// The map storing the pre-built plan nodes which can be accessed through
    /// index. This map is only used when the computation of a Substrait plan
    /// depends on other input nodes.
    std::unordered_map<uint64_t, std::shared_ptr<const PlanNode>> inputNodesMap;

    int32_t splitInfoIdx_{0};
    // std::vector<std::shared_ptr<SplitInfo>> splitInfos_;

    /// The Expression converter used to convert Substrait representations into
    /// Omni expressions.
    std::unique_ptr<SubstraitOmniExprConverter> exprConverter;
    //
    // /// Memory pool.
    // memory::MemoryPool* pool_;

    /// A map of custom configs.
    std::unordered_map<std::string, std::string> confMap;

    /// The temporary path used to write files.
    std::optional<std::string> writeFilesTempPath;

    /// A flag used to specify validation.
    bool validationMode = false;
};
} // namespace omniruntime
