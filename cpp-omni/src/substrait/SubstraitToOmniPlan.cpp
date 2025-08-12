//
// Created by root on 4/28/25.
//

#include "SubstraitToOmniPlan.h"
#include <expression/expressions.h>
#include <google/protobuf/wrappers.pb.h>
#include <vector>
#include <stack>
#include <algorithm>

namespace omniruntime {
namespace {
struct EmitInfo {
    std::vector<TypedExprPtr> expressions;
};

EmitInfo getEmitInfo(const ::substrait::RelCommon &relCommon, const PlanNodePtr &node)
{
    const auto &emit = relCommon.emit();
    int emitSize = emit.output_mapping_size();
    EmitInfo emitInfo;
    emitInfo.expressions.reserve(emitSize);
    const auto &outputType = node->OutputType();
    for (int i = 0; i < emitSize; i++) {
        int32_t mapId = emit.output_mapping(i);
        emitInfo.expressions[i] = new FieldExpr(i, outputType->GetType(i));
    }
    return emitInfo;
}
} // namespace
SortOrderInfo ToSortOrder(const ::substrait::SortField &sortField)
{
    switch (sortField.direction()) {
        case ::substrait::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_FIRST:
            return K_ASC_NULLS_FIRST;
        case ::substrait::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_LAST:
            return K_ASC_NULLS_LAST;
        case ::substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_FIRST:
            return K_DESC_NULLS_FIRST;
        case ::substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_LAST:
            return K_DESC_NULLS_LAST;
        default:
            OMNI_THROW("PARSE_ERROR", "Sort direction is not supported.");
    }
}

/// @brief Get the input type from both sides of join.
/// @param leftNode the plan node of left side.
/// @param rightNode the plan node of right side.
/// @return the input type.
DataTypesPtr getJoinInputType(const PlanNodePtr& leftNode, const PlanNodePtr& rightNode)
{
    auto outputSize = leftNode->OutputType()->GetSize() + rightNode->OutputType()->GetSize();
    std::vector<DataTypePtr> joinInputTypes;
    joinInputTypes.reserve(outputSize);

    joinInputTypes.insert(
        joinInputTypes.end(), leftNode->OutputType()->Get().begin(), leftNode->OutputType()->Get().end());
    joinInputTypes.insert(
        joinInputTypes.end(), rightNode->OutputType()->Get().begin(), rightNode->OutputType()->Get().end());

    return std::make_shared<DataTypes>(std::move(joinInputTypes));
}

/// @brief Get the direct output type of join.
/// @param leftNode the plan node of left side.
/// @param rightNode the plan node of right side.
/// @param joinType the join type.
/// @param buildSide the build side.
/// @return the output type.
std::tuple<DataTypesPtr, DataTypesPtr> getJoinOutputType(const PlanNodePtr& leftNode,
    const PlanNodePtr& rightNode)
{
    // Decide output type.
    return {leftNode->OutputType(), rightNode->OutputType()};
}

std::string SubstraitToOmniPlanConverter::FindFuncSpec(uint64_t id)
{
    return SubstraitParser::FindFunctionSpec(functionMap, id);
}

void SubstraitToOmniPlanConverter::ExtractJoinKeys(const ::substrait::Expression &joinExpression,
    std::vector<const ::substrait::Expression *> &leftExprs,
    std::vector<const ::substrait::Expression *> &rightExprs)
{
    std::stack<const ::substrait::Expression *> expressions;
    expressions.push(&joinExpression);
    while (!expressions.empty()) {
        auto visited = expressions.top();
        expressions.pop();
        if (visited->rex_type_case() == ::substrait::Expression::RexTypeCase::kScalarFunction) {
            auto findFunctionResult = SubstraitParser::FindOmniFunction(
                functionMap, visited->scalar_function().function_reference());
            const auto &funcName = SubstraitParser::GetNameBeforeDelimiter(findFunctionResult.second);
            const auto &args = visited->scalar_function().arguments();
            if (funcName == "AND") {
                expressions.push(&args[1].value());
                expressions.push(&args[0].value());
            } else if (funcName == "EQUAL") {
                leftExprs.push_back(&args[0].value());
                rightExprs.push_back(&args[1].value());
            } else {
                OMNI_THROW("Substrait Error", "Join condition {} not supported.", funcName);
            }
        } else {
            OMNI_THROW("Substrait Error", "Unable to parse from join expression: {}", joinExpression.DebugString());
        }
    }
}

PlanNodePtr SubstraitToOmniPlanConverter::ToOmniPlan(const ::substrait::WriteRel &writeRel)
{
    return nullptr;
}

PlanNodePtr SubstraitToOmniPlanConverter::ToOmniPlan(const ::substrait::ExpandRel &expandRel)
{
    PlanNodePtr childNode;
    if (expandRel.has_input()) {
        childNode = ToOmniPlan(expandRel.input());
    } else {
        OMNI_THROW("Substrait error:", "Child Rel is expected in ExpandRel.");
    }

    const auto& inputType = childNode->OutputType();

    std::vector<std::vector<TypedExprPtr>> projectSetExprs;
    projectSetExprs.reserve(expandRel.fields_size());

    for (const auto& projections : expandRel.fields()) {
        std::vector<TypedExprPtr> projectExprs;
        projectExprs.reserve(projections.switching_field().duplicates_size());

        for (const auto& projectExpr : projections.switching_field().duplicates()) {
            if (projectExpr.has_selection()) {
                auto expression = exprConverter->ToOmniExpr(projectExpr.selection(), inputType);
                projectExprs.emplace_back(expression);
            } else if (projectExpr.has_literal()) {
                auto expression = exprConverter->ToOmniExpr(projectExpr.literal());
                projectExprs.emplace_back(expression);
            } else if (projectExpr.has_scalar_function()) {
                auto expression = exprConverter->ToOmniExpr(projectExpr.scalar_function(), inputType);
                projectExprs.emplace_back(expression);
            } else {
                OMNI_THROW("Substrait error:", "The project in Expand Operator only support field or literal.");
            }
        }
        projectSetExprs.emplace_back(projectExprs);
    }

    return std::make_shared<ExpandNode>(NextPlanNodeId(), std::move(projectSetExprs), childNode);
}

PlanNodePtr SubstraitToOmniPlanConverter::ToOmniPlan(const ::substrait::WindowRel &windowRel)
{
    auto childNode = ConvertSingleInput<::substrait::WindowRel>(windowRel);
    std::vector<int32_t> windowFunctionTypes;
    std::vector<DataTypePtr> windowFunctionReturnTypesVec;
    std::vector<DataTypePtr> allTypesVec;
    auto sourceTypesVec = childNode->OutputType()->Get();
    allTypesVec.insert(allTypesVec.end(), sourceTypesVec.begin(), sourceTypesVec.end());
    std::vector<TypedExprPtr> argumentKeys;

    std::vector<int32_t> windowFrameTypes;
    std::vector<int32_t> windowFrameStartTypes;
    std::vector<int32_t> windowFrameStartChannels;
    std::vector<int32_t> windowFrameEndTypes;
    std::vector<int32_t> windowFrameEndChannels;

    std::vector<op::WindowFrameInfo> windowFrameInfos;

    for (const auto& smea : windowRel.measures()) {
        const auto& windowFunction = smea.measure();
        std::vector<substrait::Expression> expressionNodes;
        for (const auto& arg : windowFunction.arguments()) {
            expressionNodes.emplace_back(arg.value());
            auto expression = exprConverter->ToOmniExpr(arg.value(), childNode->OutputType());
            argumentKeys.emplace_back(expression);
        }
        auto funcName = SubstraitParser::FindOmniFunction(functionMap, windowFunction.function_reference());
        op::FunctionType functionType = SubstraitParser::ParseFunctionType(funcName.second, expressionNodes, false);
        windowFunctionTypes.push_back(functionType);
        auto windowFunctionReturnType = SubstraitParser::ParseType(windowFunction.output_type());
        windowFunctionReturnTypesVec.push_back(windowFunctionReturnType);
        allTypesVec.push_back(windowFunctionReturnType);
        auto type = windowFunction.window_type();
        auto lowerBound = windowFunction.lower_bound();
        auto upperBound = windowFunction.upper_bound();
        windowFrameInfos.push_back(std::move(createWindowFrameInfo(lowerBound, upperBound, type)));
    }
    for (auto& windowFrameInfo : windowFrameInfos) {
        windowFrameTypes.push_back(windowFrameInfo.GetType());
        windowFrameStartTypes.push_back(windowFrameInfo.GetStartType());
        windowFrameStartChannels.push_back(windowFrameInfo.GetStartChannel());
        windowFrameEndTypes.push_back(windowFrameInfo.GetEndType());
        windowFrameEndChannels.push_back(windowFrameInfo.GetEndChannel());
    }
    auto windowFunctionReturnTypes = std::make_shared<DataTypes>(windowFunctionReturnTypesVec);
    auto allTypes = std::make_shared<DataTypes>(allTypesVec);
    std::vector<int32_t> partitionCols;
    const auto& partitions = windowRel.partition_expressions();
    for (const auto& partition : partitions) {
        auto expression = exprConverter->ToOmniExpr(partition, childNode->OutputType());
        auto fieldExpr = dynamic_cast<const FieldExpr *>(expression);
        partitionCols.emplace_back(fieldExpr->colVal);
    }

    std::vector<int32_t> preGroupedCols;
    int32_t preSortedChannelPreFix = 0;
    int32_t expectedPositionsCount = 10000;
    auto [sortingKeys, sortingOrders, sortNullFirsts] = ProcessSortField(windowRel.sorts(), childNode->OutputType());
    return std::make_shared<WindowNode>(NextPlanNodeId(), windowFunctionTypes, partitionCols, preGroupedCols,
        sortingKeys, sortingOrders, sortNullFirsts, preSortedChannelPreFix, expectedPositionsCount,
        windowFunctionReturnTypes, allTypes, argumentKeys, windowFrameTypes, windowFrameStartTypes,
        windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, childNode);
}

const WindowFrameInfo SubstraitToOmniPlanConverter::createWindowFrameInfo(
    const ::substrait::Expression_WindowFunction_Bound& lower_bound,
    const ::substrait::Expression_WindowFunction_Bound& upper_bound,
    const ::substrait::WindowType& type)
{
    op::FrameType frameType;
    op::FrameBoundType frameStartType;
    int32_t frameStartCol;
    op::FrameBoundType frameEndType;
    int32_t frameEndCol;
    switch (type) {
        case ::substrait::WindowType::ROWS:
            frameType = op::OMNI_FRAME_TYPE_ROWS;
            break;
        case ::substrait::WindowType::RANGE:
            frameType = op::OMNI_FRAME_TYPE_RANGE;
            break;
        default:
            OMNI_THROW("Substrait Error", "Unsupported WindowRel WindowType: " + std::to_string(type));
    }
    auto boundTypeConversion = [ ](::substrait::Expression_WindowFunction_Bound boundType)
        -> std::tuple<op::FrameBoundType, int32_t> {
        if (boundType.has_current_row()) {
            return std::make_tuple(op::OMNI_FRAME_BOUND_CURRENT_ROW, -1);
        } else if (boundType.has_unbounded_following()) {
            return std::make_tuple(op::OMNI_FRAME_BOUND_UNBOUNDED_FOLLOWING, -1);
        } else if (boundType.has_unbounded_preceding()) {
            return std::make_tuple(op::OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, -1);
        } else if (boundType.has_following()) {
            OMNI_THROW("Substrait Error", "The BoundType is not supported: Bound Type: N FOLLOWING");
        } else if (boundType.has_preceding()) {
            OMNI_THROW("Substrait Error", "The BoundType is not supported: Bound Type: N PRECEDING");
        } else {
            OMNI_THROW("Substrait Error", "Unknown or unset bound type.");
        }
    };
    std::tie(frameStartType, frameStartCol) = boundTypeConversion(lower_bound);
    std::tie(frameEndType, frameEndCol) = boundTypeConversion(upper_bound);
    op::WindowFrameInfo frame(frameType, frameStartType, frameStartCol, frameEndType, frameEndCol);
    return frame;
}

PlanNodePtr SubstraitToOmniPlanConverter::ToOmniPlan(const ::substrait::SetRel &setRel)
{
    std::vector<PlanNodePtr> childNodeList;
    for (int i = 0; i < setRel.inputs_size(); i++) {
        const ::substrait::Rel &input = setRel.inputs(i);
        childNodeList.push_back(ToOmniPlan(input));
    }
    switch (setRel.op()) {
        case ::substrait::SetRel_SetOp::SetRel_SetOp_SET_OP_UNION_ALL: {
            return std::make_shared<UnionNode>(NextPlanNodeId(), childNodeList, false);
        }
        default:
            OMNI_THROW("Substrait Error", "Unsupported SetRel op: " + std::to_string(setRel.op()));
    }
}

PlanNodePtr SubstraitToOmniPlanConverter::ToOmniPlan(const ::substrait::JoinRel &joinRel)
{
    if (!joinRel.has_left()) {
        OMNI_THROW("Substrait Error", "Left Rel is expected in JoinRel.");
    }
    if (!joinRel.has_right()) {
        OMNI_THROW("Substrait Error", "Right Rel is expected in JoinRel.");
    }

    auto leftNode = ToOmniPlan(joinRel.left());
    auto rightNode = ToOmniPlan(joinRel.right());

    // Map join type.
    omniruntime::JoinType joinType;
    bool isNullAwareAntiJoin = false;
    switch (joinRel.type()) {
        case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_INNER:
            joinType = omniruntime::JoinType::OMNI_JOIN_TYPE_INNER;
            break;
        case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_OUTER:
            joinType = omniruntime::JoinType::OMNI_JOIN_TYPE_FULL;
            break;
        case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT:
            joinType = omniruntime::JoinType::OMNI_JOIN_TYPE_LEFT;
            break;
        case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT:
            joinType = omniruntime::JoinType::OMNI_JOIN_TYPE_RIGHT;
            break;
        case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT_SEMI:
            // Determine the semi join type based on extracted information.
            if (joinRel.has_advanced_extension() &&
                SubstraitParser::ConfigSetInOptimization(joinRel.advanced_extension(), "isExistenceJoin=")) {
                joinType = omniruntime::JoinType::OMNI_JOIN_TYPE_EXISTENCE;
            } else {
                joinType = omniruntime::JoinType::OMNI_JOIN_TYPE_LEFT_SEMI;
            }
            break;
        case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT_ANTI:
            // Determine the anti join type based on extracted information.
            if (joinRel.has_advanced_extension() &&
                SubstraitParser::ConfigSetInOptimization(joinRel.advanced_extension(), "isNullAwareAntiJoin=")) {
                isNullAwareAntiJoin = true;
            }
            joinType = omniruntime::JoinType::OMNI_JOIN_TYPE_LEFT_ANTI;
            break;
        default:
            OMNI_THROW("Substrait Error", "Unsupported Join type: {}", std::to_string(joinRel.type()));
    }

    // Map build side
    omniruntime::op::BuildSide buildSide = omniruntime::op::BuildSide::OMNI_BUILD_UNKNOWN;
    if (joinRel.has_advanced_extension() &&
        SubstraitParser::ConfigExistInOptimization(joinRel.advanced_extension(), "isBuildLeft=")) {
        if (SubstraitParser::ConfigSetInOptimization(joinRel.advanced_extension(), "isBuildLeft=")) {
            buildSide = omniruntime::op::BuildSide::OMNI_BUILD_LEFT;
        } else {
            buildSide = omniruntime::op::BuildSide::OMNI_BUILD_RIGHT;
        }
    }

    // extract join keys from join expression
    std::vector<const ::substrait::Expression *> leftExprs;
    std::vector<const ::substrait::Expression *> rightExprs;
    ExtractJoinKeys(joinRel.expression(), leftExprs, rightExprs);
    OMNI_CHECK(leftExprs.size() == rightExprs.size(), "Left expr size must equal to right expr size");
    size_t numKeys = leftExprs.size();

    std::vector<TypedExprPtr> leftKeys;
    std::vector<TypedExprPtr> rightKeys;
    leftKeys.reserve(numKeys);
    rightKeys.reserve(numKeys);
    auto inputType = getJoinInputType(leftNode, rightNode);
    for (size_t i = 0; i < numKeys; ++i) {
        auto leftKey = exprConverter->ToOmniExpr(*leftExprs[i], leftNode->OutputType());
        auto rightKey = exprConverter->ToOmniExpr(*rightExprs[i], rightNode->OutputType());
        leftKeys.emplace_back(leftKey);
        rightKeys.emplace_back(rightKey);
    }

    TypedExprPtr filter = nullptr;
    if (joinRel.has_post_join_filter()) {
        filter = exprConverter->ToOmniExpr(joinRel.post_join_filter(), inputType);
    }

    auto [leftOutputType, rightOutputType] = getJoinOutputType(leftNode, rightNode);

    uint32_t idx = 0;
    std::shared_ptr<DataTypes> firstType;
    std::shared_ptr<DataTypes> secondType;
    auto exchangeTable = buildSide == omniruntime::op::BuildSide::OMNI_BUILD_LEFT;
    if (exchangeTable) {
        firstType = rightNode->OutputType();
        secondType = leftNode->OutputType();
    } else {
        firstType = leftNode->OutputType();
        secondType = rightNode->OutputType();
    }

    auto vector1 = firstType->Get();
    auto vector2 = secondType->Get();
    vector1.insert(vector1.end(), vector2.begin(), vector2.end());
    auto ptr = std::make_shared<DataTypes>(vector1);
    std::vector<omniruntime::TypedExprPtr> keys = ProcessExtensionProjectNode(joinRel.advanced_extension(), ptr);

    if (joinRel.has_advanced_extension() &&
        SubstraitParser::ConfigSetInOptimization(joinRel.advanced_extension(), "isSMJ=")) {
        // Create MergeJoinNode node
        return std::make_shared<MergeJoinNode>(NextPlanNodeId(), joinType, omniruntime::op::BuildSide::OMNI_BUILD_RIGHT, leftKeys, rightKeys,
            filter, leftNode, rightNode, leftOutputType, rightOutputType, keys);
    } else {
        auto isBroadcast = joinRel.has_advanced_extension() &&
            SubstraitParser::ConfigSetInOptimization(joinRel.advanced_extension(), "isBHJ=");

        // Create HashJoinNode node
        // FIX ME param isShuffle is not used, please delete.
        return std::make_shared<HashJoinNode>(NextPlanNodeId(), joinType, buildSide, isNullAwareAntiJoin, false,
            leftKeys, rightKeys, filter, leftNode, rightNode, leftOutputType, rightOutputType, keys);
    }
}

PlanNodePtr SubstraitToOmniPlanConverter::ToOmniPlan(const ::substrait::CrossRel &crossRel)
{
    if (!crossRel.has_left()) {
        OMNI_THROW("Substrait Error", "Left Rel is expected in CrossRel.");
    }
    if (!crossRel.has_right()) {
        OMNI_THROW("Substrait Error", "Right Rel is expected in CrossRel.");
    }

    auto leftNode = ToOmniPlan(crossRel.left());
    auto rightNode = ToOmniPlan(crossRel.right());

    // Map join type.
    omniruntime::JoinType joinType;
    switch (crossRel.type()) {
        case ::substrait::CrossRel_JoinType::CrossRel_JoinType_JOIN_TYPE_INNER:
            joinType = omniruntime::JoinType::OMNI_JOIN_TYPE_INNER;
            break;
        case ::substrait::CrossRel_JoinType::CrossRel_JoinType_JOIN_TYPE_LEFT:
            joinType = omniruntime::JoinType::OMNI_JOIN_TYPE_LEFT;
            break;
        case ::substrait::CrossRel_JoinType::CrossRel_JoinType_JOIN_TYPE_RIGHT:
            joinType = omniruntime::JoinType::OMNI_JOIN_TYPE_RIGHT;
            break;
        default:
            OMNI_THROW("Substrait Error", "Unsupported Join type: {}", std::to_string(crossRel.type()));
    }

    auto inputRowType = getJoinInputType(leftNode, rightNode);
    TypedExprPtr joinConditions = nullptr;
    if (crossRel.has_expression()) {
        joinConditions = exprConverter->ToOmniExpr(crossRel.expression(), inputRowType);
    }

    auto [leftOutputType, rightOutputType] = getJoinOutputType(leftNode, rightNode);

    return std::make_shared<NestedLoopJoinNode>(NextPlanNodeId(), joinType, joinConditions,
        leftNode, rightNode, leftOutputType, rightOutputType);
}

std::vector<uint32_t> getDefaultMaskChannel(const std::vector<uint32_t>& aggFuncTypes)
{
    if (aggFuncTypes.empty()) {
        return {};
    }
    return std::vector<uint32_t>(aggFuncTypes.size(), static_cast<uint32_t>(-1));
}

PlanNodePtr SubstraitToOmniPlanConverter::ToOmniPlan(const ::substrait::AggregateRel &aggRel)
{
    auto childNode = ConvertSingleInput<::substrait::AggregateRel>(aggRel);
    PlanNodePtr expandPlanNode = nullptr;
    if (aggRel.has_advanced_extension()) {
        const auto &advancedExtension = aggRel.advanced_extension();
        if (advancedExtension.has_optimization()) {
            const auto &optimization = advancedExtension.optimization();
            ::substrait::Rel expandRel;
            optimization.UnpackTo(&expandRel);
            expandPlanNode = ToOmniPlan(expandRel);
        }
    }
    const auto &sourceDataTypes = childNode->OutputType();
    std::vector<TypedExprPtr> aggFilterExprs;
    std::vector<DataTypesPtr> aggOutputTypes;
    std::vector<uint32_t> aggFuncTypes;
    std::vector<uint32_t> maskColumns;
    std::vector<bool> inputRaws;
    std::vector<bool> outputPartial;
    std::vector<TypedExprPtr> groupingExprs;
    std::vector<DataTypePtr> nodeOutputTypes;
    DataTypesPtr outputType;
    uint32_t groupByNum = 0;

    for (const auto &grouping : aggRel.groupings()) {
        for (const auto &groupingExpr : grouping.grouping_expressions()) {
            auto omniGroupingExpr = exprConverter->ToOmniExpr(groupingExpr, sourceDataTypes);
            groupingExprs.emplace_back(omniGroupingExpr);
            nodeOutputTypes.emplace_back(omniGroupingExpr->GetReturnType());
            groupByNum++;
        }
    }

    for (const auto &measure : aggRel.measures()) {
        ::substrait::Expression substraitFilter = measure.filter();
        if (measure.has_filter()) {
            if (substraitFilter.ByteSizeLong() > 0) {
                auto omniFilter = exprConverter->ToOmniExpr(substraitFilter, sourceDataTypes);
                aggFilterExprs.emplace_back(omniFilter);
            }
        } else {
            aggFilterExprs.emplace_back(nullptr);
        }

        const auto &aggFunction = measure.measure();
        auto baseFuncName = SubstraitParser::FindOmniFunction(functionMap, aggFunction.function_reference());
        std::vector<substrait::Expression> expressionNodes;
        for (const auto &arg : aggFunction.arguments()) {
            auto argValue = arg.value();
            expressionNodes.emplace_back(argValue);
        }
        const auto &mode = aggFunction.phase();

        switch (mode) {
            case ::substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE: { // Partial
                auto substraitOutTypes = SubstraitParser::ParseStructType(aggFunction.output_type());
                aggOutputTypes.emplace_back(substraitOutTypes);
                SubstraitParser::AddStructDataType(aggFunction.output_type(), nodeOutputTypes);
                aggFuncTypes.emplace_back(
                    SubstraitParser::ParseFunctionType(baseFuncName.second, expressionNodes, true));
                inputRaws.emplace_back(true);
                outputPartial.emplace_back(true);
                break;
            }
            case ::substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE: { // PartialMerge
                auto substraitOutTypes = SubstraitParser::ParseStructType(aggFunction.output_type());
                aggOutputTypes.emplace_back(substraitOutTypes);
                SubstraitParser::AddStructDataType(aggFunction.output_type(), nodeOutputTypes);
                aggFuncTypes.emplace_back(
                    SubstraitParser::ParseFunctionType(baseFuncName.second, expressionNodes, false));
                inputRaws.emplace_back(false);
                outputPartial.emplace_back(true);
                break;
            }
            case ::substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT: { // Complete
                auto substraitOutType = SubstraitParser::ParseType(aggFunction.output_type());
                std::vector<DataTypePtr> dataTypes = {substraitOutType};
                nodeOutputTypes.emplace_back(substraitOutType);
                auto dataTypesPtr = std::make_shared<DataTypes>(std::move(dataTypes));
                aggOutputTypes.emplace_back(dataTypesPtr);
                aggFuncTypes.emplace_back(
                    SubstraitParser::ParseFunctionType(baseFuncName.second, expressionNodes, true));
                inputRaws.emplace_back(true);
                outputPartial.emplace_back(false);
                break;
            }
            case ::substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT: { // Final
                auto substraitOutType = SubstraitParser::ParseType(aggFunction.output_type());
                std::vector<DataTypePtr> dataTypes = {substraitOutType};
                nodeOutputTypes.emplace_back(substraitOutType);
                auto dataTypesPtr = std::make_shared<DataTypes>(std::move(dataTypes));
                aggOutputTypes.emplace_back(dataTypesPtr);
                aggFuncTypes.emplace_back(
                    SubstraitParser::ParseFunctionType(baseFuncName.second, expressionNodes, false));
                inputRaws.emplace_back(false);
                outputPartial.emplace_back(false);
                break;
            }
            default:
                OMNI_THROW("SUBSTRAIT_ERROR:", "Unexpected aggregation phase.");
        }
    }

    std::vector<std::vector<TypedExprPtr>> aggsKeys;
    aggsKeys.resize(aggRel.measures().size());
    int aggFunIndex = 0;
    for (const auto &measure : aggRel.measures()) {
        const auto &aggFunction = measure.measure();
        for (const auto &arg : aggFunction.arguments()) {
            auto argValue = arg.value();
            auto tempExpr = exprConverter->ToOmniExpr(argValue, sourceDataTypes);
            aggsKeys[aggFunIndex].emplace_back(tempExpr);
        }
        aggFunIndex++;
    }

    bool isStatisticalAggregate = false;
    maskColumns = getDefaultMaskChannel(aggFuncTypes);
    std::vector<DataTypes> outPutDataTypes;
    for (const auto &outputType : aggOutputTypes) {
        outPutDataTypes.emplace_back(*outputType);
    }

    outputType = std::make_shared<DataTypes>(std::move(nodeOutputTypes));
    auto aggregationNode = std::make_shared<AggregationNode>(NextPlanNodeId(), groupingExprs, groupByNum, aggsKeys,
        sourceDataTypes, outPutDataTypes, aggFuncTypes, aggFilterExprs, maskColumns, inputRaws, outputPartial,
        isStatisticalAggregate, outputType, childNode);
    if (expandPlanNode) {
        if (auto expandNode = std::dynamic_pointer_cast<const ExpandNode>(expandPlanNode)) {
            return std::make_shared<GroupingNode>(NextPlanNodeId(), expandNode, aggregationNode);
        }
        OMNI_THROW("RUNTIME_ERROR:", "Not support expandNode!");
    }
    return aggregationNode;
}

PlanNodePtr SubstraitToOmniPlanConverter::ToOmniPlan(const ::substrait::ProjectRel &projectRel)
{
    auto childNode = ConvertSingleInput<::substrait::ProjectRel>(projectRel);
    const auto &projectExprs = projectRel.expressions();
    std::vector<TypedExprPtr> expressions;
    expressions.reserve(projectExprs.size());
    const auto &inputType = childNode->OutputType();
    //  Noted that Substrait projection adds the project expressions on top of the
    //  input to the projection node. Thus we need to add the input columns first
    //  and then add the projection expressions.
    //
    //  First, adding the project names and expressions from the input to the project node
    for (uint32_t idx = 0; idx < inputType->GetSize(); idx++) {
        expressions.emplace_back(new FieldExpr(idx, inputType->GetType(idx)));
    }

    // Then, adding project expression related project names and expressions.
    for (const auto &expr : projectExprs) {
        expressions.emplace_back(exprConverter->ToOmniExpr(expr, inputType));
    }

    if (projectRel.has_common()) {
        auto relCommon = projectRel.common();
        const auto &emit = relCommon.emit();
        int emitSize = emit.output_mapping_size();
        std::vector<TypedExprPtr> emitExpressions(emitSize);
        for (int i = 0; i < emitSize; i++) {
            int32_t mapId = emit.output_mapping(i);
            emitExpressions[i] = expressions[mapId];
        }
        return std::make_shared<ProjectNode>(NextPlanNodeId(), std::move(emitExpressions), std::move(childNode));
    } else {
        return std::make_shared<ProjectNode>(NextPlanNodeId(), std::move(expressions), std::move(childNode));
    }
}

PlanNodePtr SubstraitToOmniPlanConverter::ToOmniPlan(const ::substrait::FilterRel &filterRel)
{
    auto childNode = ConvertSingleInput<::substrait::FilterRel>(filterRel);
    auto ptr = childNode->OutputType();
    std::vector<omniruntime::TypedExprPtr> keys = ProcessExtensionProjectNode(filterRel.advanced_extension(), ptr);
    auto filterNode = std::make_shared<FilterNode>(
        NextPlanNodeId(), exprConverter->ToOmniExpr(filterRel.condition(), childNode->OutputType()), childNode, keys);
    if (filterRel.has_common()) {
        return ProcessEmit(filterRel.common(), std::move(filterNode));
    } else {
        return filterNode;
    }
}

PlanNodePtr SubstraitToOmniPlanConverter::ToOmniPlan(const ::substrait::FetchRel &fetchRel)
{
    auto childNode = ConvertSingleInput<::substrait::FetchRel>(fetchRel);
    return std::make_shared<LimitNode>(NextPlanNodeId(), static_cast<int32_t>(fetchRel.offset()),
        static_cast<int32_t>(fetchRel.count()), false, childNode);
}

PlanNodePtr SubstraitToOmniPlanConverter::ToOmniPlan(const ::substrait::TopNRel &topNRel)
{
    auto childNode = ConvertSingleInput<::substrait::TopNRel>(topNRel);
    auto [sortingKeys, sortingOrders, sortNullFirsts] =
        ProcessSortFieldWithExpr(topNRel.sorts(), childNode->OutputType());
    auto partitionKeys = ProcessExtensionProjectNode(topNRel.advanced_extension(), childNode->OutputType());
    if (topNRel.has_advanced_extension() &&
        SubstraitParser::ConfigSetInOptimization(topNRel.advanced_extension(), "isTopNSort=")) {
        // Create TopNSort node
        bool isStrictTopN = false;
        if (SubstraitParser::ConfigSetInOptimization(topNRel.advanced_extension(), "isStrictTopN=")) {
            isStrictTopN = true;
        }
        return std::make_shared<TopNSortNode>(
            NextPlanNodeId(), partitionKeys, sortingKeys, sortingOrders,
            sortNullFirsts, static_cast<int32_t>(topNRel.n()), isStrictTopN, childNode);
    } else {
        return std::make_shared<TopNNode>(
            NextPlanNodeId(), sortingKeys, sortingOrders, sortNullFirsts, static_cast<int32_t>(topNRel.n()), childNode);
    }
}

PlanNodePtr SubstraitToOmniPlanConverter::ToOmniPlan(const ::substrait::ReadRel &readRel, const DataTypesPtr &type)
{
    return nullptr;
}

PlanNodePtr SubstraitToOmniPlanConverter::ToOmniPlan(const ::substrait::ReadRel &readRel)
{
    // Check if the ReadRel specifies an input of stream. If yes, build
    // ValueStreamNode as the data source.
    auto streamIdx = GetStreamIndex(readRel);
    if (streamIdx >= 0) {
        return ConstructValueStreamNode(readRel, streamIdx);
    }
    return nullptr;
}

PlanNodePtr SubstraitToOmniPlanConverter::ConstructValueStreamNode(
    const ::substrait::ReadRel &readRel, int32_t streamIdx)
{
    // Get the input schema of this iterator.
    uint64_t colNum = 0;
    std::vector<type::DataTypePtr> veloxTypeList;
    if (readRel.has_base_schema()) {
        const auto &baseSchema = readRel.base_schema();
        // Input names is not used. Instead, new input/output names will be created
        // because the ValueStreamNode in Velox does not support name change.
        colNum = baseSchema.names().size();
        veloxTypeList = SubstraitParser::ParseNamedStruct(baseSchema);
    }

    auto outputType = std::make_shared<DataTypes>(veloxTypeList);
    std::shared_ptr<ResultIterator> iterator;
    if (!validationMode) {
        OMNI_CHECK(streamIdx <= inputIters.size(), "Could not find stream index {} in input iterator list.");
        iterator = inputIters[streamIdx];
    }
    auto node = std::make_shared<ValueStreamNode>(NextPlanNodeId(), outputType, std::move(iterator));
    return node;
}

PlanNodePtr SubstraitToOmniPlanConverter::ToOmniPlan(const ::substrait::SortRel &sortRel)
{
    auto childNode = ConvertSingleInput<::substrait::SortRel>(sortRel);
    std::vector<TypedExprPtr> sortExpressions;
    const auto &sorts = sortRel.sorts();
    for (const auto &sort : sorts) {
        if (sort.has_expr()) {
            auto expression = exprConverter->ToOmniExpr(sort.expr(), childNode->OutputType());
            sortExpressions.emplace_back(expression);
        }
    }
    auto [_, sortingOrders, sortNullFirsts] = ProcessSortFieldWithExpr(sortRel.sorts(), childNode->OutputType());
    std::vector<int32_t> sortingKeys;
    return std::make_shared<OrderByNode>(
        NextPlanNodeId(), sortingKeys, sortingOrders, sortNullFirsts, childNode, sortExpressions);
}

int32_t SubstraitToOmniPlanConverter::GetStreamIndex(const ::substrait::ReadRel &sRead)
{
    if (sRead.has_local_files()) {
        const auto &fileList = sRead.local_files().items();
        if (fileList.size() == 0) {
            // bucketed scan may contains empty file list
            return -1;
        }
        // The stream input will be specified with the format of
        // "iterator:${index}".
        std::string filePath = fileList[0].uri_file();
        std::string prefix = "iterator:";
        std::size_t pos = filePath.find(prefix);
        if (pos == std::string::npos) {
            return -1;
        }

        // Get the index.
        std::string idxStr = filePath.substr(pos + prefix.size(), filePath.size());
        try {
            return stoi(idxStr);
        } catch (const std::exception &err) {
            OMNI_THROW("error", err.what());
        }
    }
    return -1;
}

std::tuple<std::vector<int32_t>, std::vector<int32_t>, std::vector<int32_t>>
SubstraitToOmniPlanConverter::ProcessSortField(
    const ::google::protobuf::RepeatedPtrField<::substrait::SortField> &sortFields, const DataTypesPtr &inputType)
{
    std::vector<int32_t> sortingKeys;
    std::vector<int32_t> sortingOrders;
    std::vector<int32_t> sortNullFirsts;
    for (const auto &sort : sortFields) {
        OMNI_CHECK(sort.has_expr(), "Sort field must have expr");
        auto expression = exprConverter->ToOmniExpr(sort.expr(), inputType);
        auto fieldExpr = dynamic_cast<const FieldExpr *>(expression);
        // OMNI_CHECK(fieldExpr==nullptr, "Sort Operator only supports field sorting
        // key");
        sortingKeys.emplace_back(fieldExpr->colVal);
        auto sortOrder = ToSortOrder(sort);
        sortingOrders.emplace_back(sortOrder.IsAscending());
        sortNullFirsts.emplace_back(sortOrder.IsNullsFirst());
    }
    return {sortingKeys, sortingOrders, sortNullFirsts};
}

SortWithExprTuple SubstraitToOmniPlanConverter::ProcessSortFieldWithExpr(
    const ::google::protobuf::RepeatedPtrField<::substrait::SortField> &sortFields, const DataTypesPtr &inputType)
{
    std::vector<TypedExprPtr> sortingKeys;
    std::vector<int32_t> sortingOrders;
    std::vector<int32_t> sortNullFirsts;
    for (const auto &sort : sortFields) {
        OMNI_CHECK(sort.has_expr(), "Sort field must have expr");
        auto expression = exprConverter->ToOmniExpr(sort.expr(), inputType);
        sortingKeys.emplace_back(expression);
        auto sortOrder = ToSortOrder(sort);
        sortingOrders.emplace_back(sortOrder.IsAscending());
        sortNullFirsts.emplace_back(sortOrder.IsNullsFirst());
    }

    return {sortingKeys, sortingOrders, sortNullFirsts};
}

std::vector<TypedExprPtr> SubstraitToOmniPlanConverter::ProcessExtensionProjectNode(
    const ::substrait::extensions::AdvancedExtension &extension, const DataTypesPtr &inputType)
{
    std::vector<TypedExprPtr> partitionKeys;
    ::substrait::Rel rel;
    if (extension.has_enhancement()) {
        const auto &enhancement = extension.enhancement();
        enhancement.UnpackTo(&rel);
    }

    if (rel.has_project()) {
        auto projectRel = rel.project();
        const auto &exprs = projectRel.expressions();
        for (const auto& expr : exprs) {
            auto expression = exprConverter->ToOmniExpr(expr, inputType);
            partitionKeys.emplace_back(expression);
        }
    }

    return partitionKeys;
}

PlanNodePtr SubstraitToOmniPlanConverter::ProcessEmit(
    const ::substrait::RelCommon &relCommon, const PlanNodePtr &noEmitNode)
{
    switch (relCommon.emit_kind_case()) {
        case ::substrait::RelCommon::EmitKindCase::kDirect:
            return noEmitNode;
        case ::substrait::RelCommon::EmitKindCase::kEmit: {
            auto emitInfo = getEmitInfo(relCommon, noEmitNode);
            return std::make_shared<ProjectNode>(NextPlanNodeId(), std::move(emitInfo.expressions), noEmitNode);
        }
        default:
            OMNI_THROW("Substrait error:", "unrecognized emit kind");
    }
}

AggregationNode::Step SubstraitToOmniPlanConverter::ToAggregationFunctionStep(
    const ::substrait::AggregateFunction &sAggFuc)
{
    const auto &phase = sAggFuc.phase();
    switch (phase) {
        case ::substrait::AGGREGATION_PHASE_UNSPECIFIED: {
            OMNI_THROW("RUNTIME_ERROR:", "Aggregation phase not specified.");
            break;
        }
        case ::substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE:
            return AggregationNode::Step::K_PARTIAL;
        case ::substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE:
            return AggregationNode::Step::K_INTERMEDIATE;
        case ::substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT:
            return AggregationNode::Step::K_SINGLE;
        case ::substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT:
            return AggregationNode::Step::K_FINAL;
        default:
            OMNI_THROW("RUNTIME_ERROR:", "Unexpected aggregation phase.");
    }
}

PlanNodePtr SubstraitToOmniPlanConverter::ToOmniPlan(const ::substrait::Rel &rel)
{
    if (rel.has_aggregate()) {
        return ToOmniPlan(rel.aggregate());
    } else if (rel.has_project()) {
        return ToOmniPlan(rel.project());
    } else if (rel.has_filter()) {
        return ToOmniPlan(rel.filter());
    } else if (rel.has_join()) {
        return ToOmniPlan(rel.join());
    } else if (rel.has_cross()) {
        return ToOmniPlan(rel.cross());
    } else if (rel.has_read()) {
        return ToOmniPlan(rel.read());
    } else if (rel.has_sort()) {
        return ToOmniPlan(rel.sort());
    } else if (rel.has_expand()) {
        return ToOmniPlan(rel.expand());
    } else if (rel.has_fetch()) {
        return ToOmniPlan(rel.fetch());
    } else if (rel.has_top_n()) {
        return ToOmniPlan(rel.top_n());
    } else if (rel.has_window()) {
        return ToOmniPlan(rel.window());
    } else if (rel.has_write()) {
        return ToOmniPlan(rel.write());
    } else if (rel.has_set()) {
        return ToOmniPlan(rel.set());
    } else {
        OMNI_THROW("error", "Substrait conversion not supported for Rel.");
    }
}

PlanNodePtr SubstraitToOmniPlanConverter::ToOmniPlan(const ::substrait::RelRoot &root)
{
    // TODO: Use the names as the output names for the whole computing.
    // const auto& names = root.names();
    if (root.has_input()) {
        const auto &rel = root.input();
        return ToOmniPlan(rel);
    } else {
        OMNI_THROW("Su", "Input is expected in RelRoot.");
    }
}

PlanNodePtr SubstraitToOmniPlanConverter::ToOmniPlan(const ::substrait::Plan &substraitPlan)
{
    // OMNI_CHECK(checkTypeExtension(substraitPlan), "The type extension only have
    // unknown type."); Construct the function map based on the Substrait
    // representation, and initialize the expression converter with it.
    ConstructFunctionMap(substraitPlan);

    // In fact, only one RelRoot or Rel is expected here.
    const auto &rel = substraitPlan.relations(0);
    if (rel.has_root()) {
        return ToOmniPlan(rel.root());
    } else if (rel.has_rel()) {
        return ToOmniPlan(rel.rel());
    } else {
        OMNI_THROW("Substrait error:", "RelRoot or Rel is expected in Plan.");
    }
}

void SubstraitToOmniPlanConverter::ConstructFunctionMap(const ::substrait::Plan &substraitPlan)
{
    // Construct the function map based on the Substrait representation.
    for (const auto &extension : substraitPlan.extensions()) {
        if (!extension.has_extension_function()) {
            continue;
        }
        const auto &sFmap = extension.extension_function();
        auto id = sFmap.function_anchor();
        auto name = sFmap.name();
        functionMap[id] = name;
    }
    exprConverter = std::make_unique<SubstraitOmniExprConverter>(functionMap);
}

std::string SubstraitToOmniPlanConverter::NextPlanNodeId()
{
    auto id = Format("{}", planNodeId);
    planNodeId++;
    return id;
}
} // namespace omniruntime
