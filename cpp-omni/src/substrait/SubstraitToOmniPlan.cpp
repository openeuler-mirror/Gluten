/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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

#include "SubstraitToOmniPlan.h"
#include <expression/expressions.h>
#include <google/protobuf/wrappers.pb.h>
#include <vector>
#include <stack>
#include <algorithm>
#include "config/OmniConfig.h"
#include "udf/UdfLoader.h"

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

::substrait::Type getUdafIntermediateType(const std::string &udafName, const ::substrait::Type &fallbackType)
{
    if (udafName.empty()) {
        return fallbackType;
    }
    const auto intermediateTypeBytes = gluten::UdfLoader::getRegisteredUdafIntermediateType(udafName);
    if (intermediateTypeBytes.empty()) {
        return fallbackType;
    }

    ::substrait::Type intermediateType;
    if (!intermediateType.ParseFromString(intermediateTypeBytes)) {
        OMNI_THROW("SUBSTRAIT_ERROR:", "Failed to parse intermediate type for UDAF: " + udafName);
    }
    return intermediateType;
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
            } else if (projectExpr.has_if_then()) {
                auto expression = exprConverter->ToOmniExpr(projectExpr.if_then(), inputType);
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
    std::vector<op::WindowFunctionOptions> windowFunctionOptions;

    std::vector<op::WindowFrameInfo> windowFrameInfos;

    for (const auto& smea : windowRel.measures()) {
        const auto& windowFunction = smea.measure();
        op::WindowFunctionOptions options;
        std::vector<substrait::Expression> expressionNodes;
        for (const auto& arg : windowFunction.arguments()) {
            expressionNodes.emplace_back(arg.value());
        }
        auto funcName = SubstraitParser::FindOmniFunction(functionMap, windowFunction.function_reference());
        op::FunctionType functionType = SubstraitParser::ParseFunctionType(funcName.second, expressionNodes, false);
        if (functionType == op::OMNI_WINDOW_TYPE_LEAD || functionType == op::OMNI_WINDOW_TYPE_LAG) {
            if (!windowFunction.arguments().empty()) {
                auto expression = exprConverter->ToOmniExpr(
                    windowFunction.arguments(0).value(), childNode->OutputType());
                argumentKeys.emplace_back(expression);
            }
            int64_t leadLagOffset = 1;
            if (windowFunction.arguments_size() >= 2) {
                const auto& offsetArg = windowFunction.arguments(1).value();
                if (offsetArg.has_literal()) {
                    leadLagOffset = SubstraitParser::GetLiteralValue<int64_t>(offsetArg.literal());
                }
            }
            int32_t defaultEndChannel = -1;
            if (windowFunction.arguments_size() >= 3) {
                const auto& defaultArg = windowFunction.arguments(2).value();
                auto defaultExpr = exprConverter->ToOmniExpr(defaultArg, childNode->OutputType());
                argumentKeys.emplace_back(defaultExpr);
                defaultEndChannel = -2;
            }
            for (const auto& option : windowFunction.options()) {
                if (option.name() == "ignoreNulls") {
                    options.flags |= op::WindowFunctionOptions::IGNORE_NULLS;
                    break;
                }
            }
            op::WindowFrameInfo frame(op::OMNI_FRAME_TYPE_ROWS,
                op::OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, static_cast<int32_t>(leadLagOffset),
                op::OMNI_FRAME_BOUND_UNBOUNDED_FOLLOWING, defaultEndChannel);
            windowFrameInfos.push_back(std::move(frame));
        } else if (functionType == op::OMNI_WINDOW_TYPE_NTH_VALUE) {
            if (!windowFunction.arguments().empty()) {
                auto expression = exprConverter->ToOmniExpr(
                    windowFunction.arguments(0).value(), childNode->OutputType());
                argumentKeys.emplace_back(expression);
            }
            if (windowFunction.arguments_size() >= 2) {
                const auto& offsetArg = windowFunction.arguments(1).value();
                auto offsetExpression = exprConverter->ToOmniExpr(offsetArg, childNode->OutputType());
                argumentKeys.emplace_back(offsetExpression);
                options.nthValueOffsetChannel = op::WindowFunctionOptions::PENDING_CHANNEL;
            }
            for (const auto& option : windowFunction.options()) {
                if (option.name() == "ignoreNulls") {
                    options.flags |= op::WindowFunctionOptions::IGNORE_NULLS;
                    break;
                }
            }
            auto frame = createWindowFrameInfo(windowFunction.lower_bound(), windowFunction.upper_bound(),
                windowFunction.window_type());
            windowFrameInfos.push_back(std::move(frame));
        } else if (functionType == op::OMNI_WINDOW_TYPE_NTILE) {
            int32_t numBuckets = 1;
            if (!windowFunction.arguments().empty()) {
                const auto& bucketsArg = windowFunction.arguments(0).value();
                if (bucketsArg.has_literal()) {
                    numBuckets = SubstraitParser::GetLiteralValue<int32_t>(bucketsArg.literal());
                }
            }
            op::WindowFrameInfo frame(op::OMNI_FRAME_TYPE_RANGE,
                op::OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, numBuckets,
                op::OMNI_FRAME_BOUND_UNBOUNDED_FOLLOWING, -1);
            windowFrameInfos.push_back(std::move(frame));
        } else {
            for (const auto& arg : windowFunction.arguments()) {
                auto expression = exprConverter->ToOmniExpr(arg.value(), childNode->OutputType());
                argumentKeys.emplace_back(expression);
            }
        }
        windowFunctionTypes.push_back(functionType);
        windowFunctionOptions.push_back(options);
        auto windowFunctionReturnType = SubstraitParser::ParseType(windowFunction.output_type());
        windowFunctionReturnTypesVec.push_back(windowFunctionReturnType);
        allTypesVec.push_back(windowFunctionReturnType);
        auto type = windowFunction.window_type();
        auto lowerBound = windowFunction.lower_bound();
        auto upperBound = windowFunction.upper_bound();
        if (functionType != op::OMNI_WINDOW_TYPE_LEAD && functionType != op::OMNI_WINDOW_TYPE_LAG &&
            functionType != op::OMNI_WINDOW_TYPE_NTH_VALUE && functionType != op::OMNI_WINDOW_TYPE_NTILE) {
            windowFrameInfos.push_back(std::move(createWindowFrameInfo(lowerBound, upperBound, type)));
        }
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
        auto fieldExpr = ExtractFieldExprFromPartitionOrSortKey(expression);
        OMNI_CHECK(fieldExpr != nullptr, "Partition expression must resolve to a field reference");
        partitionCols.emplace_back(fieldExpr->colVal);
    }

    std::vector<int32_t> preGroupedCols;
    int32_t preSortedChannelPreFix = 0;
    int32_t expectedPositionsCount = 10000;
    auto [sortingKeys, sortingOrders, sortNullFirsts] = ProcessSortField(windowRel.sorts(), childNode->OutputType());
    return std::make_shared<WindowNode>(NextPlanNodeId(), windowFunctionTypes, partitionCols, preGroupedCols,
        sortingKeys, sortingOrders, sortNullFirsts, preSortedChannelPreFix, expectedPositionsCount,
        windowFunctionReturnTypes, allTypes, argumentKeys, windowFrameTypes, windowFrameStartTypes,
        windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, windowFunctionOptions, childNode);
}

PlanNodePtr SubstraitToOmniPlanConverter::ToOmniPlan(const ::substrait::WindowGroupLimitRel &windowGroupLimitRel)
{
    auto childNode = ConvertSingleInput<::substrait::WindowGroupLimitRel>(windowGroupLimitRel);
    const auto& sourceDataTypes = childNode->OutputType();
    auto outputType = sourceDataTypes;

    int32_t n = windowGroupLimitRel.limit();
    if (n <= 0) {
        OMNI_THROW("Substrait Error", "WindowGroupLimitRel requires a positive N(limit)!");
    }

    std::vector<TypedExprPtr> partitionKeys;
    const auto& partitions = windowGroupLimitRel.partition_expressions();
    for (const auto& partition : partitions) {
        auto expression = exprConverter->ToOmniExpr(partition, sourceDataTypes);
        auto fieldExpr = ExtractFieldExprFromPartitionOrSortKey(expression);
        partitionKeys.emplace_back(fieldExpr != nullptr ? fieldExpr : expression);
    }

    auto [sortingKeys, sortingAscendings, sortNullFirsts] = ProcessSortFieldWithExpr(windowGroupLimitRel.sorts(), sourceDataTypes);

    std::string funcName;
    if (!windowGroupLimitRel.has_advanced_extension()) {
        OMNI_THROW("Substrait Error", "WindowGroupLimitRel requires advanced_extension !");
    }
    if (SubstraitParser::ConfigSetInOptimization(windowGroupLimitRel.advanced_extension(), "isRank=")) {
        funcName = "rank";
    } else if (SubstraitParser::ConfigSetInOptimization(windowGroupLimitRel.advanced_extension(), "isRowNumber=")) {
        funcName = "row_number";
    } else {
        OMNI_THROW("Substrait Error", "WindowGroupLimitRel requires rankLikeFunction rank or row_number!");
    }

    return std::make_shared<WindowGroupLimitNode>(NextPlanNodeId(), childNode, n, funcName, partitionKeys,
        sortingKeys, sortingAscendings, sortNullFirsts, outputType);
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
            return std::make_tuple(op::OMNI_FRAME_BOUND_FOLLOWING,
                static_cast<int32_t>(boundType.following().offset()));
        } else if (boundType.has_preceding()) {
            return std::make_tuple(op::OMNI_FRAME_BOUND_PRECEDING,
                static_cast<int32_t>(boundType.preceding().offset()));
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
    AggregationNode::Step aggStep = toAggregationStep(aggRel);

    PlanNodePtr expandPlanNode = nullptr;
    if (aggRel.has_advanced_extension() && std::dynamic_pointer_cast<const ExpandNode>(childNode) != nullptr) {
        const auto &advancedExtension = aggRel.advanced_extension();
        if (advancedExtension.has_optimization()) {
            const auto &optimization = advancedExtension.optimization();
            if (optimization.template Is<::substrait::Rel>()) {
                ::substrait::Rel expandRel;
                optimization.UnpackTo(&expandRel);
                expandPlanNode = std::dynamic_pointer_cast<const ExpandNode>(childNode);
            }
        }
    }
    const auto &sourceDataTypes = childNode->OutputType();
    std::vector<TypedExprPtr> aggFilterExprs;
    std::vector<DataTypesPtr> aggOutputTypes;
    std::vector<uint32_t> aggFuncTypes;
    std::vector<std::string> aggUdafNames;
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
                const auto udafName = SubstraitParser::ResolveUdafName(baseFuncName.second);
                auto outputType = getUdafIntermediateType(udafName, aggFunction.output_type());
                auto substraitOutTypes = SubstraitParser::ParseStructType(outputType);
                aggOutputTypes.emplace_back(substraitOutTypes);
                SubstraitParser::AddStructDataType(outputType, nodeOutputTypes);
                aggFuncTypes.emplace_back(
                    SubstraitParser::ParseFunctionType(baseFuncName.second, expressionNodes, true));
                aggUdafNames.emplace_back(udafName);
                inputRaws.emplace_back(true);
                outputPartial.emplace_back(true);
                break;
            }
            case ::substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE: { // PartialMerge
                const auto udafName = SubstraitParser::ResolveUdafName(baseFuncName.second);
                auto outputType = getUdafIntermediateType(udafName, aggFunction.output_type());
                auto substraitOutTypes = SubstraitParser::ParseStructType(outputType);
                aggOutputTypes.emplace_back(substraitOutTypes);
                SubstraitParser::AddStructDataType(outputType, nodeOutputTypes);
                aggFuncTypes.emplace_back(
                    SubstraitParser::ParseFunctionType(baseFuncName.second, expressionNodes, false));
                aggUdafNames.emplace_back(udafName);
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
                aggUdafNames.emplace_back(SubstraitParser::ResolveUdafName(baseFuncName.second));
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
                aggUdafNames.emplace_back(SubstraitParser::ResolveUdafName(baseFuncName.second));
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
        isStatisticalAggregate, outputType, childNode, aggStep, aggUdafNames);
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
    auto splitInfo = std::make_shared<SplitInfo>();
    if (!validationMode) {
        splitInfo = splitInfos_[splitInfoIdx_++];
    }
    std::vector<std::string> colNameList;
    std::vector<DataTypePtr> omniTypeList;
    std::vector<ColumnType> columnTypes;
    bool asLowerCase = false;
    if (readRel.has_base_schema()) {
        const auto& baseSchema = readRel.base_schema();
        colNameList.reserve(baseSchema.names().size());
        for (const auto& name : baseSchema.names()) {
            std::string fieldName = name;
            if (asLowerCase) {
                // folly::toLowerAscii(fieldName);
            }
            colNameList.emplace_back(fieldName);
        }
        omniTypeList = SubstraitParser::ParseNamedStruct(baseSchema, asLowerCase);
        SubstraitParser::ParseColumnTypes(baseSchema, columnTypes);
    }
    static const std::string K_HIVE_CONNECTOR_ID = "test-hive";
    static const std::string TABLE_NAME = "hive_table";
    bool filterPushdownEnabled = true;
    std::shared_ptr<HiveTableHandle> tableHandle;
    if (!readRel.has_advanced_extension() || !readRel.advanced_extension().has_enhancement()) {
        tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
            kHiveConnectorId(), TABLE_NAME, filterPushdownEnabled, "");
    } else {
        auto names = colNameList;
        auto types = omniTypeList;
        google::protobuf::StringValue msg;
        readRel.advanced_extension().enhancement().UnpackTo(&msg);
        tableHandle = std::make_shared<HiveTableHandle>(
            kHiveConnectorId(), TABLE_NAME, filterPushdownEnabled, msg.value());
    }

    std::vector<std::string> outNames;
    outNames.reserve(colNameList.size());
    std::unordered_map<std::string, std::shared_ptr<omniruntime::connector::ColumnHandle>> assignments;
    for (int idx = 0; idx < colNameList.size(); idx++) {
        auto outName = omniruntime::SubstraitParser::MakeNodeName(planNodeId, idx);
        assignments[outName] = std::make_shared<omniruntime::connector::hive::HiveColumnHandle>(
            colNameList[idx],
            columnTypes[idx],
            omniTypeList[idx],
            omniTypeList[idx],
            std::vector<omniruntime::type::Subfield>{},
            ColumnParseParameters{ColumnParseParameters::kISO8601});
        outNames.emplace_back(outName);
    }
    if (readRel.has_virtual_table()) {
        OMNI_THROW("readRel virtual error", "readRel virtual error");
    } else {
        auto tableScanNode = std::make_shared<TableScanNode>(
            NextPlanNodeId(), omniTypeList, outNames, std::move(tableHandle), std::move(assignments));
        splitInfoMap_[tableScanNode->Id()] = splitInfo;
        return tableScanNode;
    }
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
            auto fieldExpr = ExtractFieldExprFromPartitionOrSortKey(expression);
            sortExpressions.emplace_back(fieldExpr != nullptr ? fieldExpr : expression);
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
        auto fieldExpr = ExtractFieldExprFromPartitionOrSortKey(expression);
        OMNI_CHECK(fieldExpr != nullptr, "Sort expression must resolve to a field reference");
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
        auto fieldExpr = ExtractFieldExprFromPartitionOrSortKey(expression);
        sortingKeys.emplace_back(fieldExpr != nullptr ? fieldExpr : expression);
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

AggregationNode::Step SubstraitToOmniPlanConverter::toAggregationStep(const ::substrait::AggregateRel& aggRel) {
    if (aggRel.has_advanced_extension() &&
        SubstraitParser::ConfigSetInOptimization(aggRel.advanced_extension(), "allowFlush=")) {
        return AggregationNode::Step::K_PARTIAL;
    }
    return AggregationNode::Step::K_SINGLE;
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
    } else if (rel.has_windowgrouplimit()) {
        return ToOmniPlan(rel.windowgrouplimit());
    } else if (rel.has_write()) {
        return ToOmniPlan(rel.write());
    } else if (rel.has_set()) {
        return ToOmniPlan(rel.set());
    } else if (rel.has_generate()) {
        return ToOmniPlan(rel.generate());
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

void extractUnnestFieldExpr(
    std::shared_ptr<const PlanNode> child,
    int32_t index,
    std::vector<Expr*>& unnestFields)
{
    auto type = child->OutputType()->GetType(index);
    auto unnestFieldExpr = new FieldExpr(index, type);
    unnestFields.emplace_back(unnestFieldExpr);
}

PlanNodePtr SubstraitToOmniPlanConverter::ToOmniPlan(const ::substrait::GenerateRel &generateRel)
{
    PlanNodePtr childNode;
    if (generateRel.has_input()) {
        childNode = ToOmniPlan(generateRel.input());
    } else {
        OMNI_THROW("SUBSTRAIT_ERROR:", "Child Rel is expected in GenerateRel.");
    }
    const auto& inputType = childNode->OutputType();
  
    std::vector<Expr*> replicated;
    std::vector<Expr*> unnest;
  
    const auto& generator = generateRel.generator();
    const auto& requiredChildOutput = generateRel.child_output();
  
    replicated.reserve(requiredChildOutput.size());
    for (const auto& output : requiredChildOutput) {
        auto expression = exprConverter->ToOmniExpr(output, inputType);
        OMNI_CHECK(expression != nullptr, " the output in Generate Operator only support field");
  
        replicated.emplace_back(expression);
    }

    auto injectedProject = generateRel.has_advanced_extension() &&
        SubstraitParser::ConfigSetInOptimization(generateRel.advanced_extension(), "injectedProject=");
    if (injectedProject) {
        // Child should be either ProjectNode or ValueStreamNode in case of project fallback.
        OMNI_CHECK(
            (std::dynamic_pointer_cast<const ProjectNode>(childNode) != nullptr ||
            std::dynamic_pointer_cast<const ValueStreamNode>(childNode) != nullptr) &&
            childNode->OutputType()->GetSize() > requiredChildOutput.size(),
            "injectedProject is true, but the ProjectNode or ValueStreamNode (in case of projection fallback)"
            " is missing or does not have the corresponding projection field");
  
        bool isStack = generateRel.has_advanced_extension() &&
            SubstraitParser::ConfigSetInOptimization(generateRel.advanced_extension(), "isStack=");
        // Generator function's input is NOT a field reference.
        if (!isStack) {
            // For generator function which is not stack, e.g. explode(array(1,2,3)), a sample
            // input substrait plan is like the following:
            //
            //  Generate explode([1,2,3] AS _pre_0#129), false, [col#126]
            //  +- Project [fake_column#128, [1,2,3] AS _pre_0#129]
            //   +- RewrittenNodeWall Scan OneRowRelation[fake_column#128]
            // The last projection column in GeneratorRel's child(Project) is the column we need to unnest
            auto index = childNode->OutputType()->GetSize() - 1;
            extractUnnestFieldExpr(childNode, index, unnest);
        } else {
            // For stack function, e.g. stack(2, 1,2,3), a sample
            // input substrait plan is like the following:
            //
            // Generate stack(2, id#122, name#123, id1#124, name1#125), false, [col0#137, col1#138]
            // +- Project [id#122, name#123, id1#124, name1#125, array(id#122, id1#124) AS _pre_0#141, array(name#123,
            // name1#125) AS _pre_1#142]
            //   +- RewrittenNodeWall LocalTableScan [id#122, name#123, id1#124, name1#125]
            //
            // The last `numFields` projections are the fields we want to unnest.
            auto generatorFunc = generator.scalar_function();
            auto numRows = SubstraitParser::GetLiteralValue<int32_t>(generatorFunc.arguments(0).value().literal());
            if (numRows == 0) {
                OMNI_THROW("SUBSTRAIT_ERROR:",
                          "Division by zero error prevented: numRows cannot be 0 in stack function.");
            }
            auto numFields = static_cast<int32_t>(std::ceil((generatorFunc.arguments_size() - 1.0) / numRows));
            auto totalProjectCount = childNode->OutputType()->GetSize();
  
            for (auto i = totalProjectCount - numFields; i < totalProjectCount; ++i) {
                extractUnnestFieldExpr(childNode, i, unnest);
            }
        }
    } else {
        // Generator function's input is a field reference, e.g. explode(col), generator
        // function's first argument is the field reference we need to unnest.
        // This assumption holds for all the supported generator function:
        // explode, posexplode, inline.
        auto generatorFunc = generator.scalar_function();
        auto unnestExpr = exprConverter->ToOmniExpr(generatorFunc.arguments(0).value(), inputType);
        OMNI_CHECK(unnestExpr != nullptr, " the key in unnest Operator only support field");
        unnest.emplace_back(unnestExpr);
    }

    bool withOrdinality = false;
    if (generateRel.has_advanced_extension() &&
        SubstraitParser::ConfigSetInOptimization(generateRel.advanced_extension(), "isPosExplode=")) {
        withOrdinality = true;
    }

    // Read outer field from GenerateRel
    bool outer = generateRel.outer();

    return std::make_shared<UnnestNode>(
        NextPlanNodeId(), replicated, unnest, childNode, withOrdinality, outer);
}
} // namespace omniruntime
