//
// Created by root on 4/28/25.
//

#include "SubstraitToOmniPlan.h"
#include <expression/expressions.h>
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
/// @return the output type.
std::tuple<DataTypesPtr, DataTypesPtr> getJoinOutputType(const PlanNodePtr& leftNode,
    const PlanNodePtr& rightNode, const JoinType& joinType)
{
    // Decide output type.
    // Output of right semi join cannot include columns from the left side.
    // velox: !(core::isRightSemiFilterJoin(joinType) || core::isRightSemiProjectJoin(joinType))
    bool outputMayIncludeLeftColumns = true;

    // Output of left semi and anti joins cannot include columns from the right side.
    bool outputMayIncludeRightColumns = !(joinType == JoinType::OMNI_JOIN_TYPE_LEFT_SEMI ||
        joinType == JoinType::OMNI_JOIN_TYPE_EXISTENCE || joinType == JoinType::OMNI_JOIN_TYPE_LEFT_ANTI);

    if (outputMayIncludeLeftColumns && outputMayIncludeRightColumns) {
        return {leftNode->OutputType(), rightNode->OutputType()};
    }

    if (outputMayIncludeLeftColumns) {
        if (joinType == JoinType::OMNI_JOIN_TYPE_EXISTENCE) {
            std::vector<DataTypePtr> outputTypes = leftNode->OutputType()->Get();
            outputTypes.emplace_back(BooleanDataType::Instance());
            return {std::make_shared<DataTypes>(std::move(outputTypes)), std::make_shared<DataTypes>()};
        } else {
            return {leftNode->OutputType(), std::make_shared<DataTypes>()};
        }
    }

    if (outputMayIncludeRightColumns) {
        return {std::make_shared<DataTypes>(), rightNode->OutputType()};
    }

    OMNI_THROW("Substrait Error", "Output should include left or right columns.");
}

std::string SubstraitToOmniPlanConverter::FindFuncSpec(uint64_t id) {}

void SubstraitToOmniPlanConverter::ExtractJoinKeys(const ::substrait::Expression &joinExpression,
    std::vector<const ::substrait::Expression::FieldReference *> &leftExprs,
    std::vector<const ::substrait::Expression::FieldReference *> &rightExprs)
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
                OMNI_CHECK(std::all_of(args.cbegin(), args.cend(), [](const ::substrait::FunctionArgument& arg) {
                    return arg.value().has_selection();
                    }), "args is not all selection.");
                leftExprs.push_back(&args[0].value().selection());
                rightExprs.push_back(&args[1].value().selection());
            } else {
                OMNI_THROW("Substrait Error", "Join condition {} not supported.", funcName);
            }
        } else {
            OMNI_THROW("Substrait Error", "Unable to parse from join expression: {}", joinExpression.DebugString());
        }
    }
}

PlanNodePtr SubstraitToOmniPlanConverter::ToOmniPlan(const ::substrait::WriteRel &writeRel) {}

PlanNodePtr SubstraitToOmniPlanConverter::ToOmniPlan(const ::substrait::ExpandRel &expandRel) {}

PlanNodePtr SubstraitToOmniPlanConverter::ToOmniPlan(const ::substrait::GenerateRel &generateRel) {}

PlanNodePtr SubstraitToOmniPlanConverter::ToOmniPlan(const ::substrait::WindowRel &windowRel) {}

PlanNodePtr SubstraitToOmniPlanConverter::ToOmniPlan(const ::substrait::WindowGroupLimitRel &windowGroupLimitRel) {}

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

    // extract join keys from join expression
    std::vector<const ::substrait::Expression::FieldReference *> leftExprs;
    std::vector<const ::substrait::Expression::FieldReference *> rightExprs;
    ExtractJoinKeys(joinRel.expression(), leftExprs, rightExprs);
    OMNI_CHECK(leftExprs.size() == rightExprs.size(), "Left expr size must equal to right expr size");
    size_t numKeys = leftExprs.size();

    std::vector<std::shared_ptr<const FieldExpr>> leftKeys;
    std::vector<std::shared_ptr<const FieldExpr>> rightKeys;
    leftKeys.reserve(numKeys);
    rightKeys.reserve(numKeys);
    auto inputType = getJoinInputType(leftNode, rightNode);
    for (size_t i = 0; i < numKeys; ++i) {
        auto leftKey = dynamic_cast<const FieldExpr *>(
            exprConverter->ToOmniExpr(*leftExprs[i], leftNode->OutputType()));
        auto rightKey = dynamic_cast<const FieldExpr *>(
            exprConverter->ToOmniExpr(*rightExprs[i], rightNode->OutputType()));
        leftKeys.emplace_back(std::make_shared<const FieldExpr>(leftKey->colVal, leftKey->dataType));
        rightKeys.emplace_back(std::make_shared<const FieldExpr>(rightKey->colVal, rightKey->dataType));
    }

    TypedExprPtr filter = nullptr;
    if (joinRel.has_post_join_filter()) {
        filter = exprConverter->ToOmniExpr(joinRel.post_join_filter(), inputType);
    }

    auto [leftOutputType, rightOutputType] = getJoinOutputType(leftNode, rightNode, joinType);

    if (joinRel.has_advanced_extension() &&
        SubstraitParser::ConfigSetInOptimization(joinRel.advanced_extension(), "isSMJ=")) {
        // Create MergeJoinNode node
        return std::make_shared<MergeJoinNode>(NextPlanNodeId(), joinType, leftKeys, rightKeys,
            filter, leftNode, rightNode, leftOutputType, rightOutputType);
    } else {
        auto isBroadcast = joinRel.has_advanced_extension() &&
            SubstraitParser::ConfigSetInOptimization(joinRel.advanced_extension(), "isBHJ=");

        // Create HashJoinNode node
        return std::make_shared<HashJoinNode>(NextPlanNodeId(), joinType, isNullAwareAntiJoin, !isBroadcast,
            leftKeys, rightKeys, filter, leftNode, rightNode, leftOutputType, rightOutputType);
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
    TypedExprPtr joinConditions;
    if (crossRel.has_expression()) {
        joinConditions = exprConverter->ToOmniExpr(crossRel.expression(), inputRowType);
    }

    auto [leftOutputType, rightOutputType] = getJoinOutputType(leftNode, rightNode, joinType);

    return std::make_shared<NestedLoopJoinNode>(NextPlanNodeId(), joinType, joinConditions,
        leftNode, rightNode, leftOutputType, rightOutputType);
}

PlanNodePtr SubstraitToOmniPlanConverter::ToOmniPlan(const ::substrait::AggregateRel &aggRel) {}

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
    auto filterNode = std::make_shared<FilterNode>(
        NextPlanNodeId(), exprConverter->ToOmniExpr(filterRel.condition(), childNode->OutputType()), childNode);
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
    auto [sortingKeys, sortingOrders, sortNullFirsts] = ProcessSortField(topNRel.sorts(), childNode->OutputType());
    return std::make_shared<TopNNode>(
        NextPlanNodeId(), sortingKeys, sortingOrders, sortNullFirsts, static_cast<int32_t>(topNRel.n()), childNode);
}

PlanNodePtr SubstraitToOmniPlanConverter::ToOmniPlan(const ::substrait::ReadRel &readRel, const DataTypesPtr &type) {}

PlanNodePtr SubstraitToOmniPlanConverter::ToOmniPlan(const ::substrait::ReadRel &readRel)
{
    // Check if the ReadRel specifies an input of stream. If yes, build
    // ValueStreamNode as the data source.
    auto streamIdx = GetStreamIndex(readRel);
    if (streamIdx >= 0) {
        return ConstructValueStreamNode(readRel, streamIdx);
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
    auto [sortingKeys, sortingOrders, sortNullFirsts] = ProcessSortField(sortRel.sorts(), childNode->OutputType());
    return std::make_shared<OrderByNode>(NextPlanNodeId(), sortingKeys, sortingOrders, sortNullFirsts, childNode);
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
    } else if (rel.has_generate()) {
        return ToOmniPlan(rel.generate());
    } else if (rel.has_fetch()) {
        return ToOmniPlan(rel.fetch());
    } else if (rel.has_top_n()) {
        return ToOmniPlan(rel.top_n());
    } else if (rel.has_window()) {
        return ToOmniPlan(rel.window());
    } else if (rel.has_write()) {
        return ToOmniPlan(rel.write());
    } else if (rel.has_windowgrouplimit()) {
        return ToOmniPlan(rel.windowgrouplimit());
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
