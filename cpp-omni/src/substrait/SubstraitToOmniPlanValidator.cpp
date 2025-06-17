/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: print expression tree methods
 */

#include "SubstraitToOmniPlanValidator.h"
#include <google/protobuf/wrappers.pb.h>
#include <re2/re2.h>
#include <string>
#include "expression/expr_verifier.h"
#include "expression/expr_printer.h"
#include "expression/expressions.h"

namespace omniruntime {
namespace {
const char *ExtractFileName(const char *file) { return strrchr(file, '/') ? strrchr(file, '/') + 1 : file; }

std::unique_ptr<re2::RE2> CompilePattern(const std::string &pattern)
{
    return std::make_unique<re2::RE2>(re2::StringPiece(pattern), RE2::Quiet);
}

// Note: This method is mostly copied from
// velox/functions/sparksql/RegexFunctions.cpp Blocks patterns that contain
// character class union, intersection, or difference because these are not
// understood by RE2 and will be parsed as a different pattern than in
// java.util.regex.
bool EnsureRegexIsCompatible(const std::string &pattern, std::string &error)
{
    // If in a character class, points to the [ at the beginning of that class.
    auto charClassStart = pattern.cend();
    // This minimal regex parser looks just for the class begin/end markers.
    for (auto c = pattern.cbegin(); c < pattern.cend(); ++c) {
        if (*c == '\\') {
            ++c;
        } else if (*c == '[') {
            if (charClassStart != pattern.cend()) {
                error = "Not support character class union, intersection, "
                        "or difference ([a[b]], [a&&[b]], [a&&[^b]])";
                return false;
            }
            charClassStart = c;
            // A ] immediately after a [ does not end the character class, and is
            // instead adds the character ].
        } else if (*c == ']' && charClassStart != pattern.cend() && charClassStart + 1 != c) {
            charClassStart = pattern.cend();
        }
    }
    return true;
}

bool ValidatePattern(const std::string &pattern, std::string &error)
{
    auto re2 = CompilePattern(pattern);
    if (!re2->ok()) {
        error = "Pattern " + pattern + " compilation failed in RE2. Reason: " + re2->error();
        return false;
    }
    return EnsureRegexIsCompatible(pattern, error);
}

#define LOG_VALIDATION_MSG_FROM_EXCEPTION(err)                                                                         \
    LogValidateMsg(Format("Validation failed due to exception caught at "                                              \
                          "file:{} line:{} function:{},"                                                               \
                          "thrown from file:{} line:{} function:{}, reason:{}",                                        \
        ExtractFileName(__FILE__), __LINE__, __FUNCTION__, "err.file()", "err.line()", "err.function()",               \
        err.message()))

#define LOG_VALIDATION_MSG(reason)                                                                                     \
    LogValidateMsg(Format("Validation failed at file:{}, line:{}, function:{}, reason:{}", ExtractFileName(__FILE__),  \
        __LINE__, __FUNCTION__, reason))

const std::unordered_set<std::string> kRegexFunctions = {
    "regexp_extract", "regexp_extract_all", "regexp_replace", "rlike"};

const std::unordered_set<std::string> kBlackList = {"split_part", "factorial", "from_json", "json_array_length",
    "trunc", "sequence", "approx_percentile", "get_array_struct_fields", "map_from_arrays"};
} // namespace

bool SubstraitToOmniPlanValidator::ParseOmniType(
    const ::substrait::extensions::AdvancedExtension &extension, DataTypePtr &out)
{
    ::substrait::Type substraitType;
    // The input type is wrapped in enhancement.
    if (!extension.has_enhancement()) {
        LOG_VALIDATION_MSG("Input type is not wrapped in enhancement.");
        return false;
    }
    const auto &enhancement = extension.enhancement();
    if (!enhancement.UnpackTo(&substraitType)) {
        LOG_VALIDATION_MSG("Enhancement can't be unpacked to inputType.");
        return false;
    }

    out = SubstraitParser::ParseType(substraitType);
    return true;
}

bool SubstraitToOmniPlanValidator::FlattenSingleLevel(const DataTypePtr &type, std::vector<DataTypePtr> &out)
{
    if (type->GetId() != OMNI_ROW) {
        LOG_VALIDATION_MSG("Type is not a RowType.");
        return false;
    }
    auto rowType = std::dynamic_pointer_cast<const RowType>(type);
    if (!rowType) {
        LOG_VALIDATION_MSG("Failed to cast to RowType.");
        return false;
    }
    for (auto &field : rowType->Children()) {
        out.emplace_back(field);
    }
    return true;
}

bool SubstraitToOmniPlanValidator::ValidateRound(
    const ::substrait::Expression::ScalarFunction &scalarFunction, const DataTypesPtr &inputType)
{
    const auto &arguments = scalarFunction.arguments();
    if (arguments.size() < 2) {
        return false;
    }

    if (!arguments[1].value().has_literal()) {
        LOG_VALIDATION_MSG("Round scale is expected.");
        return false;
    }

    // Omni has different result with Spark on negative scale.
    auto typeCase = arguments[1].value().literal().literal_type_case();
    switch (typeCase) {
        case ::substrait::Expression_Literal::LiteralTypeCase::kI32:
            return (arguments[1].value().literal().i32() >= 0);
        case ::substrait::Expression_Literal::LiteralTypeCase::kI64:
            return (arguments[1].value().literal().i64() >= 0);
        default:
            LOG_VALIDATION_MSG("Round scale validation is not supported for type case " + std::to_string(typeCase));
            return false;
    }
}

bool SubstraitToOmniPlanValidator::ValidateExtractExpr(const std::vector<TypedExprPtr> &params)
{
    if (params.size() != 2) {
        LOG_VALIDATION_MSG("Value expected in variant in ExtractExpr.");
        return false;
    }

    auto functionArg = dynamic_cast<const LiteralExpr *>(params[0]);
    if (functionArg) {
        return true;
    }
    LOG_VALIDATION_MSG("Constant is expected to be the first parameter in extract.");
    return false;
}

bool SubstraitToOmniPlanValidator::ValidateRegexExpr(
    const std::string &name, const ::substrait::Expression::ScalarFunction &scalarFunction)
{
    if (scalarFunction.arguments().size() < 2) {
        LOG_VALIDATION_MSG("Wrong number of arguments for " + name);
    }

    const auto &patternArg = scalarFunction.arguments()[1].value();
    if (!patternArg.has_literal() || !patternArg.literal().has_string()) {
        LOG_VALIDATION_MSG("Pattern is not string literal for " + name);
        return false;
    }

    const auto &pattern = patternArg.literal().string();
    std::string error;
    if (!ValidatePattern(pattern, error)) {
        LOG_VALIDATION_MSG(name + " due to " + error);
        return false;
    }
    return true;
}

bool SubstraitToOmniPlanValidator::ValidateScalarFunction(
    const ::substrait::Expression::ScalarFunction &scalarFunction, const DataTypesPtr &inputType)
{
    std::vector<TypedExprPtr> params;
    params.reserve(scalarFunction.arguments().size());
    for (const auto &argument : scalarFunction.arguments()) {
        if (argument.has_value() && !ValidateExpression(argument.value(), inputType)) {
            return false;
        }
    }
    return true;
}

bool SubstraitToOmniPlanValidator::ValidateLiteral(
    const ::substrait::Expression_Literal &literal, const DataTypesPtr &inputType)
{
    if (literal.has_list()) {
        for (auto child : literal.list().values()) {
            if (!ValidateLiteral(child, inputType)) {
                // the error msg has been set, so do not need to set it again.
                return false;
            }
        }
    } else if (literal.has_map()) {
        for (auto child : literal.map().key_values()) {
            if (!ValidateLiteral(child.key(), inputType) || !ValidateLiteral(child.value(), inputType)) {
                // the error msg has been set, so do not need to set it again.
                return false;
            }
        }
    }
    return true;
}

bool SubstraitToOmniPlanValidator::ValidateCast(
    const ::substrait::Expression::Cast &castExpr, const DataTypesPtr &inputType)
{
    if (!ValidateExpression(castExpr.input(), inputType)) {
        return false;
    }
    return true;
}

bool SubstraitToOmniPlanValidator::ValidateIfThen(
    const ::substrait::Expression_IfThen &ifThen, const DataTypesPtr &inputType)
{
    for (const auto &subIfThen : ifThen.ifs()) {
        if (!ValidateExpression(subIfThen.if_(), inputType) || !ValidateExpression(subIfThen.then(), inputType)) {
            return false;
        }
    }
    if (ifThen.has_else_() && !ValidateExpression(ifThen.else_(), inputType)) {
        return false;
    }
    return true;
}

bool SubstraitToOmniPlanValidator::ValidateSingularOrList(
    const ::substrait::Expression::SingularOrList &singularOrList, const DataTypesPtr &inputType)
{
    for (const auto &option : singularOrList.options()) {
        if (!option.has_literal()) {
            LOG_VALIDATION_MSG("Option is expected as Literal.");
            return false;
        }
        if (!ValidateLiteral(option.literal(), inputType)) {
            return false;
        }
    }

    return ValidateExpression(singularOrList.value(), inputType);
}

bool SubstraitToOmniPlanValidator::ValidateExpression(
    const ::substrait::Expression &expression, const DataTypesPtr &inputType)
{
    auto typeCase = expression.rex_type_case();
    switch (typeCase) {
        case ::substrait::Expression::RexTypeCase::kScalarFunction:
            return ValidateScalarFunction(expression.scalar_function(), inputType);
        case ::substrait::Expression::RexTypeCase::kLiteral:
            return ValidateLiteral(expression.literal(), inputType);
        case ::substrait::Expression::RexTypeCase::kCast:
            return ValidateCast(expression.cast(), inputType);
        case ::substrait::Expression::RexTypeCase::kIfThen:
            return ValidateIfThen(expression.if_then(), inputType);
        case ::substrait::Expression::RexTypeCase::kSingularOrList:
            return ValidateSingularOrList(expression.singular_or_list(), inputType);
        default:
            return true;
    }
}

bool SubstraitToOmniPlanValidator::Validate(const ::substrait::WriteRel &writeRel)
{
    if (writeRel.has_input() && !Validate(writeRel.input())) {
        LOG_VALIDATION_MSG("Validation failed for input type validation in WriteRel.");
        return false;
    }

    // Validate input data type.
    DataTypePtr inputRowType;
    std::vector<DataTypePtr> types;
    if (writeRel.has_named_table()) {
        const auto &extension = writeRel.named_table().advanced_extension();
        if (!ParseOmniType(extension, inputRowType) || !FlattenSingleLevel(inputRowType, types)) {
            LOG_VALIDATION_MSG("Validation failed for input type validation in WriteRel.");
            return false;
        }
    }
    return true;
}

bool SubstraitToOmniPlanValidator::Validate(const ::substrait::FetchRel &fetchRel)
{
    // Get and Validate the input types from extension.
    if (fetchRel.has_advanced_extension()) {
        const auto &extension = fetchRel.advanced_extension();
        DataTypePtr inputRowType;
        std::vector<DataTypePtr> types;
        if (!ParseOmniType(extension, inputRowType) || !FlattenSingleLevel(inputRowType, types)) {
            LOG_VALIDATION_MSG("Unsupported input types in FetchRel.");
            return false;
        }
    }

    if (fetchRel.offset() != 0 || fetchRel.count() < 0) {
        LOG_VALIDATION_MSG("Offset and count should be valid in FetchRel.");
        return false;
    }

    return true;
}

bool SubstraitToOmniPlanValidator::Validate(const ::substrait::TopNRel &topNRel)
{
    DataTypesPtr rowType = nullptr;
    // Get and Validate the input types from extension.
    if (topNRel.has_advanced_extension()) {
        const auto &extension = topNRel.advanced_extension();
        DataTypePtr inputRowType;
        std::vector<DataTypePtr> types;
        if (!ParseOmniType(extension, inputRowType) || !FlattenSingleLevel(inputRowType, types)) {
            LOG_VALIDATION_MSG("Unsupported input types in TopNRel.");
            return false;
        }

        int32_t inputPlanNodeId = 0;
        std::vector<std::string> names;
        names.reserve(types.size());
        for (auto colIdx = 0; colIdx < types.size(); colIdx++) {
            names.emplace_back(SubstraitParser::MakeNodeName(inputPlanNodeId, colIdx));
        }
        rowType = std::make_shared<DataTypes>(std::move(types));
    }

    if (topNRel.n() < 0) {
        LOG_VALIDATION_MSG("N should be valid in TopNRel.");
        return false;
    }

    auto [sortingKeys, sortingOrders, sortNullFirsts] = planConverter.ProcessSortField(topNRel.sorts(), rowType);
    std::set<int> sortingKeyNames;
    for (const auto &sortingKey : sortingKeys) {
        auto result = sortingKeyNames.insert(sortingKey);
        if (!result.second) {
            LOG_VALIDATION_MSG("Duplicate sort keys were found in TopNRel.");
            return false;
        }
    }

    return true;
}

bool SubstraitToOmniPlanValidator::Validate(const ::substrait::ExpandRel &expandRel)
{
    if (expandRel.has_input() && !Validate(expandRel.input())) {
        LOG_VALIDATION_MSG("Input validation fails in ExpandRel.");
        return false;
    }
    DataTypesPtr rowType = nullptr;
    // Get and Validate the input types from extension.
    if (expandRel.has_advanced_extension()) {
        const auto &extension = expandRel.advanced_extension();
        DataTypePtr inputRowType;
        std::vector<DataTypePtr> types;
        if (!ParseOmniType(extension, inputRowType) || !FlattenSingleLevel(inputRowType, types)) {
            LOG_VALIDATION_MSG("Unsupported input types in ExpandRel.");
            return false;
        }

        int32_t inputPlanNodeId = 0;
        std::vector<std::string> names;
        names.reserve(types.size());
        for (auto colIdx = 0; colIdx < types.size(); colIdx++) {
            names.emplace_back(SubstraitParser::MakeNodeName(inputPlanNodeId, colIdx));
        }
        rowType = std::make_shared<DataTypes>(std::move(types));
    }

    int32_t projectSize = 0;
    // Validate fields.
    for (const auto &fields : expandRel.fields()) {
        std::vector<TypedExprPtr> expressions;
        if (fields.has_switching_field()) {
            auto projectExprs = fields.switching_field().duplicates();
            expressions.reserve(projectExprs.size());
            if (projectSize == 0) {
                projectSize = projectExprs.size();
            } else if (projectSize != projectExprs.size()) {
                LOG_VALIDATION_MSG("SwitchingField expressions size should be constant in ExpandRel.");
                return false;
            }

            for (const auto &projectExpr : projectExprs) {
                const auto &typeCase = projectExpr.rex_type_case();
                switch (typeCase) {
                    case ::substrait::Expression::RexTypeCase::kSelection:
                    case ::substrait::Expression::RexTypeCase::kLiteral:
                        break;
                    default:
                        LOG_VALIDATION_MSG("Only field or literal is supported in project of ExpandRel.");
                        return false;
                }
                if (rowType) {
                    expressions.emplace_back(exprConverter_->ToOmniExpr(projectExpr, rowType));
                }
            }

            if (rowType) {
                // Try to compile the expressions. If there is any unregistered
                // function or mismatched type, exception will be thrown.
                ExprVerifier ev;
                for (const auto &expression : expressions) {
                    if (!ev.VisitExpr(*expression)) {
                        return false;
                    }
                }
            }
        } else {
            LOG_VALIDATION_MSG("Only SwitchingField is supported in ExpandRel.");
            return false;
        }
    }

    return true;
}

bool ValidateBoundType(::substrait::Expression_WindowFunction_Bound boundType)
{
    switch (boundType.kind_case()) {
        case ::substrait::Expression_WindowFunction_Bound::kUnboundedFollowing:
        case ::substrait::Expression_WindowFunction_Bound::kUnboundedPreceding:
        case ::substrait::Expression_WindowFunction_Bound::kCurrentRow:
        case ::substrait::Expression_WindowFunction_Bound::kFollowing:
        case ::substrait::Expression_WindowFunction_Bound::kPreceding:
            break;
        default:
            return false;
    }
    return true;
}

bool SubstraitToOmniPlanValidator::Validate(const ::substrait::WindowRel &windowRel)
{
    if (windowRel.has_input() && !Validate(windowRel.input())) {
        LOG_VALIDATION_MSG("WindowRel input fails to Validate.");
        return false;
    }

    // Get and Validate the input types from extension.
    if (!windowRel.has_advanced_extension()) {
        LOG_VALIDATION_MSG("Input types are expected in WindowRel.");
        return false;
    }
    const auto &extension = windowRel.advanced_extension();
    DataTypePtr inputRowType;
    std::vector<DataTypePtr> types;
    if (!ParseOmniType(extension, inputRowType) || !FlattenSingleLevel(inputRowType, types)) {
        LOG_VALIDATION_MSG("Validation failed for input types in WindowRel.");
        return false;
    }

    if (types.empty()) {
        // See: https://github.com/apache/incubator-gluten/issues/7600.
        LOG_VALIDATION_MSG("Validation failed for empty input schema in WindowRel.");
        return false;
    }

    auto rowType = std::make_shared<DataTypes>(std::move(types));

    // Validate WindowFunction
    std::vector<std::string> funcSpecs;
    funcSpecs.reserve(windowRel.measures().size());
    for (const auto &smea : windowRel.measures()) {
        const auto &windowFunction = smea.measure();
        funcSpecs.emplace_back(planConverter.FindFuncSpec(windowFunction.function_reference()));
        SubstraitParser::ParseType(windowFunction.output_type());
        for (const auto &arg : windowFunction.arguments()) {
            auto typeCase = arg.value().rex_type_case();
            switch (typeCase) {
                case ::substrait::Expression::RexTypeCase::kSelection:
                case ::substrait::Expression::RexTypeCase::kLiteral:
                    break;
                default:
                    LOG_VALIDATION_MSG("Only field or constant is supported in window functions.");
                    return false;
            }
        }
        // Validate BoundType and Frame Type
        switch (windowFunction.window_type()) {
            case ::substrait::WindowType::ROWS:
            case ::substrait::WindowType::RANGE:
                break;
            default:
                LOG_VALIDATION_MSG("the window type only support ROWS and RANGE, and the "
                                   "input type is " +
                    std::to_string(windowFunction.window_type()));
                return false;
        }

        bool boundTypeSupported =
            ValidateBoundType(windowFunction.upper_bound()) && ValidateBoundType(windowFunction.lower_bound());
        if (!boundTypeSupported) {
            LOG_VALIDATION_MSG("Found unsupported Bound Type: upper " +
                std::to_string(windowFunction.upper_bound().kind_case()) + ", lower " +
                std::to_string(windowFunction.lower_bound().kind_case()));
            return false;
        }
    }

    // Validate groupby expression
    const auto &groupByExprs = windowRel.partition_expressions();
    std::vector<TypedExprPtr> expressions;
    expressions.reserve(groupByExprs.size());
    for (const auto &expr : groupByExprs) {
        auto expression = exprConverter_->ToOmniExpr(expr, rowType);
        auto exprField = dynamic_cast<const FieldExpr *>(expression);
        if (exprField == nullptr) {
            LOG_VALIDATION_MSG("Only field is supported for partition key in Window Operator!");
            return false;
        } else {
            expressions.emplace_back(expression);
        }
    }
    // Try to compile the expressions. If there is any unregistred funciton or
    // mismatched type, exception will be thrown.
    ExprVerifier ev;
    for (const auto &expression : expressions) {
        if (!ev.VisitExpr(*expression)) {
            return false;
        }
    }

    // Validate Sort expression
    const auto &sorts = windowRel.sorts();
    for (const auto &sort : sorts) {
        switch (sort.direction()) {
            case ::substrait::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_FIRST:
            case ::substrait::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_LAST:
            case ::substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_FIRST:
            case ::substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_LAST:
                break;
            default:
                LOG_VALIDATION_MSG("in windowRel, unsupported Sort direction " + std::to_string(sort.direction()));
                return false;
        }

        if (sort.has_expr()) {
            auto expression = exprConverter_->ToOmniExpr(sort.expr(), rowType);
            auto exprField = dynamic_cast<const FieldExpr *>(expression);
            if (!exprField) {
                LOG_VALIDATION_MSG("in windowRel, the sorting key in Sort Operator "
                                   "only support field.");
                return false;
            }
            if (!ev.VisitExpr(*expression)) {
                return false;
            }
        }
    }

    return true;
}

bool SubstraitToOmniPlanValidator::Validate(const ::substrait::SetRel &setRel)
{
    switch (setRel.op()) {
        case ::substrait::SetRel_SetOp::SetRel_SetOp_SET_OP_UNION_ALL: {
            for (int32_t i = 0; i < setRel.inputs_size(); ++i) {
                const auto &input = setRel.inputs(i);
                if (!Validate(input)) {
                    LOG_VALIDATION_MSG("ProjectRel input");
                    return false;
                }
            }
            if (!setRel.has_advanced_extension()) {
                LOG_VALIDATION_MSG("Input types are expected in SetRel.");
                return false;
            }
            const auto &extension = setRel.advanced_extension();
            DataTypePtr inputRowType;
            std::vector<std::vector<DataTypePtr>> childrenTypes;
            if (!ParseOmniType(extension, inputRowType)) {
                LOG_VALIDATION_MSG("Validation failed for input types in SetRel.");
                return false;
            }
            std::vector<DataTypesPtr> childrenRowTypes;
            for (auto i = 0; i < childrenTypes.size(); ++i) {
                auto &types = childrenTypes.at(i);
                std::vector<std::string> names;
                names.reserve(types.size());
                for (auto colIdx = 0; colIdx < types.size(); colIdx++) {
                    names.emplace_back(SubstraitParser::MakeNodeName(i, colIdx));
                }
                childrenRowTypes.push_back(std::make_shared<DataTypes>(std::move(types)));
            }

            for (auto i = 1; i < childrenRowTypes.size(); ++i) {
                if (childrenRowTypes[i] != childrenRowTypes[0]) {
                    LOG_VALIDATION_MSG("All sources of the Set operation must have the "
                                       "same output type: ");
                    return false;
                }
            }
            return true;
        }
        default:
            LOG_VALIDATION_MSG("Unsupported SetRel op: " + std::to_string(setRel.op()));
            return false;
    }
}

bool SubstraitToOmniPlanValidator::Validate(const ::substrait::SortRel &sortRel)
{
    if (sortRel.has_input() && !Validate(sortRel.input())) {
        return false;
    }

    // Get and Validate the input types from extension.
    if (!sortRel.has_advanced_extension()) {
        LOG_VALIDATION_MSG("Input types are expected in SortRel.");
        return false;
    }

    const auto &extension = sortRel.advanced_extension();
    DataTypePtr inputRowType;
    std::vector<DataTypePtr> types;
    if (!ParseOmniType(extension, inputRowType) || !FlattenSingleLevel(inputRowType, types)) {
        LOG_VALIDATION_MSG("Validation failed for input types in SortRel.");
        return false;
    }

    auto rowType = std::make_shared<DataTypes>(std::move(types));

    const auto &sorts = sortRel.sorts();
    for (const auto &sort : sorts) {
        switch (sort.direction()) {
            case ::substrait::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_FIRST:
            case ::substrait::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_LAST:
            case ::substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_FIRST:
            case ::substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_LAST:
                break;
            default:
                LOG_VALIDATION_MSG("unsupported Sort direction " + std::to_string(sort.direction()));
                return false;
        }

        if (sort.has_expr()) {
            auto expression = exprConverter_->ToOmniExpr(sort.expr(), rowType);
            auto exprField = dynamic_cast<const FieldExpr *>(expression);
            if (!exprField) {
                LOG_VALIDATION_MSG("in SortRel, the sorting key in Sort Operator only support field.");
                return false;
            }
            ExprVerifier ev;
            if (!ev.VisitExpr(*expression)) {
                return false;
            }
        }
    }

    return true;
}

bool SubstraitToOmniPlanValidator::Validate(const ::substrait::ProjectRel &projectRel)
{
    if (projectRel.has_input() && !Validate(projectRel.input())) {
        LOG_VALIDATION_MSG("ProjectRel input");
        return false;
    }

    // Get and Validate the input types from extension.
    if (!projectRel.has_advanced_extension()) {
        LOG_VALIDATION_MSG("Input types are expected in ProjectRel.");
        return false;
    }
    const auto &extension = projectRel.advanced_extension();
    DataTypePtr inputRowType;
    std::vector<DataTypePtr> types;
    if (!ParseOmniType(extension, inputRowType) || !FlattenSingleLevel(inputRowType, types)) {
        LOG_VALIDATION_MSG("Validation failed for input types in ProjectRel.");
        return false;
    }

    int32_t inputPlanNodeId = 0;
    // Create the fake input names to be used in row type.
    std::vector<std::string> names;
    names.reserve(types.size());
    for (uint32_t colIdx = 0; colIdx < types.size(); colIdx++) {
        names.emplace_back(SubstraitParser::MakeNodeName(inputPlanNodeId, colIdx));
    }
    auto rowType = std::make_shared<DataTypes>(std::move(types));

    // Validate the project expressions.
    const auto &projectExprs = projectRel.expressions();
    std::vector<TypedExprPtr> expressions;
    expressions.reserve(projectExprs.size());
    for (const auto &expr : projectExprs) {
        if (!ValidateExpression(expr, rowType)) {
            LOG_VALIDATION_MSG("substrait validation fail!");
            return false;
        }
        expressions.emplace_back(exprConverter_->ToOmniExpr(expr, rowType));
    }
    // Try to compile the expressions. If there is any unregistered function or
    // mismatched type, exception will be thrown.
    ExprVerifier ev;
    for (const auto &expression : expressions) {
        if (!ev.VisitExpr(*expression)) {
            LOG_VALIDATION_MSG("OmniExpr validation fail!");
            return false;
        }
    }
    return true;
}

bool SubstraitToOmniPlanValidator::Validate(const ::substrait::FilterRel &filterRel)
{
    if (filterRel.has_input() && !Validate(filterRel.input())) {
        LOG_VALIDATION_MSG("input of FilterRel validation fails");
        return false;
    }

    // Get and Validate the input types from extension.
    if (!filterRel.has_advanced_extension()) {
        LOG_VALIDATION_MSG("Input types are expected in FilterRel.");
        return false;
    }
    const auto &extension = filterRel.advanced_extension();
    DataTypePtr inputRowType;
    std::vector<DataTypePtr> types;
    if (!ParseOmniType(extension, inputRowType) || !FlattenSingleLevel(inputRowType, types)) {
        LOG_VALIDATION_MSG("Validation failed for input types in FilterRel.");
        return false;
    }

    int32_t inputPlanNodeId = 0;
    // Create the fake input names to be used in row type.
    std::vector<std::string> names;
    names.reserve(types.size());
    for (uint32_t colIdx = 0; colIdx < types.size(); colIdx++) {
        names.emplace_back(SubstraitParser::MakeNodeName(inputPlanNodeId, colIdx));
    }
    auto rowType = std::make_shared<DataTypes>(std::move(types));

    std::vector<TypedExprPtr> expressions;
    if (!ValidateExpression(filterRel.condition(), rowType)) {
        return false;
    }
    expressions.emplace_back(exprConverter_->ToOmniExpr(filterRel.condition(), rowType));
    // Try to compile the expressions. If there is any unregistered function
    // or mismatched type, exception will be thrown.
    ExprVerifier ev;
    for (const auto &expression : expressions) {
        if (!ev.VisitExpr(*expression)) {
            return false;
        }
    }
    return true;
}

bool SubstraitToOmniPlanValidator::Validate(const ::substrait::JoinRel &joinRel)
{
    if (joinRel.has_left() && !Validate(joinRel.left())) {
        LOG_VALIDATION_MSG("Validation fails for join left input.");
        return false;
    }

    if (joinRel.has_right() && !Validate(joinRel.right())) {
        LOG_VALIDATION_MSG("Validation fails for join right input.");
        return false;
    }

    if (joinRel.has_advanced_extension() &&
        SubstraitParser::ConfigSetInOptimization(joinRel.advanced_extension(), "isSMJ=")) {
        switch (joinRel.type()) {
            case ::substrait::JoinRel_JoinType_JOIN_TYPE_INNER:
            case ::substrait::JoinRel_JoinType_JOIN_TYPE_OUTER:
            case ::substrait::JoinRel_JoinType_JOIN_TYPE_LEFT:
            case ::substrait::JoinRel_JoinType_JOIN_TYPE_RIGHT:
            case ::substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_SEMI:
            case ::substrait::JoinRel_JoinType_JOIN_TYPE_RIGHT_SEMI:
            case ::substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_ANTI:
                break;
            default:
                LOG_VALIDATION_MSG("Sort merge join type is not supported: " + std::to_string(joinRel.type()));
                return false;
        }
    }
    switch (joinRel.type()) {
        case ::substrait::JoinRel_JoinType_JOIN_TYPE_INNER:
        case ::substrait::JoinRel_JoinType_JOIN_TYPE_OUTER:
        case ::substrait::JoinRel_JoinType_JOIN_TYPE_LEFT:
        case ::substrait::JoinRel_JoinType_JOIN_TYPE_RIGHT:
        case ::substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_SEMI:
        case ::substrait::JoinRel_JoinType_JOIN_TYPE_RIGHT_SEMI:
        case ::substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_ANTI:
            break;
        default:
            LOG_VALIDATION_MSG("Join type is not supported: " + std::to_string(joinRel.type()));
            return false;
    }

    // Validate input types.
    if (!joinRel.has_advanced_extension()) {
        LOG_VALIDATION_MSG("Input types are expected in JoinRel.");
        return false;
    }

    const auto &extension = joinRel.advanced_extension();
    DataTypePtr inputRowType;
    std::vector<DataTypePtr> types;
    if (!ParseOmniType(extension, inputRowType) || !FlattenSingleLevel(inputRowType, types)) {
        LOG_VALIDATION_MSG("Validation failed for input types in JoinRel.");
        return false;
    }

    auto rowType = std::make_shared<DataTypes>(std::move(types));

    if (joinRel.has_expression()) {
        std::vector<const ::substrait::Expression::FieldReference *> leftExprs;
        std::vector<const ::substrait::Expression::FieldReference *> rightExprs;
        planConverter.ExtractJoinKeys(joinRel.expression(), leftExprs, rightExprs);
    }

    if (joinRel.has_post_join_filter()) {
        auto expression = exprConverter_->ToOmniExpr(joinRel.post_join_filter(), rowType);
        ExprVerifier ev;
        if (!ev.VisitExpr(*expression)) {
            return false;
        }
    }
    return true;
}

bool SubstraitToOmniPlanValidator::Validate(const ::substrait::CrossRel &crossRel)
{
    if (crossRel.has_left() && !Validate(crossRel.left())) {
        LogValidateMsg("Native validation failed due to: validation fails for "
                       "cross join left input. ");
        return false;
    }

    if (crossRel.has_right() && !Validate(crossRel.right())) {
        LogValidateMsg("Native validation failed due to: validation fails for "
                       "cross join right input. ");
        return false;
    }

    // Validate input types.
    if (!crossRel.has_advanced_extension()) {
        LogValidateMsg("Native validation failed due to: Input types are expected "
                       "in CrossRel.");
        return false;
    }

    switch (crossRel.type()) {
        case ::substrait::CrossRel_JoinType_JOIN_TYPE_INNER:
        case ::substrait::CrossRel_JoinType_JOIN_TYPE_LEFT:
            break;
        default:
            LOG_VALIDATION_MSG("Unsupported Join type in CrossRel");
            return false;
    }

    const auto &extension = crossRel.advanced_extension();
    DataTypePtr inputRowType;
    std::vector<DataTypePtr> types;
    if (!ParseOmniType(extension, inputRowType) || !FlattenSingleLevel(inputRowType, types)) {
        LogValidateMsg("Native validation failed due to: Validation failed for "
                       "input types in CrossRel");
        return false;
    }

    auto rowType = std::make_shared<DataTypes>(std::move(types));

    if (crossRel.has_expression()) {
        auto expression = exprConverter_->ToOmniExpr(crossRel.expression(), rowType);
        ExprVerifier ev;
        if (!ev.VisitExpr(*expression)) {
            return false;
        }
    }

    return true;
}

bool SubstraitToOmniPlanValidator::Validate(const ::substrait::AggregateRel &aggRel)
{
    // impl the expr check here
    return true;
}

bool SubstraitToOmniPlanValidator::Validate(const ::substrait::ReadRel &readRel)
{
    // Validate filter in ReadRel.
    if (readRel.has_filter()) {
        std::vector<DataTypePtr> omniTypeList;
        if (readRel.has_base_schema()) {
            const auto& baseSchema = readRel.base_schema();
            omniTypeList = SubstraitParser::ParseNamedStruct(baseSchema);
        }

        auto rowType = std::make_shared<DataTypes>(std::move(omniTypeList));
        std::vector<TypedExprPtr> expressions;
        if (!ValidateExpression(readRel.filter(), rowType)) {
            return false;
        }
    }

    return true;
}

bool SubstraitToOmniPlanValidator::Validate(const ::substrait::Rel &rel)
{
    if (rel.has_aggregate()) {
        return Validate(rel.aggregate());
    } else if (rel.has_project()) {
        return Validate(rel.project());
    } else if (rel.has_filter()) {
        return Validate(rel.filter());
    } else if (rel.has_join()) {
        return Validate(rel.join());
    } else if (rel.has_cross()) {
        return Validate(rel.cross());
    } else if (rel.has_read()) {
        return Validate(rel.read());
    } else if (rel.has_sort()) {
        return Validate(rel.sort());
    } else if (rel.has_expand()) {
        return Validate(rel.expand());
    } else if (rel.has_fetch()) {
        return Validate(rel.fetch());
    } else if (rel.has_top_n()) {
        return Validate(rel.top_n());
    } else if (rel.has_window()) {
        return Validate(rel.window());
    } else if (rel.has_write()) {
        return Validate(rel.write());
    } else if (rel.has_set()) {
        return Validate(rel.set());
    } else {
        LOG_VALIDATION_MSG("Unsupported relation type: " + rel.GetTypeName());
        return false;
    }
}

bool SubstraitToOmniPlanValidator::Validate(const ::substrait::RelRoot &relRoot)
{
    if (relRoot.has_input()) {
        return Validate(relRoot.input());
    } else {
        return false;
    }
}

bool SubstraitToOmniPlanValidator::Validate(const ::substrait::Plan &plan)
{
    try {
        // Create plan converter and expression converter to help the validation.
        planConverter.ConstructFunctionMap(plan);
        exprConverter_ = planConverter.GetExprConverter();

        for (const auto &rel : plan.relations()) {
            if (rel.has_root()) {
                return Validate(rel.root());
            } else if (rel.has_rel()) {
                return Validate(rel.rel());
            }
        }

        return false;
    } catch (const OmniException &err) {
        LOG_VALIDATION_MSG(err.what());
        return false;
    }
}
} // namespace omniruntime
