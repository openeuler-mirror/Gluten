/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: print expression tree methods
 */

#pragma once

#include "SubstraitToOmniPlan.h"
#include "memory/memory_pool.h"

namespace omniruntime {
/// This class is used to validate whether the computing of
/// a Substrait plan is supported in Omni.
class SubstraitToOmniPlanValidator {
public:
    explicit SubstraitToOmniPlanValidator(mem::MemoryPool *pool)
        : pool(pool), planConverter(confMap, std::nullopt, true) {}

    /// Used to validate whether the computing of this Plan is supported.
    bool Validate(const ::substrait::Plan &plan);

    const std::vector<std::string> &GetValidateLog() const
    {
        return validateLog;
    }

private:
    /// Used to validate whether the computing of this Write is supported.
    bool Validate(const ::substrait::WriteRel &writeRel);

    /// Used to validate whether the computing of this Limit is supported.
    bool Validate(const ::substrait::FetchRel &fetchRel);

    /// Used to validate whether the computing of this TopN is supported.
    bool Validate(const ::substrait::TopNRel &topNRel);

    /// Used to validate whether the computing of this Expand is supported.
    bool Validate(const ::substrait::ExpandRel &expandRel);

    /// Used to validate whether the computing of this Sort is supported.
    bool Validate(const ::substrait::SortRel &sortRel);

    /// Used to validate whether the computing of this Window is supported.
    bool Validate(const ::substrait::WindowRel &windowRel);

    /// Used to validate whether the computing of this WindowGroupLimit is supported.
    bool Validate(const ::substrait::WindowGroupLimitRel &windowGroupLimitRel);

    /// Used to validate whether the computing of this Set is supported.
    bool Validate(const ::substrait::SetRel &setRel);

    /// Used to validate whether the computing of this Aggregation is supported.
    bool Validate(const ::substrait::AggregateRel &aggRel);

    /// Used to validate whether the computing of this Project is supported.
    bool Validate(const ::substrait::ProjectRel &projectRel);

    /// Used to validate whether the computing of this Filter is supported.
    bool Validate(const ::substrait::FilterRel &filterRel);

    /// Used to validate Join.
    bool Validate(const ::substrait::JoinRel &joinRel);

    /// Used to validate Cartesian product.
    bool Validate(const ::substrait::CrossRel &crossRel);

    /// Used to validate whether the computing of this Read is supported.
    bool Validate(const ::substrait::ReadRel &readRel);

    /// Used to validate whether the computing of this Rel is supported.
    bool Validate(const ::substrait::Rel &rel);

    /// Used to validate whether the computing of this RelRoot is supported.
    bool Validate(const ::substrait::RelRoot &relRoot);

    /// A memory pool used for function validation.
    mem::MemoryPool *pool;

    // Unused customized conf map.
    std::unordered_map<std::string, std::string> confMap = {};

    /// A converter used to convert Substrait plan into Omni's plan node.
    SubstraitToOmniPlanConverter planConverter;

    /// An expression converter used to convert Substrait representations into
    /// Omni expressions.
    SubstraitOmniExprConverter *exprConverter_ = nullptr;

    std::vector<std::string> validateLog;

    /// Used to get types from advanced extension and validate them, then convert to a Omni type that has arbitrary
    /// levels of nesting.
    bool ParseOmniType(const ::substrait::extensions::AdvancedExtension &extension, DataTypePtr &out);

    /// Flattens a Omni type with single level of nesting into a std::vector of child types.
    bool FlattenSingleLevel(const DataTypePtr &type, std::vector<DataTypePtr> &out);

    /// Validate the round scalar function.
    bool ValidateRound(const ::substrait::Expression::ScalarFunction &scalarFunction, const DataTypesPtr &inputType);

    /// Validate extract function.
    bool ValidateExtractExpr(const std::vector<TypedExprPtr> &params);

    /// Validates regex functions.
    /// Ensures the second pattern argument is a literal string.
    /// Check if the pattern can pass with RE2 compilation.
    bool ValidateRegexExpr(const std::string &name, const ::substrait::Expression::ScalarFunction &scalarFunction);

    /// Validate Substrait scarlar function.
    bool ValidateScalarFunction(
        const ::substrait::Expression::ScalarFunction &scalarFunction,
        const DataTypesPtr &inputType);

    /// Validate Substrait Cast expression.
    bool ValidateCast(const ::substrait::Expression::Cast &castExpr, const DataTypesPtr &inputType);

    /// Validate Substrait expression.
    bool ValidateExpression(const ::substrait::Expression &expression, const DataTypesPtr &inputType);

    /// Validate Substrait literal.
    bool ValidateLiteral(const ::substrait::Expression_Literal &literal, const DataTypesPtr &inputType);

    /// Validate Substrait if-then expression.
    bool ValidateIfThen(const ::substrait::Expression_IfThen &ifThen, const DataTypesPtr &inputType);

    /// Validate Substrait IN expression.
    bool ValidateSingularOrList(
        const ::substrait::Expression::SingularOrList &singularOrList,
        const DataTypesPtr &inputType);

    /// Add necessary log for fallback
    void LogValidateMsg(const std::string &log)
    {
        validateLog.emplace_back(log);
    }
};
} // namespace gluten
