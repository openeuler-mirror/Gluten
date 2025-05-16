/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: print expression tree methods
 */

#include "SubstraitToOmniExpr.h"

namespace omniruntime
{
// template<typename T>
// void setLiteralValue(const ::substrait::Expression::Literal &literal, FlatVector<T> *vector, vector_size_t index)
// {
//     if (literal.has_null()) {
//         vector->setNull(index, true);
//     } else {
//         vector->set(index, gluten::SubstraitParser::getLiteralValue<T>(literal));
//     }
// }

DataTypePtr GetScalarType(const ::substrait::Expression::Literal &literal)
{
    auto typeCase = literal.literal_type_case();
    switch (typeCase) {
        case ::substrait::Expression_Literal::LiteralTypeCase::kBoolean:
            return BooleanType();
        case ::substrait::Expression_Literal::LiteralTypeCase::kI16:
            return ShortType();
        case ::substrait::Expression_Literal::LiteralTypeCase::kI32:
            return IntType();
        case ::substrait::Expression_Literal::LiteralTypeCase::kI64:
        case ::substrait::Expression_Literal::LiteralTypeCase::kFp64:
            return DoubleType();
        case ::substrait::Expression_Literal::LiteralTypeCase::kDecimal: {
            auto precision = literal.decimal().precision();
            auto scale = literal.decimal().scale();
            auto type = Decimal64Type(precision, scale);
            return type;
        }
        case ::substrait::Expression_Literal::LiteralTypeCase::kDate:
            return Date64Type();
        case ::substrait::Expression_Literal::LiteralTypeCase::kTimestamp:
            return TimestampType();
        case ::substrait::Expression_Literal::LiteralTypeCase::kString:
            return VarcharType();
        case ::substrait::Expression_Literal::LiteralTypeCase::kVarChar:
            return VarcharType();
        default:
            return nullptr;
    }
}

/// Whether null will be returned on cast failure.
bool IsNullOnFailure(::substrait::Expression::Cast::FailureBehavior failureBehavior)
{
    switch (failureBehavior) {
        case ::substrait::Expression_Cast_FailureBehavior_FAILURE_BEHAVIOR_UNSPECIFIED:
        case ::substrait::Expression_Cast_FailureBehavior_FAILURE_BEHAVIOR_THROW_EXCEPTION:
            return false;
        case ::substrait::Expression_Cast_FailureBehavior_FAILURE_BEHAVIOR_RETURN_NULL:
            return true;
        default:
            OMNI_THROW("SUBSTRAIT_ERROR:", "The given failure behavior is NOT supported: '{}'",
                std::to_string(failureBehavior));
    }
}

template<DataTypeId kind>
std::shared_ptr<LiteralExpr> ConstructConstantVector(const ::substrait::Expression::Literal &substraitLit,
    const DataTypePtr &type)
{
    if (substraitLit.has_binary()) {
        return std::make_shared<LiteralExpr>(
            new std::string(SubstraitParser::GetLiteralValue<std::string>(substraitLit)),
            type);
    } else {
        using T = typename NativeType<kind>::type;
        return std::make_shared<LiteralExpr>(SubstraitParser::GetLiteralValue<T>(substraitLit), type);
    }
}

std::shared_ptr<const FieldExpr> SubstraitOmniExprConverter::ToOmniExpr(
    const ::substrait::Expression::FieldReference &substraitField, const DataTypesPtr &inputType)
{
    auto typeCase = substraitField.reference_type_case();
    switch (typeCase) {
        case ::substrait::Expression::FieldReference::ReferenceTypeCase::kDirectReference: {
            const auto &directRef = substraitField.direct_reference();

            const auto *tmp = &directRef.struct_field();
            auto idx = tmp->field();
            auto fieldAccess = std::make_shared<FieldExpr>(idx, inputType->GetType(idx));
            return fieldAccess;
        }
        default:
            OMNI_THROW("SUBSTRAIT_ERROR:", "Substrait conversion not supported for Reference '{}'",
                std::to_string(typeCase));
    }
}

TypedExprPtr SubstraitOmniExprConverter::ToOmniExpr(const ::substrait::Expression::ScalarFunction &substraitFunc,
    const DataTypesPtr &inputType) {}

TypedExprPtr SubstraitOmniExprConverter::ToOmniExpr(const ::substrait::Expression::SingularOrList &singularOrList,
    const DataTypesPtr &inputType) {}

TypedExprPtr SubstraitOmniExprConverter::ToOmniExpr(const ::substrait::Expression::Cast &castExpr,
    const DataTypesPtr &inputType) {}

TypedExprPtr SubstraitOmniExprConverter::toExtractExpr(const std::vector<TypedExprPtr> &params,
    const DataTypePtr &outputType) {}

std::shared_ptr<const LiteralExpr> SubstraitOmniExprConverter::ToOmniExpr(
    const ::substrait::Expression::Literal &substraitLit) {}

TypedExprPtr SubstraitOmniExprConverter::ToOmniExpr(const ::substrait::Expression::IfThen &substraitIfThen,
    const DataTypesPtr &inputType) {}

std::shared_ptr<const LiteralExpr> SubstraitOmniExprConverter::literalsToConstantExpr(
    const std::vector<::substrait::Expression::Literal> &literals) {}

/// Create expression for lambda.
std::shared_ptr<const Expr> SubstraitOmniExprConverter::toLambdaExpr(
    const ::substrait::Expression::ScalarFunction &substraitFunc,
    const DataTypesPtr &inputType) {}

// TypedExprPtr SubstraitOmniExprConverter::toExtractExpr(const std::vector<TypedExprPtr> &params,
//     const DataTypePtr &outputType)
// {
//     auto functionArg = std::dynamic_pointer_cast<const LiteralExpr>(params[0]);
//     if (functionArg) {
//         // Get the function argument.
//         auto variant = functionArg->value();
//         if (!variant.hasValue()) {
//             OMNI_THROW("SUBSTRAIT_ERROR:", "Value expected in variant.");
//         }
//         // The first parameter specifies extracting from which field.
//         std::string from = variant.value<std::string>();
//
//         // The second parameter is the function parameter.
//         std::vector<TypedExprPtr> exprParams;
//         exprParams.reserve(1);
//         exprParams.emplace_back(params[1]);
//         auto iter = extractDatetimeFunctionMap_.find(from);
//         if (iter != extractDatetimeFunctionMap_.end()) {
//             return std::make_shared<const ToOmniExprCallTypedExpr>(outputType, std::move(exprParams), iter->second);
//         } else {
//             OMNI_THROW("SUBSTRAIT_ERROR:", "Extract from {} not supported.", from);
//         }
//     }
//     OMNI_THROW("SUBSTRAIT_ERROR:", "Constant is expected to be the first parameter in extract.");
// }
//
// TypedExprPtr SubstraitOmniExprConverter::ToOmniExpr(const ::substrait::Expression::ScalarFunction &substraitFunc,
//     const DataTypesPtr &inputType)
// {
//     std::vector<TypedExprPtr> params;
//     params.reserve(substraitFunc.arguments().size());
//     for (const auto &sArg: substraitFunc.arguments()) {
//         params.emplace_back(ToOmniExpr(sArg.value(), inputType));
//     }
//     const auto &veloxFunction = SubstraitParser::findVeloxFunction(functionMap_, substraitFunc.function_reference());
//     const auto &outputType = SubstraitParser::parseType(substraitFunc.output_type());
//
//     if (veloxFunction == "lambdafunction") {
//         return toLambdaExpr(substraitFunc, inputType);
//     } else if (veloxFunction == "namedlambdavariable") {
//         return makeFieldAccessExpr(substraitFunc.arguments(0).value().literal().string(), outputType, nullptr);
//     } else if (veloxFunction == "extract") {
//         return toExtractExpr(std::move(params), outputType);
//     } else {
//         return std::make_shared<const ToOmniExprCallTypedExpr>(outputType, std::move(params), veloxFunction);
//     }
// }
//
// std::shared_ptr<const LiteralExpr> SubstraitOmniExprConverter::literalsToConstantExpr(
//     const std::vector<::substrait::Expression::Literal> &literals)
// {
//     std::vector<variant> variants;
//     variants.reserve(literals.size());
//     VELOX_CHECK_GE(literals.size(), 0, "List should have at least one item.");
//     std::optional<DataTypePtr> literalType;
//     for (const auto &literal: literals) {
//         auto veloxVariant = ToOmniExpr(literal);
//         if (!literalType.has_value()) {
//             literalType = veloxVariant->type();
//         }
//         variants.emplace_back(veloxVariant->value());
//     }
//     VELOX_CHECK(literalType.has_value(), "Type expected.");
//     auto varArray = variant::array(variants);
//     ArrayVectorPtr arrayVector = variantArrayToVector(ARRAY(literalType.value()), varArray.array(), pool_);
//     // Wrap the array vector into constant vector.
//     auto constantVector = BaseVector::wrapInConstant(1 /*length*/, 0 /*index*/, arrayVector);
//     return std::make_shared<const ToOmniExprConstantTypedExpr>(constantVector);
// }
//
// TypedExprPtr SubstraitOmniExprConverter::ToOmniExpr(const substrait::Expression::Cast &castExpr,
//     const DataTypesPtr &inputType)
// {
//     auto type = SubstraitParser::parseType(castExpr.type());
//     bool nullOnFailure = isNullOnFailure(castExpr.failure_behavior());
//
//     std::vector inputs{ToOmniExpr(castExpr.input(), inputType)};
//     return std::make_shared<FuncExpr>("CAST", inputs);
// }
//
// TypedExprPtr SubstraitOmniExprConverter::ToOmniExpr(const substrait::Expression::IfThen &ifThenExpr,
//     const DataTypesPtr &inputType)
// {
//     // Params are concatenated conditions and results with an optional "else" at
//     // the end, e.g. {condition1, result1, condition2, result2,..else}
//     std::vector<TypedExprPtr> params;
//     // If and then expressions are in pairs.
//     params.reserve(ifThenExpr.ifs().size() * 2);
//     std::optional<DataTypePtr> outputType = std::nullopt;
//     for (const auto &ifThen: ifThenExpr.ifs()) {
//         params.emplace_back(ToOmniExpr(ifThen.if_(), inputType));
//         const auto &thenExpr = ToOmniExpr(ifThen.then(), inputType);
//         // Get output type from the first then expression.
//         if (!outputType.has_value()) {
//             outputType = thenExpr->GetReturnType();
//         }
//         params.emplace_back(thenExpr);
//     }
//
//     if (ifThenExpr.has_else_()) {
//         params.reserve(1);
//         params.emplace_back(ToOmniExpr(ifThenExpr.else_(), inputType));
//     }
//     if (ifThenExpr.ifs().size() == 1) {
//         // If there is only one if-then clause, use if expression.
//         return std::make_shared<const IfExpr>(outputType.value(), params[0].get(), params[1]);
//     }
//     return std::make_shared<const IfExpr>(outputType.value(), std::move(params), "switch");
// }

TypedExprPtr SubstraitOmniExprConverter::ToOmniExpr(const substrait::Expression &substraitExpr,
    const DataTypesPtr &inputType)
{
    auto typeCase = substraitExpr.rex_type_case();
    switch (typeCase) {
        case ::substrait::Expression::RexTypeCase::kLiteral:
            return ToOmniExpr(substraitExpr.literal());
        case ::substrait::Expression::RexTypeCase::kScalarFunction:
            return ToOmniExpr(substraitExpr.scalar_function(), inputType);
        case ::substrait::Expression::RexTypeCase::kSelection:
            return ToOmniExpr(substraitExpr.selection(), inputType);
        case ::substrait::Expression::RexTypeCase::kCast:
            return ToOmniExpr(substraitExpr.cast(), inputType);
        case ::substrait::Expression::RexTypeCase::kIfThen:
            return ToOmniExpr(substraitExpr.if_then(), inputType);
        case ::substrait::Expression::RexTypeCase::kSingularOrList:
            return ToOmniExpr(substraitExpr.singular_or_list(), inputType);
        default:
            OMNI_THROW("Substrait_Error:", "Substrait conversion not supported for Expression '{}'",
                std::to_string(typeCase));
    }
}

std::unordered_map<std::string, std::string> SubstraitOmniExprConverter::extractDatetimeFunctionMap_ = {
    {"MILLISECOND", "millisecond"},
    {"SECOND", "second"},
    {"MINUTE", "minute"},
    {"HOUR", "hour"},
    {"DAY", "day"},
    {"DAY_OF_WEEK", "dayofweek"},
    {"WEEK_DAY", "weekday"},
    {"DAY_OF_YEAR", "dayofyear"},
    {"MONTH", "month"},
    {"QUARTER", "quarter"},
    {"YEAR", "year"},
    {"WEEK_OF_YEAR", "week_of_year"},
    {"YEAR_OF_WEEK", "year_of_week"}
};
}
