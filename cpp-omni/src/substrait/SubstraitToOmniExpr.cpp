/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: print expression tree methods
 */

#include "SubstraitToOmniExpr.h"
#include "expression/parserhelper.h"

constexpr const char *SUBSTRAIT_PARSE_ERROR = "SUBSTRAIT_PARSE_ERROR";
namespace omniruntime {

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
            return LongType();
        case ::substrait::Expression_Literal::LiteralTypeCase::kFp64:
            return DoubleType();
        case ::substrait::Expression_Literal::LiteralTypeCase::kDecimal: {
            auto precision = literal.decimal().precision();
            auto scale = literal.decimal().scale();
            if (precision <= DECIMAL64_DEFAULT_PRECISION) {
                auto type = Decimal64Type(precision, scale);
                return type;
            } else {
                auto type = Decimal128Type(precision, scale);
                return type;
            }
        }
        case ::substrait::Expression_Literal::LiteralTypeCase::kDate:
            return Date32Type();
        case ::substrait::Expression_Literal::LiteralTypeCase::kTimestamp:
            return TimestampType();
        case ::substrait::Expression_Literal::LiteralTypeCase::kString:
            return VarcharType();
        case ::substrait::Expression_Literal::LiteralTypeCase::kVarChar:
            return VarcharType();
        default:
            OMNI_THROW(
                "GET_SCALAR_TYPE_ERROR:", "the given typeCase is not supported: '{}' ", std::to_string(typeCase));
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

TypedExprPtr SubstraitOmniExprConverter::ToOmniExpr(
    const ::substrait::Expression::FieldReference &substraitField, const DataTypesPtr &inputType)
{
    auto typeCase = substraitField.reference_type_case();
    switch (typeCase) {
        case ::substrait::Expression::FieldReference::ReferenceTypeCase::kDirectReference: {
            const auto &directRef = substraitField.direct_reference();

            const auto *tmp = &directRef.struct_field();
            auto idx = tmp->field();
            // not support complicated types
            return new FieldExpr(idx, inputType->GetType(idx));
        }
        default:
            OMNI_THROW(
                "SUBSTRAIT_ERROR:", "Substrait conversion not supported for Reference '{}'", std::to_string(typeCase));
    }
}

TypedExprPtr SubstraitOmniExprConverter::ToOmniExpr(
    const ::substrait::Expression::ScalarFunction &substraitFunc, const DataTypesPtr &inputType)
{
    const auto &omniFunction = SubstraitParser::FindOmniFunction(functionMap_, substraitFunc.function_reference());
    const auto &outputType = SubstraitParser::ParseType(substraitFunc.output_type());
    auto type = omniFunction.first;
    auto funcName = omniFunction.second;
    Operator op = StringToOperator(funcName);
    std::vector<Expr *> args;
    args.reserve(substraitFunc.arguments().size());
    for (const auto &sArg : substraitFunc.arguments()) {
        args.emplace_back(ToOmniExpr(sArg.value(), inputType));
    }
    if (type == IS_NOT_NULL_OMNI_EXPR_TYPE) {
        OMNI_CHECK(args[0] != nullptr, "args[0] is null");
        auto isNullExpr = new IsNullExpr(args[0]);
        return new UnaryExpr(Operator::NOT, isNullExpr, std::make_shared<BooleanDataType>());
    } else if (type == IS_NULL_OMNI_EXPR_TYPE) {
        OMNI_CHECK(args[0] != nullptr, "args[0] is null");
        return new IsNullExpr(args[0]);
    } else if (type == UNARY_OMNI_EXPR_TYPE) {
        OMNI_CHECK(args[0] != nullptr, "args[0] is null");
        OMNI_CHECK(op != Operator::INVALIDOP, "the operator is INVALIDOP");
        return new UnaryExpr(op, args[0], std::make_shared<BooleanDataType>());
    } else if (type == BINARY_OMNI_EXPR_TYPE) {
        OMNI_CHECK(outputType != nullptr, "outputType is null");
        OMNI_CHECK(args[0] != nullptr, "args[0] is null");
        OMNI_CHECK(op != Operator::INVALIDOP, "the operator is INVALIDOP");
        if (args[1] == nullptr) {
            delete args[0];
            OMNI_THROW("SUBSTRAIT_ERROR:", "The args[1] in ScalarFunction is nullptr");
        }
        return new BinaryExpr(op, args[0], args[1], std::move(outputType));
    } else if (type == FUNCTION_OMNI_EXPR_TYPE) {
        if (funcName == "concat") {
            return UnfoldConcatStringFunc(args, outputType);
        }
        if (funcName == "MakeDecimal" && args.size() == 2) {
            // only use first arg in func MakeDecimal
            return new FuncExpr(funcName, {args[0]}, std::move(outputType));
        }
        if ((funcName == "RLike") && args.size() == RLIKE_INPUT) {
            auto secondArg = args[1];
            if (secondArg->GetType() != ExprType::LITERAL_E) {
                Expr::DeleteExprs(args);
                OMNI_THROW("SUBSTRAIT_ERROR:", "The type of args[1] is not equal to LITERAL_E");
            }
            auto literalExpr = static_cast<LiteralExpr *>(secondArg);
        }
        // check the signature matches
        std::vector<DataTypeId> argTypes(args.size());
        std::transform(args.begin(), args.end(), argTypes.begin(),
            [](Expr *expr) -> DataTypeId { return expr->GetReturnTypeId(); });
        return new FuncExpr(funcName, args, std::move(outputType));
    } else if (type == COALESCE_OMNI_EXPR_TYPE) {
        if (args.size() != COALESCE_INPUT) {
            OMNI_THROW("SUBSTRAIT_ERROR:", "coalesce expression only support two input parameters");
        }
        OMNI_CHECK(args[0] != nullptr, "args[0] is null");
        if (args[1] == nullptr) {
            delete args[0];
            OMNI_THROW("SUBSTRAIT_ERROR:", "The args[1] in COALESCE_OMNI_EXPR_TYPE is nullptr");
        }
        return new CoalesceExpr(args[0], args[1]);
    } else if (type == HIVE_UDF_FUNCTION_OMNI_EXPR_TYPE) {
        throw omniruntime::exception::OmniException(SUBSTRAIT_PARSE_ERROR, "The UDF function Unsupported yet");
    } else {
        OMNI_THROW(
            "SUBSTRAIT_ERROR:", "function type {} and function {} is unsupported yet", std::to_string(type), funcName);
    }
}

TypedExprPtr SubstraitOmniExprConverter::UnfoldConcatStringFunc(std::vector<Expr *> args,
    DataTypePtr outputType)
{
    int concatParams = 2;
    int argSize = args.size();
    if (argSize == concatParams) {
        return new FuncExpr("concat", {args[0], args[1]}, std::move(outputType));
    }
    std::vector<Expr*> newArgs(args.begin() + 1, args.end());
    TypedExprPtr ret = UnfoldConcatStringFunc(newArgs, outputType);
    return new FuncExpr("concat", {args[0], ret}, std::move(outputType));
}

TypedExprPtr SubstraitOmniExprConverter::ToOmniExpr(
    const ::substrait::Expression::SingularOrList &singularOrList, const DataTypesPtr &inputType)
{
    std::vector<Expr *> args;
    // first element of arguments is the value to be compared to every other
    // argument
    args.push_back(ToOmniExpr(singularOrList.value(), inputType));
    for (const auto &option : singularOrList.options()) {
        Expr *arg = ToOmniExpr(option.literal());
        if (arg != nullptr) {
            args.push_back(arg);
        } else {
            Expr::DeleteExprs(args);
            OMNI_THROW("SUBSTRAIT_ERROR:", "The OmniExpression of the singularOrList.literal here is null");
        }
    }
    return new InExpr(args);
}

TypedExprPtr SubstraitOmniExprConverter::ToOmniExpr(
    const ::substrait::Expression::Cast &castExpr, const DataTypesPtr &inputType)
{
    auto retType = SubstraitParser::ParseType(castExpr.type());
    auto expr = ToOmniExpr(castExpr.input(), inputType);
    auto retTypeId = retType->GetId();
    auto argReturnType = expr->GetReturnType();
    if (retTypeId == argReturnType->GetId()) {
        if (TypeUtil::IsStringType(argReturnType->GetId())) {
            auto argWidth = static_cast<VarcharDataType *>(argReturnType.get())->GetWidth();
            auto retWidth = static_cast<VarcharDataType *>(retType.get())->GetWidth();
            if (argWidth <= retWidth) {
                return expr;
            }
        } else if (TypeUtil::IsDecimalType(retTypeId)) {
            auto argScale = static_cast<DecimalDataType *>(argReturnType.get())->GetScale();
            auto argPrecision = static_cast<DecimalDataType *>(argReturnType.get())->GetPrecision();
            auto retScale = static_cast<DecimalDataType *>(retType.get())->GetScale();
            auto retPrecision = static_cast<DecimalDataType *>(retType.get())->GetPrecision();
            if (argScale == retScale && argPrecision <= retPrecision) {
                return expr;
            }
        } else {
            return expr;
        }
    }
    std::vector<Expr *> args;
    args.push_back(expr);
    std::vector<DataTypeId> argTypes(args.size());
    std::transform(
        args.begin(), args.end(), argTypes.begin(), [](Expr *expr) -> DataTypeId { return expr->GetReturnTypeId(); });
    return new FuncExpr("CAST", args, std::move(retType));
}

TypedExprPtr SubstraitOmniExprConverter::ToOmniExpr(const ::substrait::Expression::Literal &substraitLit)
{
    auto typeCase = substraitLit.literal_type_case();
    switch (typeCase) {
        case ::substrait::Expression_Literal::LiteralTypeCase::kBoolean:
            return new LiteralExpr(substraitLit.boolean(), BooleanType());
        case ::substrait::Expression_Literal::LiteralTypeCase::kI16:
            return new LiteralExpr(substraitLit.i16(), ShortType());
        case ::substrait::Expression_Literal::LiteralTypeCase::kI32:
            return new LiteralExpr(substraitLit.i32(), IntType());
        case ::substrait::Expression_Literal::LiteralTypeCase::kI64:
            return new LiteralExpr(substraitLit.i64(), LongType());
        case ::substrait::Expression_Literal::LiteralTypeCase::kFp64:
            return new LiteralExpr(substraitLit.fp64(), DoubleType());
        case ::substrait::Expression_Literal::LiteralTypeCase::kDate:
            return new LiteralExpr(substraitLit.date(), Date32Type());
        case ::substrait::Expression_Literal::LiteralTypeCase::kTimestamp:
            return new LiteralExpr(substraitLit.timestamp(), TimestampType());
        case ::substrait::Expression_Literal::LiteralTypeCase::kString: {
            auto *stringVal = new std::string(substraitLit.string());
            return new LiteralExpr(stringVal, VarcharType(stringVal->length()));
        }
        case ::substrait::Expression_Literal::LiteralTypeCase::kDecimal: {
            auto decimal = substraitLit.decimal().value();
            auto precision = substraitLit.decimal().precision();
            auto scale = substraitLit.decimal().scale();
            int128_t decimalValue;
            memcpy_s(&decimalValue, sizeof(int128_t), decimal.c_str(), sizeof(int128_t));
            if (precision <= DECIMAL64_DEFAULT_PRECISION) {
                return new LiteralExpr(static_cast<int64_t>(decimalValue), Decimal64Type(precision, scale));
            } else {
                auto *dec128String = new std::string(Uint128ToStr(decimalValue));
                return new LiteralExpr(dec128String, Decimal128Type(precision, scale));
            }
        }
        case ::substrait::Expression_Literal::LiteralTypeCase::kNull: {
            auto dataType = SubstraitParser::ParseType(substraitLit.null());
            LiteralExpr *expr;
            if (TypeUtil::IsDecimalType(dataType->GetId())) {
                auto precision = std::dynamic_pointer_cast<DecimalDataType>(dataType)->GetPrecision();
                auto scale = std::dynamic_pointer_cast<DecimalDataType>(dataType)->GetScale();
                expr = ParserHelper::GetDefaultValueForType(dataType->GetId(), precision, scale);
            } else {
                expr = ParserHelper::GetDefaultValueForType(dataType->GetId());
            }
            if (expr == nullptr) {
                OMNI_THROW("SUBSTRAIT_ERROR:", "The LiteralExpr in kNull case here is null");
            }
            expr->isNull = true;
            return expr;
        }
        default:
            throw omniruntime::exception::OmniException(SUBSTRAIT_PARSE_ERROR,
                "Substrait conversion not supported for type case '{}' " + std::to_string(typeCase));
    }
}

TypedExprPtr SubstraitOmniExprConverter::ToOmniExpr(
    const ::substrait::Expression::IfThen &substraitIfThen, const DataTypesPtr &inputType)
{
    auto ifs = substraitIfThen.ifs();
    if (ifs.size() > 1) {
        throw omniruntime::exception::OmniException(SUBSTRAIT_PARSE_ERROR, "IF size >1");
    }
    Expr *cond = ToOmniExpr(ifs.Get(0).if_(), inputType);
    if (cond == nullptr) {
        return nullptr;
    }
    Expr *trueExpr = (ToOmniExpr(ifs.Get(0).then(), inputType));
    if (trueExpr == nullptr) {
        delete cond;
        return nullptr;
    }
    Expr *falseExpr = (ToOmniExpr(substraitIfThen.else_(), inputType));
    if (falseExpr == nullptr) {
        delete cond;
        delete trueExpr;
        return nullptr;
    }
    if (TypeUtil::IsStringType(falseExpr->GetReturnTypeId()) && falseExpr->GetType() == ExprType::LITERAL_E &&
        static_cast<LiteralExpr *>(falseExpr)->stringVal->compare("null") == 0) {
        delete falseExpr;
        auto literalExpr = ParserHelper::GetDefaultValueForType(trueExpr->GetReturnTypeId());
        if (literalExpr == nullptr) {
            delete cond;
            delete trueExpr;
            literalExpr->isNull = true;
            OMNI_THROW("substrait_error", "the literal expression in substraitIfThen case is null here");
        }
        return new IfExpr(cond, trueExpr, literalExpr);
    }
    return new IfExpr(cond, trueExpr, falseExpr);
}

TypedExprPtr SubstraitOmniExprConverter::ToOmniExpr(
    const substrait::Expression &substraitExpr, const DataTypesPtr &inputType)
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
            OMNI_THROW(
                "Substrait_Error:", "Substrait conversion not supported for Expression '{}'", std::to_string(typeCase));
    }
}
} // namespace omniruntime
