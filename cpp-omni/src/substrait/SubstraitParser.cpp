/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: print expression tree methods
 */

#include "SubstraitParser.h"
#include <string>
#include "google/protobuf/wrappers.pb.h"

namespace omniruntime {
std::vector<type::DataTypePtr> SubstraitParser::ParseNamedStruct(
    const ::substrait::NamedStruct &namedStruct, bool asLowerCase)
{
    // Note that "names" are not used.

    // Parse Struct.
    const auto &substraitStruct = namedStruct.struct_();
    const auto &substraitTypes = substraitStruct.types();
    std::vector<type::DataTypePtr> typeList;
    typeList.reserve(substraitTypes.size());
    for (const auto &type : substraitTypes) {
        typeList.emplace_back(ParseType(type, asLowerCase));
    }
    return typeList;
}
type::DataTypePtr SubstraitParser::ParseType(const ::substrait::Type &substraitType, bool asLowerCase)
{
    switch (substraitType.kind_case()) {
        case ::substrait::Type::KindCase::kBool:
            return type::BooleanType();
        case ::substrait::Type::KindCase::kI16:
            return type::ShortType();
        case ::substrait::Type::KindCase::kI32:
            return type::IntType();
        case ::substrait::Type::KindCase::kI64:
            return type::LongType();
        case ::substrait::Type::KindCase::kFp64:
            return type::DoubleType();
        case ::substrait::Type::KindCase::kString:
            return type::VarcharType();
        case ::substrait::Type::KindCase::kDate:
            return type::Date32Type();
        case ::substrait::Type::KindCase::kTimestamp:
            return type::TimestampType();
        case ::substrait::Type::KindCase::kDecimal: {
            auto precision = substraitType.decimal().precision();
            auto scale = substraitType.decimal().scale();
            if (precision <= MAX_PRECISION_64) {
                return type::Decimal64Type(precision, scale);
            }
            return type::Decimal128Type(precision, scale);
        }
        case ::substrait::Type::KindCase::kStruct: {
            const auto &substraitStruct = substraitType.struct_();
            const auto &structTypes = substraitStruct.types();
            std::vector<type::DataTypePtr> types;
            for (const auto &structType : structTypes) {
                types.emplace_back(ParseType(structType, asLowerCase));
            }
            return std::make_shared<type::RowType>(types);
        }
        default:
            OMNI_THROW("Substrait Error:", "Parsing for Substrait type not supported: {}", substraitType.DebugString());
    }
}

std::pair<SubstraitToOmniExprType, std::string> SubstraitParser::FindOmniFunction(
    const std::unordered_map<uint64_t, std::string> &functionMap, uint64_t id)
{
    std::string funcSpec = FindFunctionSpec(functionMap, id);
    std::string funcName = GetNameBeforeDelimiter(funcSpec);
    return MapToOmniFunction(funcName);
}

std::string SubstraitParser::FindFunctionSpec(const std::unordered_map<uint64_t, std::string> &functionMap, uint64_t id)
{
    auto x = functionMap.find(id);
    if (x == functionMap.end()) {
        OMNI_THROW("Could not find function id {} in function map.", std::to_string(id));
    }
    return x->second;
}

std::string SubstraitParser::GetNameBeforeDelimiter(const std::string &signature, const std::string &delimiter)
{
    std::size_t pos = signature.find(delimiter);
    if (pos == std::string::npos) {
        return signature;
    }
    return signature.substr(0, pos);
}

std::vector<std::string> SubstraitParser::GetSubFunctionTypes(const std::string &substraitFunction)
{
    // Get the position of ":" in the function name.
    size_t pos = substraitFunction.find(':');
    // Get the parameter types.
    std::vector<std::string> types;
    if (pos == std::string::npos || pos == substraitFunction.size() - 1) {
        return types;
    }
    // Extract input types with delimiter.
    for (;;) {
        const size_t endPos = substraitFunction.find('_', pos + 1);
        if (endPos == std::string::npos) {
            std::string typeName = substraitFunction.substr(pos + 1);
            if (typeName != "opt" && typeName != "req") {
                types.emplace_back(typeName);
            }
            break;
        }

        const std::string typeName = substraitFunction.substr(pos + 1, endPos - pos - 1);
        if (typeName != "opt" && typeName != "req") {
            types.emplace_back(typeName);
        }
        pos = endPos;
    }
    return types;
}

bool SubstraitParser::ParseReferenceSegment(
    const ::substrait::Expression::ReferenceSegment &refSegment, uint32_t &fieldIndex)
{}

std::string SubstraitParser::MakeNodeName(int nodeId, int colIdx)
{
    std::string result = "n" + std::to_string(nodeId) + "_" + std::to_string(colIdx);
    return result;
}

std::pair<SubstraitToOmniExprType, std::string> SubstraitParser::MapToOmniFunction(const std::string &substraitFunction)
{
    auto it = substraitOmniFunctionMap.find(substraitFunction);
    if (it != substraitOmniFunctionMap.end()) {
        return it->second;
    }
    throw omniruntime::exception::OmniException(
        SUBSTRAIT_PARSE_ERROR, "Could not find function in function map:" + substraitFunction);
}

bool SubstraitParser::ConfigSetInOptimization(
    const ::substrait::extensions::AdvancedExtension &extension, const std::string &config)
{
    if (extension.has_optimization()) {
        google::protobuf::StringValue msg;
        extension.optimization().UnpackTo(&msg);
        std::size_t pos = msg.value().find(config);
        if ((pos != std::string::npos) && (msg.value().substr(pos + config.size(), 1) == "1")) {
            return true;
        }
    }
    return false;
}

template <typename T>
T SubstraitParser::GetLiteralValue(const ::substrait::Expression::Literal & /* literal */)
{
    OMNI_THROW("Substrait Error:", "1");
}

template <>
int8_t SubstraitParser::GetLiteralValue(const ::substrait::Expression::Literal &literal)
{
    return static_cast<int8_t>(literal.i8());
}

template <>
int16_t SubstraitParser::GetLiteralValue(const ::substrait::Expression::Literal &literal)
{
    return static_cast<int16_t>(literal.i16());
}

template <>
int32_t SubstraitParser::GetLiteralValue(const ::substrait::Expression::Literal &literal)
{
    if (literal.has_date()) {
        return int32_t(literal.date());
    }
    return literal.i32();
}

template <>
int64_t SubstraitParser::GetLiteralValue(const ::substrait::Expression::Literal &literal)
{
    if (literal.has_decimal()) {
        auto decimal = literal.decimal().value();
        type::int128_t decimalValue;
        memcpy(&decimalValue, decimal.c_str(), 16);
        return static_cast<int64_t>(decimalValue);
    }
    return literal.i64();
}

template <>
type::int128_t SubstraitParser::GetLiteralValue(const ::substrait::Expression::Literal &literal)
{
    auto decimal = literal.decimal().value();
    type::int128_t decimalValue;
    memcpy(&decimalValue, decimal.c_str(), 16);
    // TODO:
    return 1;
}

template <>
double SubstraitParser::GetLiteralValue(const ::substrait::Expression::Literal &literal)
{
    return literal.fp64();
}

template <>
float SubstraitParser::GetLiteralValue(const ::substrait::Expression::Literal &literal)
{
    return literal.fp32();
}

template <>
bool SubstraitParser::GetLiteralValue(const ::substrait::Expression::Literal &literal)
{
    return literal.boolean();
}

template <>
std::string SubstraitParser::GetLiteralValue(const ::substrait::Expression::Literal &literal)
{
    if (literal.has_string()) {
        return literal.string();
    } else if (literal.has_var_char()) {
        return literal.var_char().value();
    } else if (literal.has_binary()) {
        return literal.binary();
    } else {
        OMNI_THROW("Substrait Error:", "Unexpected string or binary literal");
    }
}

type::DataTypesPtr SubstraitParser::ParseStructType(const ::substrait::Type &substraitType)
{
    const auto &substraitStruct = substraitType.struct_();
    const auto &structTypes = substraitStruct.types();
    std::vector<type::DataTypePtr> types;
    for (int i = 0; i < structTypes.size(); i++) {
        types.emplace_back(ParseType(structTypes[i]));
    }
    return std::make_shared<type::DataTypes>(std::move(types));
}

op::FunctionType SubstraitParser::ParseFunctionType(
    const std::string &funcName, std::vector<substrait::Expression> &expressionNodes, bool isMergeCount)
{
    if (funcName.empty()) {
        OMNI_THROW("Substrait Error:", "parse function type failed as func name is NULL");
    }

    if (funcName == "sum") {
        return op::OMNI_AGGREGATION_TYPE_SUM;
    } else if (funcName == "max") {
        return op::OMNI_AGGREGATION_TYPE_MAX;
    } else if (funcName == "avg") {
        return op::OMNI_AGGREGATION_TYPE_AVG;
    } else if (funcName == "min") {
        return op::OMNI_AGGREGATION_TYPE_MIN;
    } else if (funcName == "count") {
        if (expressionNodes.empty()) {
            OMNI_THROW("Substrait Error:", "Unsupported aggregate function without expressions", funcName);
        }
        substrait::Expression firstExpNode = expressionNodes.front();
        if (firstExpNode.rex_type_case() == ::substrait::Expression::RexTypeCase::kLiteral) {
            if (isMergeCount) {
                return op::OMNI_AGGREGATION_TYPE_COUNT_COLUMN;
            }
            return op::OMNI_AGGREGATION_TYPE_COUNT_ALL;
        } else {
            return op::OMNI_AGGREGATION_TYPE_COUNT_COLUMN;
        }
    } else if (funcName == "first_ignore_null") {
        return op::OMNI_AGGREGATION_TYPE_FIRST_IGNORENULL;
    } else if (funcName == "first") {
        return op::OMNI_AGGREGATION_TYPE_FIRST_INCLUDENULL;
    } else if (funcName == "rank") {
        return op::OMNI_WINDOW_TYPE_RANK;
    } else if (funcName == "row_number") {
        return op::OMNI_WINDOW_TYPE_ROW_NUMBER;
    } else {
        OMNI_THROW("Substrait Error:", "Unsupported aggregate or window function: {}", funcName);
    }
}

std::unordered_map<std::string, std::pair<SubstraitToOmniExprType, std::string>>
SubstraitParser::substraitOmniFunctionMap = {
    {"is_not_null", {IS_NOT_NULL_OMNI_EXPR_TYPE, "IS_NOT_NULL"}},
    {"is_null", {IS_NULL_OMNI_EXPR_TYPE, "IS_NULL"}},
    {"not", {UNARY_OMNI_EXPR_TYPE, "NOT"}},
    {"not_equal", {BINARY_OMNI_EXPR_TYPE, "NOT_EQUAL"}},
    {"add", {BINARY_OMNI_EXPR_TYPE, "ADD"}},
    {"subtract", {BINARY_OMNI_EXPR_TYPE, "SUBTRACT"}},
    {"multiply", {BINARY_OMNI_EXPR_TYPE, "MULTIPLY"}},
    {"divide", {BINARY_OMNI_EXPR_TYPE, "DIVIDE"}},
    {"and", {BINARY_OMNI_EXPR_TYPE, "AND"}},
    {"gt", {BINARY_OMNI_EXPR_TYPE, "GREATER_THAN"}},
    {"gte", {BINARY_OMNI_EXPR_TYPE, "GREATER_THAN_OR_EQUAL"}},
    {"lt", {BINARY_OMNI_EXPR_TYPE, "LESS_THAN"}},
    {"lte", {BINARY_OMNI_EXPR_TYPE, "LESS_THAN_OR_EQUAL"}},
    {"equal", {BINARY_OMNI_EXPR_TYPE, "EQUAL"}},
    {"or", {BINARY_OMNI_EXPR_TYPE, "OR"}},
    {"lower", {FUNCTION_OMNI_EXPR_TYPE, "lower"}},
    {"upper", {FUNCTION_OMNI_EXPR_TYPE, "upper"}},
    {"char_length", {FUNCTION_OMNI_EXPR_TYPE, "length"}},
    {"replace", {FUNCTION_OMNI_EXPR_TYPE, "replace"}},
    {"substring", {FUNCTION_OMNI_EXPR_TYPE, "substr"}},
    {"cast", {FUNCTION_OMNI_EXPR_TYPE, "CAST"}},
    {"abs", {FUNCTION_OMNI_EXPR_TYPE, "abs"}},
    {"round", {FUNCTION_OMNI_EXPR_TYPE, "round"}},
    {"rlike", {FUNCTION_OMNI_EXPR_TYPE, "RLike"}},
    {"md5", {FUNCTION_OMNI_EXPR_TYPE, "Md5"}},
    {"concat", {FUNCTION_OMNI_EXPR_TYPE, "concat"}},
    {"xxhash64", {FUNCTION_OMNI_EXPR_TYPE, "xxhash64"}},
    {"starts_with", {FUNCTION_OMNI_EXPR_TYPE, "StartsWith"}},
    {"ends_with", {FUNCTION_OMNI_EXPR_TYPE, "EndsWith"}},
    {"unscaled_value", {FUNCTION_OMNI_EXPR_TYPE, "UnscaledValue"}},
    {"coalesce", {COALESCE_OMNI_EXPR_TYPE, "COALESCE"}},
    {"modulus", {BINARY_OMNI_EXPR_TYPE, "MODULUS"}},
    {"strpos", {FUNCTION_OMNI_EXPR_TYPE, "instr"}},
    {"greatest", {FUNCTION_OMNI_EXPR_TYPE, "Greatest"}},
    {"contains", {FUNCTION_OMNI_EXPR_TYPE, "Contains"}},
    {"murmur3hash", {FUNCTION_OMNI_EXPR_TYPE, "mm3hash"}},
    {"rank", {FUNCTION_OMNI_EXPR_TYPE, "rank"}},
    {"row_number", {FUNCTION_OMNI_EXPR_TYPE, "row_number"}},
    {"count", {FUNCTION_OMNI_EXPR_TYPE, "count"}},
    {"sum", {FUNCTION_OMNI_EXPR_TYPE, "sum"}},
    {"min", {FUNCTION_OMNI_EXPR_TYPE, "min"}},
    {"max", {FUNCTION_OMNI_EXPR_TYPE, "max"}},
    {"avg", {FUNCTION_OMNI_EXPR_TYPE, "avg"}},
    {"first", {FUNCTION_OMNI_EXPR_TYPE, "first"}},
    {"first_ignore_null", {FUNCTION_OMNI_EXPR_TYPE, "first_ignore_null"}}};
} // namespace omniruntime
