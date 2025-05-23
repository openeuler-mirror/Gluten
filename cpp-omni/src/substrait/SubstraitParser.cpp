/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: print expression tree methods
 */

#include <fmt/core.h>
#include <fmt/format.h>
#include <string>
#include "google/protobuf/wrappers.pb.h"
#include "SubstraitParser.h"

namespace omniruntime {
std::vector<type::DataTypePtr> SubstraitParser::ParseNamedStruct(const ::substrait::NamedStruct &namedStruct,
    bool asLowerCase)
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

std::string SubstraitParser::getNameBeforeDelimiter(const std::string& signature, const std::string& delimiter) {
  std::size_t pos = signature.find(delimiter);
  if (pos == std::string::npos) {
    return signature;
  }
  return signature.substr(0, pos);
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

std::string SubstraitParser::FindOmniFunction(
    const std::unordered_map<uint64_t, std::string>& functionMap,
    uint64_t id) {
  std::string funcSpec = findFunctionSpec(functionMap, id);
  std::string funcName = getNameBeforeDelimiter(funcSpec);
  return mapToOmniFunction(funcName);
}

std::string SubstraitParser::FindFunctionSpec(const std::unordered_map<uint64_t, std::string> &functionMap,
    uint64_t id)
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

bool SubstraitParser::ParseReferenceSegment(const ::substrait::Expression::ReferenceSegment &refSegment,
    uint32_t &fieldIndex) {}

std::string SubstraitParser::MakeNodeName(int nodeId, int colIdx)
{
    std::string result = "n" + std::to_string(nodeId) + "_" + std::to_string(colIdx);
    return result;
}

std::string SubstraitParser::MapToOmniFunction(const std::string &substraitFunction, bool isDecimal)
{
    auto it = substraitOmniFunctionMap.find(substraitFunction);
    if (isDecimal) {
        if (substraitFunction == "lt" || substraitFunction == "lte" || substraitFunction == "gt" ||
            substraitFunction == "gte" || substraitFunction == "equal") {
            return "decimal_" + it->second;
        }
        if (substraitFunction == "round") {
            return "decimal_round";
        }
    }
    if (it != substraitOmniFunctionMap.end()) {
        return it->second;
    }
    // If not finding the mapping from Substrait function name to Velox function
    // name, the original Substrait function name will be used.
    return substraitFunction;
}

bool SubstraitParser::ConfigSetInOptimization(
    const ::substrait::extensions::AdvancedExtension &extension,
    const std::string &config)
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

std::unordered_map<std::string, std::string> SubstraitParser::substraitOmniFunctionMap = {
    {"is_not_null", "isnotnull"},
    {"is_null", "isnull"},
    {"equal", "equalto"},
    {"equal_null_safe", "equalnullsafe"},
    {"lt", "lessthan"},
    {"lte", "lessthanorequal"},
    {"gt", "greaterthan"},
    {"gte", "greaterthanorequal"},
    {"not_equal", "notequalto"},
    {"char_length", "length"},
    {"strpos", "instr"},
    {"ends_with", "endswith"},
    {"starts_with", "startswith"},
    {"bit_or", "bitwise_or_agg"},
    {"bit_and", "bitwise_and_agg"},
    {"murmur3hash", "hash_with_seed"},
    {"xxhash64", "xxhash64_with_seed"},
    {"modulus", "remainder"},
    {"date_format", "format_datetime"},
    {"negative", "unaryminus"}
};
}
