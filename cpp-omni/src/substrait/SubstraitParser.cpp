/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: print expression tree methods
 */

#include <string>
#include "SubstraitParser.h"

namespace omniruntime
{
std::vector<type::DataTypePtr> SubstraitParser::ParseNamedStruct(const ::substrait::NamedStruct &namedStruct,
    bool asLowerCase)
{
    // Note that "names" are not used.

    // Parse Struct.
    const auto &substraitStruct = namedStruct.struct_();
    const auto &substraitTypes = substraitStruct.types();
    std::vector<type::DataTypePtr> typeList;
    typeList.reserve(substraitTypes.size());
    for (const auto &type: substraitTypes) {
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
            if (precision < 17) {
                return type::Decimal64Type(precision, scale);
            }
            return type::Decimal128Type(precision, scale);
        }
        default:
            OMNI_THROW("Substrait Error:", "Parsing for Substrait type not supported: {}", substraitType.DebugString());
    }
}

bool SubstraitParser::ParseReferenceSegment(const ::substrait::Expression::ReferenceSegment &refSegment,
    uint32_t &fieldIndex) {}

std::string SubstraitParser::MakeNodeName(int nodeId, int colIdx) {
    std::string result = "n" + std::to_string(nodeId) + "_" + std::to_string(colIdx);
    return result;
}

template<typename T>
T SubstraitParser::GetLiteralValue(const ::substrait::Expression::Literal & /* literal */)
{
    OMNI_THROW("Substrait Error:", "1");
}

template<>
int8_t SubstraitParser::GetLiteralValue(const ::substrait::Expression::Literal &literal)
{
    return static_cast<int8_t>(literal.i8());
}

template<>
int16_t SubstraitParser::GetLiteralValue(const ::substrait::Expression::Literal &literal)
{
    return static_cast<int16_t>(literal.i16());
}

template<>
int32_t SubstraitParser::GetLiteralValue(const ::substrait::Expression::Literal &literal)
{
    if (literal.has_date()) {
        return int32_t(literal.date());
    }
    return literal.i32();
}

template<>
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

template<>
type::int128_t SubstraitParser::GetLiteralValue(const ::substrait::Expression::Literal &literal)
{
    auto decimal = literal.decimal().value();
    type::int128_t decimalValue;
    memcpy(&decimalValue, decimal.c_str(), 16);
    // TODO:
    return 1;
}

template<>
double SubstraitParser::GetLiteralValue(const ::substrait::Expression::Literal &literal)
{
    return literal.fp64();
}

template<>
float SubstraitParser::GetLiteralValue(const ::substrait::Expression::Literal &literal)
{
    return literal.fp32();
}

template<>
bool SubstraitParser::GetLiteralValue(const ::substrait::Expression::Literal &literal)
{
    return literal.boolean();
}

template<>
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
}
