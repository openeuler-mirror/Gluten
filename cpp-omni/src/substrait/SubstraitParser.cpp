/*
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

#include "SubstraitParser.h"
#include <string>
#include "google/protobuf/wrappers.pb.h"

namespace omniruntime {
const std::string HIVE_SIMPLE_TAG = "HiveSimpleUDF#";

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

type::DataTypePtr SubstraitParser::ParseKStructType(const ::substrait::Type &substraitType, bool asLowerCase, bool isNest)
{
    const auto& substraitStruct = substraitType.struct_();
    const auto& structTypes = substraitStruct.types();
    const auto& structNames = substraitStruct.names();
    std::vector<type::DataTypePtr> types;
    std::vector<std::string> names;
    types.reserve(structTypes.size());
    names.reserve(structTypes.size());
    for (int i = 0; i < structTypes.size(); i++) {
        types.emplace_back(ParseType(structTypes[i], asLowerCase));
        // Use field name from Substrait if available, otherwise use auto-generated name
        if (i < structNames.size() && !structNames[i].empty()) {
            names.emplace_back(structNames[i]);
        } else {
            names.emplace_back("field" + std::to_string(i));
        }
    }
    return std::make_shared<type::RowType>(std::move(types), std::move(names));
}

type::DataTypePtr SubstraitParser::ParseType(const ::substrait::Type &substraitType, bool asLowerCase, bool isNest)
{
    switch (substraitType.kind_case()) {
        case ::substrait::Type::KindCase::kNothing:
        case ::substrait::Type::KindCase::kBool:
            return type::BooleanType();
        case ::substrait::Type::KindCase::kI8:
            return type::ByteType();
        case ::substrait::Type::KindCase::kI16:
            return type::ShortType();
        case ::substrait::Type::KindCase::kI32:
            return type::IntType();
        case ::substrait::Type::KindCase::kI64:
            return type::LongType();
        case ::substrait::Type::KindCase::kFp64:
            return type::DoubleType();
        case ::substrait::Type::KindCase::kFp32:
            return type::FloatType();
        case ::substrait::Type::KindCase::kString:
            return type::VarcharType();
        case ::substrait::Type::KindCase::kDate:
            return type::Date32Type();
        case ::substrait::Type::KindCase::kTimestamp:
            return type::TimestampType();
        case ::substrait::Type::KindCase::kBinary:
            return type::VarBinaryType();
        case ::substrait::Type::KindCase::kDecimal: {
            auto precision = substraitType.decimal().precision();
            auto scale = substraitType.decimal().scale();
            if (precision <= MAX_PRECISION_64) {
                return type::Decimal64Type(precision, scale);
            }
            return type::Decimal128Type(precision, scale);
        }
        case ::substrait::Type::KindCase::kStruct: {
            return ParseKStructType(substraitType, asLowerCase, isNest);
        }
        case ::substrait::Type::KindCase::kList: {
            const auto& fieldType = substraitType.list().type();
            return std::make_shared<type::ArrayType>(ParseType(fieldType, asLowerCase));
        }
        case ::substrait::Type::KindCase::kMap: {
            const auto& sMap = substraitType.map();
            const auto& keyType = sMap.key();
            const auto& valueType = sMap.value();
            return std::make_shared<type::MapType>(ParseType(keyType, asLowerCase), ParseType(valueType, asLowerCase));
        }
        default:
            OMNI_THROW("Substrait Error:", "Parsing for Substrait type not supported: {}", substraitType.DebugString());
    }
}

void SubstraitParser::ParseColumnTypes(
    const ::substrait::NamedStruct& namedStruct,
    std::vector<ColumnType>& columnTypes)
{
    const auto& columnsTypes = namedStruct.column_types();
    if (columnsTypes.size() == 0) {
        // Regard all columns as regular columns.
        columnTypes.resize(namedStruct.names().size(), ColumnType::kRegular);
        return;
    }

    columnTypes.reserve(columnsTypes.size());
    for (const auto& columnType : columnsTypes) {
        switch (columnType) {
            case ::substrait::NamedStruct::NORMAL_COL:
                columnTypes.push_back(ColumnType::kRegular);
                break;
            case ::substrait::NamedStruct::PARTITION_COL:
                columnTypes.push_back(ColumnType::kPartitionKey);
                break;
            case ::substrait::NamedStruct::METADATA_COL:
                columnTypes.push_back(ColumnType::kSynthesized);
                break;
            case ::substrait::NamedStruct::ROWINDEX_COL:
                columnTypes.push_back(ColumnType::kRowIndex);
                break;
            default:
                std::cout << "Thread.currentThread() parseColumnTypes" ;
        }
    }
    return;
}

std::pair<SubstraitToOmniExprType, std::string> SubstraitParser::FindOmniFunction(
    const std::unordered_map<uint64_t, std::string> &functionMap, uint64_t id)
{
    std::string funcSpec = FindFunctionSpec(functionMap, id);
    std::string funcName = GetNameBeforeDelimiter(funcSpec);
    if (funcName.find(HIVE_SIMPLE_TAG) == 0) {
        return {HIVE_UDF_FUNCTION_OMNI_EXPR_TYPE, funcName.erase(0, HIVE_SIMPLE_TAG.length())};
    }
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
        const auto& optimization = extension.optimization();
        if (!optimization.Is<google::protobuf::StringValue>()) {
            return false;
        }
        google::protobuf::StringValue msg;
        extension.optimization().UnpackTo(&msg);
        std::size_t pos = msg.value().find(config);
        if ((pos != std::string::npos) && (msg.value().substr(pos + config.size(), 1) == "1")) {
            return true;
        }
    }
    return false;
}

bool SubstraitParser::ConfigExistInOptimization(
    const ::substrait::extensions::AdvancedExtension &extension, const std::string &config)
{
    if (extension.has_optimization()) {
        google::protobuf::StringValue msg;
        extension.optimization().UnpackTo(&msg);
        std::size_t pos = msg.value().find(config);
        if (pos != std::string::npos) {
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

void SubstraitParser::AddStructDataType(
    const ::substrait::Type &substraitType, std::vector<omniruntime::type::DataTypePtr> &outputDataTypes)
{
    const auto &substraitStruct = substraitType.struct_();
    const auto &structTypes = substraitStruct.types();
    std::vector<type::DataTypePtr> types;
    for (int i = 0; i < structTypes.size(); i++) {
        outputDataTypes.emplace_back(ParseType(structTypes[i]));
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
    } else if (funcName == "try_sum") {
        return op::OMNI_AGGREGATION_TYPE_TRY_SUM;
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
    } else if (funcName == "stddev_samp") {
        return op::OMNI_AGGREGATION_TYPE_SAMP;
    } else if (funcName == "stddev_pop") {
        return op::OMNI_AGGREGATION_TYPE_STD_POP;
    } else if (funcName == "var_samp") {
        return op::OMNI_AGGREGATION_TYPE_VAR_SAMP;
    } else if (funcName == "var_pop") {
        return op::OMNI_AGGREGATION_TYPE_VAR_POP;
    } else if (funcName == "first") {
        return op::OMNI_AGGREGATION_TYPE_FIRST_INCLUDENULL;
    } else if (funcName == "last_ignore_null") {
        return op::OMNI_AGGREGATION_TYPE_LAST_IGNORENULL;
    } else if (funcName == "last") {
        return op::OMNI_AGGREGATION_TYPE_LAST_INCLUDENULL;
    } else if (funcName == "rank") {
        return op::OMNI_WINDOW_TYPE_RANK;
    } else if (funcName == "percent_rank") {
        return op::OMNI_WINDOW_TYPE_PERCENT_RANK;
    } else if (funcName == "cume_dist") {
        return op::OMNI_WINDOW_TYPE_CUME_DIST;
    } else if (funcName == "row_number") {
        return op::OMNI_WINDOW_TYPE_ROW_NUMBER;
    } else if (funcName == "lead") {
        return op::OMNI_WINDOW_TYPE_LEAD;
    } else if (funcName == "lag") {
        return op::OMNI_WINDOW_TYPE_LAG;
    } else if (funcName == "nth_value") {
        return op::OMNI_WINDOW_TYPE_NTH_VALUE;
    } else if (funcName == "ntile") {
        return op::OMNI_WINDOW_TYPE_NTILE;
    } else if (funcName == "dense_rank") {
        return op::OMNI_WINDOW_TYPE_DENSE_RANK;
    } else if (funcName == "bloom_filter_agg") {
        return op::OMNI_AGGREGATION_TYPE_BLOOM_FILTER;
    } else if (funcName == "bit_and") {
        return op::OMNI_AGGREGATION_TYPE_BIT_AND;
    } else if (funcName == "bit_or") {
        return op::OMNI_AGGREGATION_TYPE_BIT_OR;
    } else if (funcName == "bit_xor") {
        return op::OMNI_AGGREGATION_TYPE_BIT_XOR;
    } else if (funcName == "corr") {
 	    return op::OMNI_AGGREGATION_TYPE_CORR;
 	} else if (funcName == "covar_pop") {
 	    return op::OMNI_AGGREGATION_TYPE_COVAR_POP;
 	} else if (funcName == "covar_samp") {
 	   return op::OMNI_AGGREGATION_TYPE_COVAR_SAMP;
 	} else if (funcName == "regr_count") {
        return op::OMNI_AGGREGATION_TYPE_REGR_COUNT;
    } else if (funcName == "regr_intercept") {
        return op::OMNI_AGGREGATION_TYPE_REGR_INTERCEPT;
    } else if (funcName == "regr_r2") {
        return op::OMNI_AGGREGATION_TYPE_REGR_R2;
    } else if (funcName == "regr_slope") {
        return op::OMNI_AGGREGATION_TYPE_REGR_SLOPE;
    } else if (funcName == "regr_sxx") {
        return op::OMNI_AGGREGATION_TYPE_REGR_SXX;
    } else if (funcName == "regr_sxy") {
        return op::OMNI_AGGREGATION_TYPE_REGR_SXY;
    } else if (funcName == "regr_syy") {
        return op::OMNI_AGGREGATION_TYPE_REGR_SYY;
    } else if (funcName == "regr_avgx") {
        return op::OMNI_AGGREGATION_TYPE_REGR_AVGX;
    } else if (funcName == "regr_avgy") {
        return op::OMNI_AGGREGATION_TYPE_REGR_AVGY;
    } else if (funcName == "regr_replacement") {
        return op::OMNI_AGGREGATION_TYPE_REGR_REPLACEMENT;
 	} else if (funcName == "min_by") {
        return op::OMNI_AGGREGATION_TYPE_MIN_BY;
    } else if (funcName == "max_by") {
        return op::OMNI_AGGREGATION_TYPE_MAX_BY;
    } else if (funcName == "approx_distinct") {
        return op::OMNI_AGGREGATION_TYPE_APPROX_COUNT_DISTINCT;
    } else if (funcName == "collect_set") {
        return op::OMNI_AGGREGATION_TYPE_COLLECT_SET;
    } else if (funcName == "collect_list") {
        return op::OMNI_AGGREGATION_TYPE_COLLECT_LIST;
    } else if (funcName == "skewness") {
        return op::OMNI_AGGREGATION_TYPE_SKEWNESS;
    } else if (funcName == "kurtosis") {
        return op::OMNI_AGGREGATION_TYPE_KURTOSIS;
    } else if (funcName == "approx_percentile") {
        return op::OMNI_AGGREGATION_TYPE_APPROX_PERCENTILE;
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
    {"neq", {BINARY_OMNI_EXPR_TYPE, "NOT_EQUAL"}},
    {"add", {BINARY_OMNI_EXPR_TYPE, "ADD"}},
    {"subtract", {BINARY_OMNI_EXPR_TYPE, "SUBTRACT"}},
    {"multiply", {BINARY_OMNI_EXPR_TYPE, "MULTIPLY"}},
    {"divide", {BINARY_OMNI_EXPR_TYPE, "DIVIDE"}},
    {"try_add", {BINARY_OMNI_EXPR_TYPE, "TRY_ADD"}},
    {"try_subtract", {BINARY_OMNI_EXPR_TYPE, "TRY_SUBTRACT"}},
    {"try_multiply", {BINARY_OMNI_EXPR_TYPE, "TRY_MULTIPLY"}},
    {"try_divide", {BINARY_OMNI_EXPR_TYPE, "TRY_DIVIDE"}},
    {"and", {BINARY_OMNI_EXPR_TYPE, "AND"}},
    {"gt", {BINARY_OMNI_EXPR_TYPE, "GREATER_THAN"}},
    {"gte", {BINARY_OMNI_EXPR_TYPE, "GREATER_THAN_OR_EQUAL"}},
    {"lt", {BINARY_OMNI_EXPR_TYPE, "LESS_THAN"}},
    {"lte", {BINARY_OMNI_EXPR_TYPE, "LESS_THAN_OR_EQUAL"}},
    {"equal", {BINARY_OMNI_EXPR_TYPE, "EQUAL"}},
    {"eq", {BINARY_OMNI_EXPR_TYPE, "EQUAL"}},
    {"equal_null_safe", {FUNCTION_OMNI_EXPR_TYPE, "equal_null_safe"}},
    {"or", {BINARY_OMNI_EXPR_TYPE, "OR"}},
    {"checked_substract", {BINARY_OMNI_EXPR_TYPE, "TRY_SUBTRACT"}},
    {"lower", {FUNCTION_OMNI_EXPR_TYPE, "lower"}},
    {"upper", {FUNCTION_OMNI_EXPR_TYPE, "upper"}},
    {"char_length", {FUNCTION_OMNI_EXPR_TYPE, "length"}},
    {"character_length", {FUNCTION_OMNI_EXPR_TYPE, "length"}},
    {"replace", {FUNCTION_OMNI_EXPR_TYPE, "replace"}},
    {"substring", {FUNCTION_OMNI_EXPR_TYPE, "substr"}},
    {"cast", {FUNCTION_OMNI_EXPR_TYPE, "CAST"}},
    {"abs", {FUNCTION_OMNI_EXPR_TYPE, "abs"}},
    {"array", {FUNCTION_OMNI_EXPR_TYPE, "array"}},
    {"get_array_item", {FUNCTION_OMNI_EXPR_TYPE, "get_array_item"}},
    {"round", {FUNCTION_OMNI_EXPR_TYPE, "round"}},
    {"rlike", {FUNCTION_OMNI_EXPR_TYPE, "RLike"}},
    {"like", {FUNCTION_OMNI_EXPR_TYPE, "LIKE"}},
    {"size", {FUNCTION_OMNI_EXPR_TYPE, "size"}},
    {"element_at", {FUNCTION_OMNI_EXPR_TYPE, "element_at"}},
    {"split", {FUNCTION_OMNI_EXPR_TYPE, "split"}},
    {"slice", {FUNCTION_OMNI_EXPR_TYPE, "slice"}},
    {"hive_string_string", {FUNCTION_OMNI_EXPR_TYPE, "hive_string_string"}},
    {"exp", {FUNCTION_OMNI_EXPR_TYPE, "exp"}},
    {"md5", {FUNCTION_OMNI_EXPR_TYPE, "Md5"}},
    {"concat", {FUNCTION_OMNI_EXPR_TYPE, "concat"}},
    {"concat_ws", {FUNCTION_OMNI_EXPR_TYPE, "concat_ws"}},
    {"reverse", {FUNCTION_OMNI_EXPR_TYPE, "reverse"}},
    {"repeat", {FUNCTION_OMNI_EXPR_TYPE, "repeat"}},
    {"xxhash64", {FUNCTION_OMNI_EXPR_TYPE, "xxhash64"}},
    {"starts_with", {FUNCTION_OMNI_EXPR_TYPE, "StartsWith"}},
    {"ends_with", {FUNCTION_OMNI_EXPR_TYPE, "EndsWith"}},
    {"unscaled_value", {FUNCTION_OMNI_EXPR_TYPE, "UnscaledValue"}},
    {"coalesce", {COALESCE_OMNI_EXPR_TYPE, "COALESCE"}},
    {"modulus", {BINARY_OMNI_EXPR_TYPE, "MODULUS"}},
    {"strpos", {FUNCTION_OMNI_EXPR_TYPE, "instr"}},
    {"greatest", {FUNCTION_OMNI_EXPR_TYPE, "Greatest"}},
    {"least", {FUNCTION_OMNI_EXPR_TYPE, "Least"}},
    {"StaticInvokeCharTypeWriteSideCheck", {FUNCTION_OMNI_EXPR_TYPE, "StaticInvokeCharTypeWriteSideCheck"}},
    {"StaticInvokeVarcharTypeWriteSideCheck", {FUNCTION_OMNI_EXPR_TYPE, "StaticInvokeVarcharTypeWriteSideCheck"}},
    {"StaticInvokeCharReadPadding", {FUNCTION_OMNI_EXPR_TYPE, "StaticInvokeCharReadPadding"}},
    {"contains", {FUNCTION_OMNI_EXPR_TYPE, "Contains"}},
    {"locate", {FUNCTION_OMNI_EXPR_TYPE, "locate"}},
    {"position", {FUNCTION_OMNI_EXPR_TYPE, "position"}},
    {"ascii", {FUNCTION_OMNI_EXPR_TYPE, "ascii"}},
    {"chr", {FUNCTION_OMNI_EXPR_TYPE, "chr"}},
    {"char", {FUNCTION_OMNI_EXPR_TYPE, "char"}},
    {"base64", {FUNCTION_OMNI_EXPR_TYPE, "base64"}},
    {"unbase64", {FUNCTION_OMNI_EXPR_TYPE, "unbase64"}},
    {"murmur3hash", {FUNCTION_OMNI_EXPR_TYPE, "mm3hash"}},
    {"rank", {FUNCTION_OMNI_EXPR_TYPE, "rank"}},
    {"percent_rank", {FUNCTION_OMNI_EXPR_TYPE, "percent_rank"}},
    {"cume_dist", {FUNCTION_OMNI_EXPR_TYPE, "cume_dist"}},
    {"row_number", {FUNCTION_OMNI_EXPR_TYPE, "row_number"}},
    {"lead", {FUNCTION_OMNI_EXPR_TYPE, "lead"}},
    {"lag", {FUNCTION_OMNI_EXPR_TYPE, "lag"}},
    {"nth_value", {FUNCTION_OMNI_EXPR_TYPE, "nth_value"}},
    {"ntile", {FUNCTION_OMNI_EXPR_TYPE, "ntile"}},
    {"dense_rank", {FUNCTION_OMNI_EXPR_TYPE, "dense_rank"}},
    {"count", {FUNCTION_OMNI_EXPR_TYPE, "count"}},
    {"sum", {FUNCTION_OMNI_EXPR_TYPE, "sum"}},
    {"try_sum", {FUNCTION_OMNI_EXPR_TYPE, "try_sum"}},
    {"min", {FUNCTION_OMNI_EXPR_TYPE, "min"}},
    {"max", {FUNCTION_OMNI_EXPR_TYPE, "max"}},
    {"avg", {FUNCTION_OMNI_EXPR_TYPE, "avg"}},
    {"power", {FUNCTION_OMNI_EXPR_TYPE, "power"}},
    {"first", {FUNCTION_OMNI_EXPR_TYPE, "first"}},
    {"last", {FUNCTION_OMNI_EXPR_TYPE, "last"}},
    {"bloom_filter_agg", {FUNCTION_OMNI_EXPR_TYPE, "bloom_filter_agg"}},
    {"substring_index", {FUNCTION_OMNI_EXPR_TYPE, "substring_index"}},
    {"regexp_extract", {FUNCTION_OMNI_EXPR_TYPE, "regexp_extract"}},
    {"regexp_extract_all", {FUNCTION_OMNI_EXPR_TYPE, "regexp_extract_all"}},
    {"regexp_replace", {FUNCTION_OMNI_EXPR_TYPE, "regexp_replace"}},
    {"make_decimal", {FUNCTION_OMNI_EXPR_TYPE, "MakeDecimal"}},
    {"unix_timestamp", {FUNCTION_OMNI_EXPR_TYPE, "unix_timestamp"}},
    {"from_unixtime", {FUNCTION_OMNI_EXPR_TYPE, "from_unixtime"}},
    {"get_timestamp", {FUNCTION_OMNI_EXPR_TYPE, "get_timestamp"}},
    {"to_unix_timestamp", {FUNCTION_OMNI_EXPR_TYPE, "to_unix_timestamp"}},
    {"to_utc_timestamp", {FUNCTION_OMNI_EXPR_TYPE, "to_utc_timestamp"}},
    {"from_utc_timestamp", {FUNCTION_OMNI_EXPR_TYPE, "from_utc_timestamp"}},
    {"first_ignore_null", {FUNCTION_OMNI_EXPR_TYPE, "first_ignore_null"}},
    {"last_ignore_null", {FUNCTION_OMNI_EXPR_TYPE, "last_ignore_null"}},
    {"stddev_samp", {FUNCTION_OMNI_EXPR_TYPE, "stddev_samp"}},
    {"stddev_pop", {FUNCTION_OMNI_EXPR_TYPE, "stddev_pop"}},
    {"var_samp", {FUNCTION_OMNI_EXPR_TYPE, "var_samp"}},
    {"var_pop", {FUNCTION_OMNI_EXPR_TYPE, "var_pop"}},
    {"date_add", {FUNCTION_OMNI_EXPR_TYPE, "date_add"}},
    {"date_sub", {FUNCTION_OMNI_EXPR_TYPE, "date_sub"}},
    {"datediff", {FUNCTION_OMNI_EXPR_TYPE, "date_diff"}},
    {"date_format", {FUNCTION_OMNI_EXPR_TYPE, "DateFormat"}},
    {"add_months", {FUNCTION_OMNI_EXPR_TYPE, "add_months"}},
    {"months_between", {FUNCTION_OMNI_EXPR_TYPE, "months_between"}},
    {"trunc", {FUNCTION_OMNI_EXPR_TYPE, "trunc_date"}},
    {"date_trunc", {FUNCTION_OMNI_EXPR_TYPE, "date_trunc"}},
    {"extract", {FUNCTION_OMNI_EXPR_TYPE, "extract"}},
    {"hour", {FUNCTION_OMNI_EXPR_TYPE, "hour"}},
    {"minute", {FUNCTION_OMNI_EXPR_TYPE, "minute"}},
    {"second", {FUNCTION_OMNI_EXPR_TYPE, "second"}},
    {"month", {FUNCTION_OMNI_EXPR_TYPE, "month"}},
    {"quarter", {FUNCTION_OMNI_EXPR_TYPE, "quarter"}},
    {"year", {FUNCTION_OMNI_EXPR_TYPE, "year"}},
    {"day", {FUNCTION_OMNI_EXPR_TYPE, "day"}},
    {"dayofmonth", {FUNCTION_OMNI_EXPR_TYPE, "dayofmonth"}},
    {"dayofweek", {FUNCTION_OMNI_EXPR_TYPE, "dayofweek"}},
    {"dayofyear", {FUNCTION_OMNI_EXPR_TYPE, "dayofyear"}},
    {"next_day", {FUNCTION_OMNI_EXPR_TYPE, "next_day"}},
    {"week_of_year", {FUNCTION_OMNI_EXPR_TYPE, "week_of_year"}},
    {"weekday", {FUNCTION_OMNI_EXPR_TYPE, "weekday"}},
    {"year_of_week", {FUNCTION_OMNI_EXPR_TYPE, "year_of_week"}},
    {"trim", {FUNCTION_OMNI_EXPR_TYPE, "Trim"}},
    {"ltrim", {FUNCTION_OMNI_EXPR_TYPE, "LTrim"}},
    {"rtrim", {FUNCTION_OMNI_EXPR_TYPE, "RTrim"}},
    {"btrim", {FUNCTION_OMNI_EXPR_TYPE, "BTrim"}},
    {"overlay", {FUNCTION_OMNI_EXPR_TYPE, "overlay"}},
    {"floor", {FUNCTION_OMNI_EXPR_TYPE, "floor"}},
    {"empty2null", {FUNCTION_OMNI_EXPR_TYPE, "empty2null"}},
    {"get_json_object", {FUNCTION_OMNI_EXPR_TYPE, "GetJsonObject"}},
    {"to_json", {FUNCTION_OMNI_EXPR_TYPE, "to_json"}},
    {"json_object_keys", {FUNCTION_OMNI_EXPR_TYPE, "json_object_keys"}},
    {"json_array_length", {FUNCTION_OMNI_EXPR_TYPE, "json_array_length"}},
    {"from_json", {FUNCTION_OMNI_EXPR_TYPE, "from_json"}},
    {"might_contain", {FUNCTION_OMNI_EXPR_TYPE, "might_contain"}},
    {"bitwise_and", {FUNCTION_OMNI_EXPR_TYPE, "bitwise_and"}},
    {"bitwise_or", {FUNCTION_OMNI_EXPR_TYPE, "bitwise_or"}},
    {"bitwise_xor", {FUNCTION_OMNI_EXPR_TYPE, "bitwise_xor"}},
    {"bitwise_not", {FUNCTION_OMNI_EXPR_TYPE, "bitwise_not"}},
    {"bit_get", {FUNCTION_OMNI_EXPR_TYPE, "bit_get"}},
    {"shiftleft", {FUNCTION_OMNI_EXPR_TYPE, "shiftleft"}},
    {"shiftright", {FUNCTION_OMNI_EXPR_TYPE, "shiftright"}},
    {"negative", {FUNCTION_OMNI_EXPR_TYPE, "negative"}},
    {"min_by", {FUNCTION_OMNI_EXPR_TYPE, "min_by"}},
    {"max_by", {FUNCTION_OMNI_EXPR_TYPE, "max_by"}},
    {"approx_distinct", {FUNCTION_OMNI_EXPR_TYPE, "approx_distinct"}},
    {"skewness", {FUNCTION_OMNI_EXPR_TYPE, "skewness"}},
    {"kurtosis", {FUNCTION_OMNI_EXPR_TYPE, "kurtosis"}},
    {"zip_with", {FUNCTION_OMNI_EXPR_TYPE, "zip_with"}},
    {"transform", {FUNCTION_OMNI_EXPR_TYPE, "transform"}},
    {"transform_keys", {FUNCTION_OMNI_EXPR_TYPE, "transform_keys"}},
    {"transform_values", {FUNCTION_OMNI_EXPR_TYPE, "transform_values"}},
    {"map_filter", {FUNCTION_OMNI_EXPR_TYPE, "map_filter"}},
    {"forall", {FUNCTION_OMNI_EXPR_TYPE, "forall"}},
    {"exists", {FUNCTION_OMNI_EXPR_TYPE, "exists"}},
    {"filter", {FUNCTION_OMNI_EXPR_TYPE, "filter"}},
    {"lambdafunction", {FUNCTION_OMNI_EXPR_TYPE, "lambdafunction"}},
    {"namedlambdavariable", {FUNCTION_OMNI_EXPR_TYPE, "namedlambdavariable"}},
    {"map_from_arrays", {FUNCTION_OMNI_EXPR_TYPE, "map_from_arrays"}},
    {"map_zip_with", {FUNCTION_OMNI_EXPR_TYPE, "map_zip_with"}},
    {"acosh", {FUNCTION_OMNI_EXPR_TYPE, "acosh"}},
    {"acos", {FUNCTION_OMNI_EXPR_TYPE, "acos"}},
    {"map_keys", {FUNCTION_OMNI_EXPR_TYPE, "map_keys"}},
    {"map_values", {FUNCTION_OMNI_EXPR_TYPE, "map_values"}},
    {"asin", {FUNCTION_OMNI_EXPR_TYPE, "asin"}},
    {"asinh", {FUNCTION_OMNI_EXPR_TYPE, "asinh"}},
    {"atan", {FUNCTION_OMNI_EXPR_TYPE, "atan"}},
    {"atan2", {FUNCTION_OMNI_EXPR_TYPE, "atan2"}},
    {"cos", {FUNCTION_OMNI_EXPR_TYPE, "cos"}},
    {"cosh", {FUNCTION_OMNI_EXPR_TYPE, "cosh"}},
    {"bit_and", {FUNCTION_OMNI_EXPR_TYPE, "bit_and"}},
    {"bit_or", {FUNCTION_OMNI_EXPR_TYPE, "bit_or"}},
    {"bit_xor", {FUNCTION_OMNI_EXPR_TYPE, "bit_xor"}},
    {"corr", {FUNCTION_OMNI_EXPR_TYPE, "corr"}},
    {"covar_pop", {FUNCTION_OMNI_EXPR_TYPE, "covar_pop"}},
    {"covar_samp", {FUNCTION_OMNI_EXPR_TYPE, "covar_samp"}},
//     {"cbrt", {FUNCTION_OMNI_EXPR_TYPE, "cbrt"}},
    {"ceil", {FUNCTION_OMNI_EXPR_TYPE, "ceil"}},
    {"log1p", {FUNCTION_OMNI_EXPR_TYPE, "log1p"}},
    {"log2", {FUNCTION_OMNI_EXPR_TYPE, "log2"}},
    {"log10", {FUNCTION_OMNI_EXPR_TYPE, "log10"}},
    {"log", {FUNCTION_OMNI_EXPR_TYPE, "log"}},
    {"sign", {FUNCTION_OMNI_EXPR_TYPE, "sign"}},
    {"sinh", {FUNCTION_OMNI_EXPR_TYPE, "sinh"}},
    {"hypot", {FUNCTION_OMNI_EXPR_TYPE, "hypot"}},
    {"sqrt", {FUNCTION_OMNI_EXPR_TYPE, "sqrt"}},
    {"sec", {FUNCTION_OMNI_EXPR_TYPE, "sec"}},
    {"pmod", {FUNCTION_OMNI_EXPR_TYPE, "pmod"}},
    {"positive", {FUNCTION_OMNI_EXPR_TYPE, "positive"}},
    {"power", {FUNCTION_OMNI_EXPR_TYPE, "power"}},
    {"rint", {FUNCTION_OMNI_EXPR_TYPE, "rint"}},
    {"round", {FUNCTION_OMNI_EXPR_TYPE, "round"}},
    {"lpad", {FUNCTION_OMNI_EXPR_TYPE, "lpad"}},
    {"rpad", {FUNCTION_OMNI_EXPR_TYPE, "rpad"}},
    {"flatten", {FUNCTION_OMNI_EXPR_TYPE, "flatten"}},
    {"rand", {FUNCTION_OMNI_EXPR_TYPE, "rand"}},
    {"random", {FUNCTION_OMNI_EXPR_TYPE, "random"}},
    {"hex", {FUNCTION_OMNI_EXPR_TYPE, "hex"}},
    {"atanh", {FUNCTION_OMNI_EXPR_TYPE, "atanh"}},
    {"cot", {FUNCTION_OMNI_EXPR_TYPE, "cot"}},
 	{"csc", {FUNCTION_OMNI_EXPR_TYPE, "csc"}},
 	{"conv", {FUNCTION_OMNI_EXPR_TYPE, "conv"}},
    {"degrees", {FUNCTION_OMNI_EXPR_TYPE, "degrees"}},
    {"split_part", {FUNCTION_OMNI_EXPR_TYPE, "split_part"}},
    {"bit_count", {FUNCTION_OMNI_EXPR_TYPE, "bit_count"}},
    {"bit_length", {FUNCTION_OMNI_EXPR_TYPE, "bit_length"}},
    {"factorial", {FUNCTION_OMNI_EXPR_TYPE, "factorial"}},
    {"floor", {FUNCTION_OMNI_EXPR_TYPE, "floor"}},
    {"nanvl", {FUNCTION_OMNI_EXPR_TYPE, "nanvl"}},
    {"timestamp_micros", {FUNCTION_OMNI_EXPR_TYPE, "timestamp_micros"}},
    {"timestamp_millis", {FUNCTION_OMNI_EXPR_TYPE, "timestamp_millis"}},
    {"timestamp_seconds", {FUNCTION_OMNI_EXPR_TYPE, "timestamp_seconds"}},
    {"unix_seconds", {FUNCTION_OMNI_EXPR_TYPE, "unix_seconds"}},
    {"unix_millis", {FUNCTION_OMNI_EXPR_TYPE, "unix_millis"}},
    {"unix_micros", {FUNCTION_OMNI_EXPR_TYPE, "unix_micros"}},
    {"unix_date", {FUNCTION_OMNI_EXPR_TYPE, "unix_date"}},
    {"date_from_unix_date", {FUNCTION_OMNI_EXPR_TYPE, "date_from_unix_date"}},
    {"sort_array", {FUNCTION_OMNI_EXPR_TYPE, "sort_array"}},
    {"array_max", {FUNCTION_OMNI_EXPR_TYPE, "array_max"}},
    {"array_min", {FUNCTION_OMNI_EXPR_TYPE, "array_min"}},
    {"array_repeat", {FUNCTION_OMNI_EXPR_TYPE, "array_repeat"}},
    {"array_remove", {FUNCTION_OMNI_EXPR_TYPE, "array_remove"}},
    {"array_compact", {FUNCTION_OMNI_EXPR_TYPE, "array_compact"}},
    {"array_contains", {FUNCTION_OMNI_EXPR_TYPE, "array_contains"}},
    {"array_distinct", {FUNCTION_OMNI_EXPR_TYPE, "array_distinct"}},
    {"array_except", {FUNCTION_OMNI_EXPR_TYPE, "array_except"}},
    {"array_insert", {FUNCTION_OMNI_EXPR_TYPE, "array_insert"}},
    {"array_intersect", {FUNCTION_OMNI_EXPR_TYPE, "array_intersect"}},
    {"array_join", {FUNCTION_OMNI_EXPR_TYPE, "array_join"}},
    {"array_position", {FUNCTION_OMNI_EXPR_TYPE, "array_position"}},
    {"array_prepend", {FUNCTION_OMNI_EXPR_TYPE, "array_prepend"}},
    {"array_union", {FUNCTION_OMNI_EXPR_TYPE, "array_union"}},
    {"arrays_overlap", {FUNCTION_OMNI_EXPR_TYPE, "arrays_overlap"}},
    {"arrays_zip", {FUNCTION_OMNI_EXPR_TYPE, "arrays_zip"}},
    {"shuffle", {FUNCTION_OMNI_EXPR_TYPE, "shuffle"}},
    {"coalesce", {FUNCTION_OMNI_EXPR_TYPE, "coalesce"}},
    {"if", {FUNCTION_OMNI_EXPR_TYPE, "if"}},
    {"translate", {FUNCTION_OMNI_EXPR_TYPE, "translate"}},
    {"flatten", {FUNCTION_OMNI_EXPR_TYPE, "flatten"}},
    {"div", {FUNCTION_OMNI_EXPR_TYPE, "div"}},
    {"checked_div", {FUNCTION_OMNI_EXPR_TYPE, "div"}},
    {"expm1", {FUNCTION_OMNI_EXPR_TYPE, "expm1"}},
    {"unhex", {FUNCTION_OMNI_EXPR_TYPE, "unhex"}},
    {"width_bucket", {FUNCTION_OMNI_EXPR_TYPE, "width_bucket"}},
    {"spark_partition_id", {FUNCTION_OMNI_EXPR_TYPE, "spark_partition_id"}},
    {"uuid", {FUNCTION_OMNI_EXPR_TYPE, "uuid"}},
    {"collect_set", {FUNCTION_OMNI_EXPR_TYPE, "collect_set"}},
    {"cardinality", {FUNCTION_OMNI_EXPR_TYPE, "cardinality"}},
    {"crc32", {FUNCTION_OMNI_EXPR_TYPE, "crc32"}},
    {"isnan", {FUNCTION_OMNI_EXPR_TYPE, "isnan"}},
    {"row_constructor", {FUNCTION_OMNI_EXPR_TYPE, "named_struct"}},
    {"named_struct", {FUNCTION_OMNI_EXPR_TYPE, "named_struct"}},
    {"collect_list", {FUNCTION_OMNI_EXPR_TYPE, "collect_list"}},
    {"make_date", {FUNCTION_OMNI_EXPR_TYPE, "make_date"}},
    {"make_timestamp", {FUNCTION_OMNI_EXPR_TYPE, "make_timestamp"}},
    {"last_day", {FUNCTION_OMNI_EXPR_TYPE, "last_day"}},
    {"approx_percentile", {FUNCTION_OMNI_EXPR_TYPE, "approx_percentile"}},
    {"str_to_map", {FUNCTION_OMNI_EXPR_TYPE, "str_to_map"}},
    {"map_entries", {FUNCTION_OMNI_EXPR_TYPE, "map_entries"}},
    {"map_concat", {FUNCTION_OMNI_EXPR_TYPE, "map_concat"}},
    {"map", {FUNCTION_OMNI_EXPR_TYPE, "map"}},
    {"find_in_set", {FUNCTION_OMNI_EXPR_TYPE, "find_in_set"}},
    {"initcap", {FUNCTION_OMNI_EXPR_TYPE, "initcap"}},
    {"levenshtein", {FUNCTION_OMNI_EXPR_TYPE, "levenshtein"}},
    {"sha1", {FUNCTION_OMNI_EXPR_TYPE, "sha1"}},
    {"sha2", {FUNCTION_OMNI_EXPR_TYPE, "sha2"}},
    {"regr_count", {FUNCTION_OMNI_EXPR_TYPE, "regr_count"}},
    {"regr_intercept", {FUNCTION_OMNI_EXPR_TYPE, "regr_intercept"}},
    {"regr_r2", {FUNCTION_OMNI_EXPR_TYPE, "regr_r2"}},
    {"regr_slope", {FUNCTION_OMNI_EXPR_TYPE, "regr_slope"}},
    {"regr_sxx", {FUNCTION_OMNI_EXPR_TYPE, "regr_sxx"}},
    {"regr_sxy", {FUNCTION_OMNI_EXPR_TYPE, "regr_sxy"}},
    {"regr_syy", {FUNCTION_OMNI_EXPR_TYPE, "regr_syy"}},
    {"regr_avgx", {FUNCTION_OMNI_EXPR_TYPE, "regr_avgx"}},
    {"regr_avgy", {FUNCTION_OMNI_EXPR_TYPE, "regr_avgy"}},
    {"regr_replacement", {FUNCTION_OMNI_EXPR_TYPE, "regr_replacement"}},
    {"soundex", {FUNCTION_OMNI_EXPR_TYPE, "soundex"}},
    {"array_append", {FUNCTION_OMNI_EXPR_TYPE, "array_append"}},
};
} // namespace omniruntime
