/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: print expression tree methods
 */

#pragma once
#include "type/data_types.h"
#include "util/omni_exception.h"
#include "substrait/algebra.pb.h"
#include "substrait/capabilities.pb.h"
#include "substrait/extensions/extensions.pb.h"
#include "substrait/function.pb.h"
#include "substrait/parameterized_types.pb.h"
#include "substrait/plan.pb.h"
#include "substrait/type.pb.h"
#include "substrait/type_expressions.pb.h"

#include "util/type_util.h"

namespace omniruntime {
/// This class contains some common functions used to parse Substrait
/// components, and convert them into recognizable representations.
enum SubstraitToOmniExprType {
    IS_NULL_OMNI_EXPR_TYPE = 0,
    IS_NOT_NULL_OMNI_EXPR_TYPE,
    UNARY_OMNI_EXPR_TYPE,
    BINARY_OMNI_EXPR_TYPE,
    FUNCTION_OMNI_EXPR_TYPE,
    COALESCE_OMNI_EXPR_TYPE,
    HIVE_UDF_FUNCTION_OMNI_EXPR_TYPE
};

constexpr const char* SUBSTRAIT_PARSE_ERROR  = "SUBSTRAIT_PARSE_ERROR";
class SubstraitParser {
public:
    /// Used to parse Substrait NamedStruct.
    static std::vector<type::DataTypePtr> ParseNamedStruct(const ::substrait::NamedStruct &namedStruct,
        bool asLowerCase = false);

    /// Used to find the Omni function name according to the function id
    /// from a pre-constructed function map.
    static std::pair<SubstraitToOmniExprType,std::string> FindOmniFunction(const std::unordered_map<uint64_t, std::string> &functionMap, uint64_t id);

    /// Parse Substrait Type to Omni type.
    static type::DataTypePtr ParseType(const ::substrait::Type &substraitType, bool asLowerCase = false);

    /// Parse Substrait ReferenceSegment and extract the field index. Return false if the segment is not a valid unnested
    /// field.
    static bool ParseReferenceSegment(const ::substrait::Expression::ReferenceSegment &refSegment,
        uint32_t &fieldIndex);

    /// Make names in the format of {prefix}_{index}.
    static std::vector<std::string> MakeNames(const std::string &prefix, int size);

    /// Make node name in the format of n{nodeId}_{colIdx}.
    static std::string MakeNodeName(int nodeId, int colIdx);

    /// Get the column index from a node name in the format of
    /// n{nodeId}_{colIdx}.
    static int GetIdxFromNodeName(const std::string &nodeName);

    /// Find the Substrait function name according to the function id
    /// from a pre-constructed function map. The function specification can be
    /// a simple name or a compound name. The compound name format is:
    /// <function name>:<short_arg_type0>_<short_arg_type1>_..._<short_arg_typeN>.
    /// Currently, the input types in the function specification are not used. But
    /// in the future, they should be used for the validation according the
    /// specifications in Substrait yaml files.
    static std::string FindFunctionSpec(const std::unordered_map<uint64_t, std::string> &functionMap, uint64_t id);

    /// Extracts the name of a function by splitting signature with delimiter.
    static std::string GetNameBeforeDelimiter(const std::string &signature, const std::string &delimiter = ":");

    /// This function is used get the types from the compound name.
    static std::vector<std::string> GetSubFunctionTypes(const std::string &subFuncSpec);

    /// Map the Substrait function keyword into Omni function keyword.
    static std::pair<SubstraitToOmniExprType,std::string> MapToOmniFunction(const std::string &substraitFunction);


    /// @brief Return whether a config is set as true in AdvancedExtension
    /// optimization.
    /// @param extension Substrait advanced extension.
    /// @param config the key string of a config.
    /// @return Whether the config is set as true.
    static bool ConfigSetInOptimization(const ::substrait::extensions::AdvancedExtension &, const std::string &config);

    /// Extract input types from Substrait function signature.
    static std::vector<type::DataTypePtr> SigToTypes(const std::string &functionSig);

    // Get values for the different supported types.
    template <typename T>
    static T GetLiteralValue(const ::substrait::Expression::Literal & /* literal */);

private:
    /// A map used for mapping Substrait function keywords into Omni functions'
    /// keywords. Key: the Substrait function keyword, Value: the Omni function
    /// keyword. For those functions with different names in Substrait and Omni,
    /// a mapping relation should be added here.
    static std::unordered_map<std::string, std::pair<SubstraitToOmniExprType,std::string>> substraitOmniFunctionMap;

    // The map is uesd for mapping substrait type.
    // Key: type in function name.
    // Value: substrait type name.
    static const std::unordered_map<std::string, std::string> typeMap;

    static const uint32_t MAX_PRECISION_64 = 18;
    static const uint32_t MAX_PRECISION_128 = 38;
};
}
