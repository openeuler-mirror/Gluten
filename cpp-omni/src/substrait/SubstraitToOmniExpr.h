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

#pragma once
#include <memory>
#include <optional>
#include "SubstraitParser.h"
#include "expression/expressions.h"
#include "plannode/planNode.h"
#include "util/type_util.h"

namespace omniruntime {
using namespace expressions;
using TypedExprPtr = expressions::Expr *;
const int RLIKE_INPUT = 2;
const int COALESCE_INPUT = 2;

/**
 * Extracts a FieldExpr from an expression that may be wrapped in Spark's
 * floating-point normalization pattern for complex types (e.g. STRUCT containing DOUBLE):
 *   Simple struct:   IF(IsNull(field), null_literal, field)
 *   Struct w/ float: IF(IsNull(field), null_literal, named_struct(...normalization...))
 * Both patterns are semantically referencing the original column, so we unwrap
 * to retrieve the underlying FieldExpr for partition/sort key extraction.
 *
 * @return The underlying FieldExpr pointer, or nullptr if the pattern is unrecognized.
 */
inline FieldExpr *ExtractFieldExprFromPartitionOrSortKey(Expr *expression)
{
    if (expression->GetType() == ExprType::FIELD_E) {
        return static_cast<FieldExpr *>(expression);
    }
    if (expression->GetType() == ExprType::IF_E) {
        auto *ifExpr = static_cast<IfExpr *>(expression);
        if (ifExpr->condition->GetType() == ExprType::IS_NULL_E &&
            ifExpr->trueExpr->GetType() == ExprType::LITERAL_E &&
            static_cast<LiteralExpr *>(ifExpr->trueExpr)->isNull) {
            auto *isNullExpr = static_cast<IsNullExpr *>(ifExpr->condition);
            if (isNullExpr->value->GetType() == ExprType::FIELD_E) {
                return static_cast<FieldExpr *>(isNullExpr->value);
            }
            if (ifExpr->falseExpr->GetType() == ExprType::FIELD_E) {
                return static_cast<FieldExpr *>(ifExpr->falseExpr);
            }
        }
    }
    return nullptr;
}

class SubstraitOmniExprConverter {
public:
    /// subParser: A Substrait parser used to convert Substrait representations
    /// into recognizable representations. functionMap: A pre-constructed map
    /// storing the relations between the function id and the function name.
    SubstraitOmniExprConverter(const std::unordered_map<uint64_t, std::string> &functionMap,
        const std::unordered_map<std::string, std::string> &confMap)
        : queryConfig_(confMap), functionMap_(functionMap)
    {}

    /// Stores the variant and its type.
    // struct TypedVariant {
    //   variant OmniVariant;
    //   DataTypePtr variantType;
    // };

    /// Convert Substrait Field into Omni Field Expression.
    TypedExprPtr ToOmniExpr(
        const ::substrait::Expression::FieldReference &substraitField, const DataTypesPtr &inputType);

    /// Convert Substrait ScalarFunction into Omni Expression.
    TypedExprPtr ToOmniExpr(
        const ::substrait::Expression::ScalarFunction &substraitFunc, const DataTypesPtr &inputType);

    /// Convert Substrait SingularOrList into Omni Expression.
    TypedExprPtr ToOmniExpr(
        const ::substrait::Expression::SingularOrList &singularOrList, const DataTypesPtr &inputType);

    /// Convert Substrait CastExpression to Omni Expression.
    TypedExprPtr ToOmniExpr(const ::substrait::Expression::Cast &castExpr, const DataTypesPtr &inputType);

    /// Used to convert Substrait Literal into Omni Expression.
    TypedExprPtr ToOmniExpr(const ::substrait::Expression::Literal &substraitLit, const DataTypePtr defaultType = nullptr);

    /// Create expression for extract.
    static TypedExprPtr ToExtractExpr(const std::vector<TypedExprPtr>& params, const DataTypePtr& outputType);

    /// Convert Substrait Expression into Omni Expression.
    TypedExprPtr ToOmniExpr(const ::substrait::Expression &substraitExpr, const DataTypesPtr &inputType, const DataTypePtr defaultType = nullptr);

    /// Convert Substrait IfThen into switch or if expression.
    TypedExprPtr ToOmniExpr(const ::substrait::Expression::IfThen &substraitIfThen, const DataTypesPtr &inputType, const int32_t index = 0);

    TypedExprPtr UnfoldConcatStringFunc(std::vector<Expr *> args, DataTypePtr outputType);

    CoalesceExpr* BuildNestedCoalesceExpr(const std::vector<Expr*>& args);

    TypedExprPtr toLambdaExpr(std::vector<Expr *> &&args, const DataTypesPtr &inputType);

private:
    config::QueryConfig queryConfig_;

    /// Memory pool.
    // memory::MemoryPool* pool_;

    /// The map storing the relations between the function id and the function
    /// name.
    std::unordered_map<uint64_t, std::string> functionMap_;

    // The map storing the Substrait extract function input field and velox
    // function name.
    static std::unordered_map<std::string, std::string> extractDatetimeFunctionMap_;
};
} // namespace omniruntime
