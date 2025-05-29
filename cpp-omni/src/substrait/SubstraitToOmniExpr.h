/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: print expression tree methods
 */

#pragma once
#include <memory>
#include <optional>
#include "util/type_util.h"
#include "plannode/planNode.h"
#include "expression/expressions.h"
#include "SubstraitParser.h"

namespace omniruntime {
using namespace expressions;
using TypedExprPtr = expressions::Expr*;
const int RLIKE_INPUT = 2;

class SubstraitOmniExprConverter {
public:
    /// subParser: A Substrait parser used to convert Substrait representations
    /// into recognizable representations. functionMap: A pre-constructed map
    /// storing the relations between the function id and the function name.
    explicit SubstraitOmniExprConverter(const std::unordered_map<uint64_t, std::string> &functionMap)
        : functionMap_(functionMap) {}

    /// Stores the variant and its type.
    // struct TypedVariant {
    //   variant OmniVariant;
    //   DataTypePtr variantType;
    // };

    /// Convert Substrait Field into Omni Field Expression.
    TypedExprPtr ToOmniExpr(const ::substrait::Expression::FieldReference &substraitField,
        const DataTypesPtr &inputType);

    /// Convert Substrait ScalarFunction into Omni Expression.
    TypedExprPtr ToOmniExpr(const ::substrait::Expression::ScalarFunction &substraitFunc,
        const DataTypesPtr &inputType);

    /// Convert Substrait SingularOrList into Omni Expression.
    TypedExprPtr ToOmniExpr(const ::substrait::Expression::SingularOrList &singularOrList,
        const DataTypesPtr &inputType);

    /// Convert Substrait CastExpression to Omni Expression.
    TypedExprPtr ToOmniExpr(const ::substrait::Expression::Cast &castExpr, const DataTypesPtr &inputType);

    /// Used to convert Substrait Literal into Omni Expression.
    TypedExprPtr ToOmniExpr(const ::substrait::Expression::Literal &substraitLit);

    /// Convert Substrait Expression into Omni Expression.
    TypedExprPtr ToOmniExpr(const ::substrait::Expression &substraitExpr, const DataTypesPtr &inputType);

    /// Convert Substrait IfThen into switch or if expression.
    TypedExprPtr ToOmniExpr(const ::substrait::Expression::IfThen &substraitIfThen, const DataTypesPtr &inputType);

    /// Wrap a constant vector from literals with an array vector inside to create
    /// the constant expression.
    std::shared_ptr<const LiteralExpr> literalsToConstantExpr(
        const std::vector<::substrait::Expression::Literal> &literals);

    /// Create expression for lambda.
    std::shared_ptr<const Expr> toLambdaExpr(const ::substrait::Expression::ScalarFunction &substraitFunc,
        const DataTypesPtr &inputType);

private:
    /// Memory pool.
    // memory::MemoryPool* pool_;

    /// The map storing the relations between the function id and the function
    /// name.
    std::unordered_map<uint64_t, std::string> functionMap_;

};
}
