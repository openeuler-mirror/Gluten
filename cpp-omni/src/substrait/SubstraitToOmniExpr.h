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
using TypedExprPtr = std::shared_ptr<const expressions::Expr>;

class SubstraitOmniExprConverter {
public:
    /// subParser: A Substrait parser used to convert Substrait representations
    /// into recognizable representations. functionMap: A pre-constructed map
    /// storing the relations between the function id and the function name.
    explicit SubstraitOmniExprConverter(const std::unordered_map<uint64_t, std::string> &functionMap)
        : functionMap_(functionMap) {}

    /// Stores the variant and its type.
    // struct TypedVariant {
    //   variant veloxVariant;
    //   DataTypePtr variantType;
    // };

    /// Convert Substrait Field into Velox Field Expression.
    static std::shared_ptr<const FieldExpr> ToOmniExpr(const ::substrait::Expression::FieldReference &substraitField,
        const DataTypesPtr &inputType);

    /// Convert Substrait ScalarFunction into Velox Expression.
    TypedExprPtr ToOmniExpr(const ::substrait::Expression::ScalarFunction &substraitFunc,
        const DataTypesPtr &inputType);

    /// Convert Substrait SingularOrList into Velox Expression.
    TypedExprPtr ToOmniExpr(const ::substrait::Expression::SingularOrList &singularOrList,
        const DataTypesPtr &inputType);

    /// Convert Substrait CastExpression to Velox Expression.
    TypedExprPtr ToOmniExpr(const ::substrait::Expression::Cast &castExpr, const DataTypesPtr &inputType);

    /// Create expression for extract.
    static TypedExprPtr toExtractExpr(const std::vector<TypedExprPtr> &params, const DataTypePtr &outputType);

    /// Used to convert Substrait Literal into Velox Expression.
    std::shared_ptr<const LiteralExpr> ToOmniExpr(const ::substrait::Expression::Literal &substraitLit);

    /// Convert Substrait Expression into Velox Expression.
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

    // The map storing the Substrait extract function input field and velox
    // function name.
    static std::unordered_map<std::string, std::string> extractDatetimeFunctionMap_;
};
}
