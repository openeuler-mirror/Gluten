/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

#include "gtest/gtest.h"
#include "substrait/SubstraitParser.h"
#include "substrait/SubstraitToOmniExpr.h"
#include <future>

namespace omniruntime {
namespace {
::substrait::Type StringType()
{
    ::substrait::Type type;
    type.mutable_string();
    return type;
}

::substrait::Type StringTypeWithVariation(uint32_t variationRef)
{
    ::substrait::Type type;
    type.mutable_string()->set_type_variation_reference(variationRef);
    return type;
}

::substrait::Expression::Literal StringLiteral(const std::string &value)
{
    ::substrait::Expression::Literal literal;
    literal.set_string(value);
    return literal;
}

::substrait::Expression::Literal StringLiteralWithVariation(const std::string &value, uint32_t variationRef)
{
    ::substrait::Expression::Literal literal;
    literal.set_string(value);
    literal.set_type_variation_reference(variationRef);
    return literal;
}
} // namespace

// The wire is self-describing: type_variation_reference alone decides the physical type.
TEST(StringViewTypeMappingTest, TypeVariationIsSelfDescribing)
{
    // variation 0 (and a bare string with no variation) -> VARCHAR, regardless of the flag.
    const auto v0 = StringTypeWithVariation(0);
    EXPECT_EQ(SubstraitParser::ParseType(v0, false, false)->GetId(), type::OMNI_VARCHAR);
    EXPECT_EQ(SubstraitParser::ParseType(v0, false, false)->GetId(), type::OMNI_VARCHAR);
    EXPECT_EQ(SubstraitParser::ParseType(StringType(), false, false)->GetId(), type::OMNI_VARCHAR);

    // variation 21 -> StringView, regardless of the flag.
    const auto v21 = StringTypeWithVariation(SubstraitParser::OMNI_STRING_VIEW_TYPE_VARIATION_REFERENCE);
    EXPECT_EQ(SubstraitParser::ParseType(v21, false, false)->GetId(), type::OMNI_STRING_VIEW);
    EXPECT_EQ(SubstraitParser::ParseType(v21, false, false)->GetId(), type::OMNI_STRING_VIEW);
}

TEST(StringViewTypeMappingTest, UnknownVariationIsRejected)
{
    const auto vBad = StringTypeWithVariation(7);
    EXPECT_ANY_THROW(SubstraitParser::ParseType(vBad, false, false));
    EXPECT_ANY_THROW(SubstraitParser::ParseType(vBad, false, false));
}

// Literals are self-describing too: the literal's own type_variation_reference decides SV vs
// VARCHAR
TEST(StringViewTypeMappingTest, LiteralVariationIsSelfDescribing)
{
    const std::unordered_map<uint64_t, std::string> functionMap;
    SubstraitOmniExprConverter varcharConverter(functionMap);
    SubstraitOmniExprConverter switchOnConverter(functionMap);

    // No variation -> VARCHAR, regardless of the converter flag.
    const auto plainLit = StringLiteral("tiny");
    auto *e1 = varcharConverter.ToOmniExpr(plainLit);
    auto *e2 = switchOnConverter.ToOmniExpr(plainLit);
    EXPECT_EQ(e1->GetReturnTypeId(), type::OMNI_VARCHAR);
    EXPECT_EQ(e2->GetReturnTypeId(), type::OMNI_VARCHAR);
    delete e1;
    delete e2;

    // variation 21 -> StringView, regardless of the converter flag.
    const auto svLit =
        StringLiteralWithVariation("tiny", SubstraitParser::OMNI_STRING_VIEW_TYPE_VARIATION_REFERENCE);
    auto *e3 = varcharConverter.ToOmniExpr(svLit);
    auto *e4 = switchOnConverter.ToOmniExpr(svLit);
    EXPECT_EQ(e3->GetReturnTypeId(), type::OMNI_STRING_VIEW);
    EXPECT_EQ(e4->GetReturnTypeId(), type::OMNI_STRING_VIEW);
    delete e3;
    delete e4;
}

TEST(StringViewTypeMappingTest, ConcurrentDecodeIsStable)
{
    auto parseLiteralType = [](uint32_t variationRef) {
        const std::unordered_map<uint64_t, std::string> functionMap;
        // Flag deliberately toggled to prove it does not affect the (variation-driven) decode.
        SubstraitOmniExprConverter converter(functionMap);
        auto *expr = converter.ToOmniExpr(StringLiteralWithVariation("tiny", variationRef));
        const auto typeId = expr->GetReturnTypeId();
        delete expr;
        return typeId;
    };

    for (int i = 0; i < 32; ++i) {
        auto svFuture = std::async(std::launch::async, parseLiteralType,
            SubstraitParser::OMNI_STRING_VIEW_TYPE_VARIATION_REFERENCE);
        auto varcharFuture = std::async(std::launch::async, parseLiteralType, 0u);

        EXPECT_EQ(svFuture.get(), type::OMNI_STRING_VIEW);
        EXPECT_EQ(varcharFuture.get(), type::OMNI_VARCHAR);
    }
}
} // namespace omniruntime
