/**
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "gtest/gtest.h"
#include "shuffle/arrow_type_bridge.h"
#include "shuffle/type.h"

using namespace omniruntime::type;

TEST(ArrowTypeBridge, FixedWidthMapping)
{
    auto d = DataTypeToDescriptor(IntDataType::Instance());
    EXPECT_EQ(d.typeId, OMNI_INT);
    EXPECT_EQ(d.numChildren, 0);
    EXPECT_EQ(PhysicalTypeOf(d), OmniPhysicalType::INT32);
    EXPECT_EQ(PhysicalTypeByteWidth(PhysicalTypeOf(d)), 4);
    EXPECT_EQ(NumBuffers(d), 2);   // validity + values
}

TEST(ArrowTypeBridge, Decimal64IsDistinctPhysicalType)
{
    auto d = DataTypeToDescriptor(std::make_shared<Decimal64DataType>(18, 2));
    EXPECT_EQ(d.typeId, OMNI_DECIMAL64);
    EXPECT_EQ(d.precision, 18);
    EXPECT_EQ(d.scale, 2);
    EXPECT_EQ(PhysicalTypeOf(d), OmniPhysicalType::DECIMAL64);   // 不退化为 INT64
    EXPECT_EQ(PhysicalTypeByteWidth(PhysicalTypeOf(d)), 8);      // 8B 直存
}

TEST(ArrowTypeBridge, Decimal128Mapping)
{
    auto d = DataTypeToDescriptor(std::make_shared<Decimal128DataType>(38, 4));
    EXPECT_EQ(PhysicalTypeOf(d), OmniPhysicalType::DECIMAL128);
    EXPECT_EQ(PhysicalTypeByteWidth(PhysicalTypeOf(d)), 16);
}

TEST(ArrowTypeBridge, VarcharIsBinaryLayout)
{
    auto d = DataTypeToDescriptor(VarcharDataType::Instance());
    EXPECT_EQ(PhysicalTypeOf(d), OmniPhysicalType::BINARY);
    EXPECT_EQ(PhysicalTypeByteWidth(PhysicalTypeOf(d)), 0);  // 变长
    EXPECT_EQ(NumBuffers(d), 3);   // validity + offsets + values
}

TEST(ArrowTypeBridge, ArrayHasChildAndListLayout)
{
    // ARRAY<INT>：外层 List，child=INT
    auto d = DataTypeToDescriptor(std::make_shared<ArrayType>(IntType()));
    EXPECT_EQ(PhysicalTypeOf(d), OmniPhysicalType::LIST);
    EXPECT_EQ(d.numChildren, 1);
    ASSERT_EQ(static_cast<int>(d.children.size()), 1);
    EXPECT_EQ(PhysicalTypeOf(d.children[0]), OmniPhysicalType::INT32);
}

TEST(ArrowTypeBridge, NestedArrayLosesNoChildren)
{
    // ARRAY<ARRAY<INT>>：内层 ARRAY 必须保留 INT 子描述符（修复点回归保护）
    auto inner = std::make_shared<ArrayType>(IntType());
    auto outer = std::make_shared<ArrayType>(inner);
    auto d = DataTypeToDescriptor(outer);
    ASSERT_EQ(d.children.size(), 1u);
    EXPECT_EQ(d.children[0].typeId, OMNI_ARRAY);
    ASSERT_EQ(d.children[0].children.size(), 1u);
    EXPECT_EQ(d.children[0].children[0].typeId, OMNI_INT);
}

TEST(ArrowTypeBridge, MapKeyValueHeterogeneous)
{
    // MAP<VARCHAR, ARRAY<INT>>：key/value 异构且 value 递归
    auto mp = std::make_shared<MapType>(VarcharDataType::Instance(),
                                        std::make_shared<ArrayType>(IntType()));
    auto d = DataTypeToDescriptor(mp);
    EXPECT_EQ(PhysicalTypeOf(d), OmniPhysicalType::MAP);
    ASSERT_EQ(d.children.size(), 2u);
    EXPECT_EQ(d.children[0].typeId, OMNI_VARCHAR);          // key
    EXPECT_EQ(d.children[1].typeId, OMNI_ARRAY);            // value
    ASSERT_EQ(d.children[1].children.size(), 1u);
    EXPECT_EQ(d.children[1].children[0].typeId, OMNI_INT);
}

TEST(ArrowTypeBridge, StructMultiField)
{
    // STRUCT<INT, VARCHAR>：多字段 struct 子列数完整（修复点回归保护）
    std::vector<std::shared_ptr<DataType>> fields;
    fields.push_back(IntType());
    fields.push_back(VarcharType());
    auto d = DataTypeToDescriptor(std::make_shared<RowType>(fields));
    EXPECT_EQ(PhysicalTypeOf(d), OmniPhysicalType::STRUCT);
    ASSERT_EQ(d.children.size(), 2u);
    EXPECT_EQ(d.children[0].typeId, OMNI_INT);
    EXPECT_EQ(d.children[1].typeId, OMNI_VARCHAR);
}

TEST(ArrowTypeBridge, RoundTripDescriptorToOmniType)
{
    auto d = DataTypeToDescriptor(LongDataType::Instance());
    auto back = DescriptorToOmniType(d);
    EXPECT_EQ(back->GetId(), OMNI_LONG);
}

TEST(ArrowTypeBridge, RoundTripComplexDescriptor)
{
    // ARRAY<STRUCT<INT, ARRAY<INT>>> 完整 roundtrip（DataType → descriptor → DataType）
    auto innerArr = std::make_shared<ArrayType>(IntType());
    std::vector<std::shared_ptr<DataType>> fields;
    fields.push_back(IntType());
    fields.push_back(innerArr);
    auto rowType = std::make_shared<RowType>(fields);
    auto arrType = std::make_shared<ArrayType>(rowType);

    auto d = DataTypeToDescriptor(arrType);
    auto back = DescriptorToOmniType(d);
    EXPECT_EQ(back->GetId(), OMNI_ARRAY);
    auto backArr = std::dynamic_pointer_cast<ArrayType>(back);
    ASSERT_NE(backArr, nullptr);
    EXPECT_EQ(backArr->ElementType()->GetId(), OMNI_ROW);
    auto backRow = std::dynamic_pointer_cast<RowType>(backArr->ElementType());
    ASSERT_NE(backRow, nullptr);
    ASSERT_EQ(backRow->size(), 2u);
    EXPECT_EQ(backRow->childAt(0)->GetId(), OMNI_INT);
    EXPECT_EQ(backRow->childAt(1)->GetId(), OMNI_ARRAY);
}
