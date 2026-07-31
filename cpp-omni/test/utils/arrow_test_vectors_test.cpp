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

#include <gtest/gtest.h>

#include "arrow_test_vectors.h"
#include "shuffle/arrow_type_bridge.h"
#include "shuffle/arrow_columnar_deserializer.h"
#include "shuffle/type.h"
#include <vector/vector_common.h>

using namespace omniruntime::vec;
using namespace omniruntime::type;

// --- Tests for BuildColumnarBuffersFromVectorBatch + round-trip ---

TEST(ArrowTestVectors, BuildAndDeserializeRoundTrip_INT) {
  auto* original = MakeBatch_INT(5);
  ASSERT_NE(original, nullptr);
  ASSERT_EQ(original->GetVectorCount(), 1);

  std::vector<OmniTypeDescriptor> schema = {DataTypeToDescriptor(IntDataType::Instance())};

  auto buffers = BuildColumnarBuffersFromVectorBatch(*original, schema);

  auto* back = VectorHelper::CreateVector(OMNI_FLAT, OMNI_INT, 5);
  size_t idx = 0;
  DeserializeArrowBufferToOmniVector(schema[0], 5, buffers, idx, back);

  VectorBatch backBatch(5);
  backBatch.Append(back);

  EXPECT_TRUE(VectorBatchesEqual(*original, backBatch));

  // NOTE: 'back' is owned by backBatch (stack-allocated); do NOT delete it.
  delete original;
}

TEST(ArrowTestVectors, BuildAndDeserializeRoundTrip_INT_LONG_VARCHAR) {
  auto* original = MakeBatch_INT_LONG_VARCHAR(6, 2);
  ASSERT_NE(original, nullptr);
  ASSERT_EQ(original->GetVectorCount(), 3);

  std::vector<OmniTypeDescriptor> schema = {
      DataTypeToDescriptor(IntDataType::Instance()),
      DataTypeToDescriptor(LongDataType::Instance()),
      DataTypeToDescriptor(VarcharDataType::Instance())};

  auto buffers = BuildColumnarBuffersFromVectorBatch(*original, schema);
  size_t idx = 0;

  VectorBatch backBatch(6);
  for (int i = 0; i < 3; ++i) {
    auto* back = VectorHelper::CreateVector(OMNI_FLAT,
                                            schema[i].typeId, 6);
    DeserializeArrowBufferToOmniVector(schema[i], 6, buffers, idx, back);
    backBatch.Append(back);
  }

  EXPECT_TRUE(VectorBatchesEqual(*original, backBatch));

  // NOTE: vectors in backBatch are owned by the stack-allocated batch; do NOT delete them individually.
  delete original;
}

TEST(ArrowTestVectors, BuildAndDeserializeRoundTrip_DECIMAL64) {
  constexpr uint32_t kPrecision = 7;
  constexpr uint32_t kScale = 2;
  auto* original = MakeBatch_DECIMAL64(4, kPrecision, kScale);
  ASSERT_NE(original, nullptr);
  ASSERT_EQ(original->GetVectorCount(), 1);

  std::vector<OmniTypeDescriptor> schema = {
      DataTypeToDescriptor(std::make_shared<Decimal64DataType>(kPrecision, kScale))};

  auto buffers = BuildColumnarBuffersFromVectorBatch(*original, schema);

  auto* back = VectorHelper::CreateVector(OMNI_FLAT, OMNI_DECIMAL64, 4);
  size_t idx = 0;
  DeserializeArrowBufferToOmniVector(schema[0], 4, buffers, idx, back);

  VectorBatch backBatch(4);
  backBatch.Append(back);

  EXPECT_TRUE(VectorBatchesEqual(*original, backBatch));

  // NOTE: 'back' is owned by backBatch (stack-allocated); do NOT delete it.
  delete original;
}

// --- Tests for VectorBatchesEqual ---

TEST(ArrowTestVectors, EqualBatchesTrue) {
  auto* a = MakeBatch_INT(4);
  auto* b = MakeBatch_INT(4);
  ASSERT_NE(a, nullptr);
  ASSERT_NE(b, nullptr);
  EXPECT_TRUE(VectorBatchesEqual(*a, *b));
  delete a;
  delete b;
}

TEST(ArrowTestVectors, UnequalBatchesFalse_DifferentRowCount) {
  auto* a = MakeBatch_INT(4);
  auto* b = MakeBatch_INT(5);
  ASSERT_NE(a, nullptr);
  ASSERT_NE(b, nullptr);
  EXPECT_FALSE(VectorBatchesEqual(*a, *b));
  delete a;
  delete b;
}

TEST(ArrowTestVectors, UnequalBatchesFalse_DifferentValue) {
  auto* a = MakeBatch_INT(4);
  // Create a batch with the same row count but different values
  auto* vb = new VectorBatch(4);
  auto* vec = new Vector<int32_t>(4);
  for (int i = 0; i < 4; ++i)
    vec->SetValue(i, 99);  // Different from MakeBatch_INT's 1,2,3,4
  vb->Append(vec);
  EXPECT_FALSE(VectorBatchesEqual(*a, *vb));
  delete a;
  delete vb;
}

TEST(ArrowTestVectors, EqualBatchesWithNulls) {
  auto* a = MakeBatch_INT_LONG_VARCHAR(6, 2);
  auto* b = MakeBatch_INT_LONG_VARCHAR(6, 2);
  ASSERT_NE(a, nullptr);
  ASSERT_NE(b, nullptr);
  EXPECT_TRUE(VectorBatchesEqual(*a, *b));
  delete a;
  delete b;
}

TEST(ArrowTestVectors, UnequalBatchesFalse_DifferentNullPattern) {
  auto* a = MakeBatch_INT_LONG_VARCHAR(6, 2);
  auto* b = MakeBatch_INT_LONG_VARCHAR(6, 3);  // Different nullSeed
  ASSERT_NE(a, nullptr);
  ASSERT_NE(b, nullptr);
  EXPECT_FALSE(VectorBatchesEqual(*a, *b));
  delete a;
  delete b;
}

// --- Tests for MakeBatch constructors ---

TEST(ArrowTestVectors, MakeBatch_INT_ReturnsCorrectBatch) {
  auto* vb = MakeBatch_INT(3);
  ASSERT_NE(vb, nullptr);
  EXPECT_EQ(vb->GetVectorCount(), 1);
  EXPECT_EQ(vb->GetRowCount(), 3);
  auto* vec = vb->Get(0);
  EXPECT_EQ(vec->GetTypeId(), OMNI_INT);
  EXPECT_EQ(vec->GetSize(), 3);
  // Values should be 1, 2, 3
  EXPECT_FALSE(vec->IsNull(0));
  EXPECT_FALSE(vec->IsNull(1));
  EXPECT_FALSE(vec->IsNull(2));
  delete vb;
}

TEST(ArrowTestVectors, MakeBatch_INT_LONG_VARCHAR_HasNulls) {
  uint32_t nullSeed = 2;
  auto* vb = MakeBatch_INT_LONG_VARCHAR(6, nullSeed);
  ASSERT_NE(vb, nullptr);
  EXPECT_EQ(vb->GetVectorCount(), 3);

  // nullSeed=2: rows where i % 3 == 0 should be null (i=0,3)
  for (int col = 0; col < 3; ++col) {
    auto* vec = vb->Get(col);
    for (int row = 0; row < 6; ++row) {
      if (row % static_cast<int>(nullSeed + 1) == 0) {
        EXPECT_TRUE(vec->IsNull(row))
            << "col=" << col << " row=" << row << " should be null";
      } else {
        EXPECT_FALSE(vec->IsNull(row))
            << "col=" << col << " row=" << row << " should not be null";
      }
    }
  }
  delete vb;
}

TEST(ArrowTestVectors, MakeBatch_VARCHAR_CHAR_ReturnsCorrectTypes) {
  auto* vb = MakeBatch_VARCHAR_CHAR(4, 0);
  ASSERT_NE(vb, nullptr);
  EXPECT_EQ(vb->GetVectorCount(), 2);
  EXPECT_EQ(vb->Get(0)->GetTypeId(), OMNI_VARCHAR);
  EXPECT_EQ(vb->Get(1)->GetTypeId(), OMNI_CHAR);
  delete vb;
}

TEST(ArrowTestVectors, MakeBatch_ArrayInt_ReturnsCorrectStructure) {
  auto* vb = MakeBatch_ArrayInt(3, 0);
  ASSERT_NE(vb, nullptr);
  EXPECT_EQ(vb->GetVectorCount(), 2);
  EXPECT_EQ(vb->Get(0)->GetTypeId(), OMNI_INT);
  EXPECT_EQ(vb->Get(1)->GetTypeId(), OMNI_ARRAY);
  delete vb;
}

TEST(ArrowTestVectors, MakeBatch_DictionaryIntLong_ReturnsDictVectors) {
  auto* vb = MakeBatch_DictionaryIntLong(6);
  ASSERT_NE(vb, nullptr);
  EXPECT_EQ(vb->GetVectorCount(), 3);
  EXPECT_EQ(vb->Get(0)->GetTypeId(), OMNI_INT);
  // Dictionary vectors still carry their underlying DataTypeId
  EXPECT_EQ(vb->Get(1)->GetTypeId(), OMNI_INT);
  EXPECT_EQ(vb->Get(2)->GetTypeId(), OMNI_LONG);
  delete vb;
}
