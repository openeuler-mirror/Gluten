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

/**
 * Task 10: 列式写侧·阶段B —— 变长列 gather 改 Arrow buffer
 * Arrow 专项单测（WIP：旧 shuffle_test.cpp 不作基线，以 ArrowColumnarVariable.* 绿为准）
 *
 * 测试范围：
 *   1. 混合 schema (INT+VARCHAR) split → 验证变长列 offsets + values buffer 结构正确
 *   2. 含 null 的 VARCHAR split → 验证 validity bitmap 非空（取反后 Arrow 置位=有效）
 *
 * 说明：使用混合 schema（定宽+VARCHAR）确保 CacheVectorBatch 被触发。
 *       缓存批 buffers 按 schema 列序展开：
 *         INT:    [validity(int)][values(int)]
 *         VARCHAR:[validity(varchar)][offsets(varchar)][values(varchar)]
 *       全有效时 validity 为 nullptr 哨兵。
 */

#include "gtest/gtest.h"
#include "../utils/test_utils.h"
#include <string>

static std::string tmpShuffleFilePath = "/tmp/shuffleTests/arrow_varchar";

// ==================================================================================================
// TEST 1: INT + VARCHAR（全有效）→ 验证变长列 offsets + values buffer 结构正确
// ==================================================================================================
TEST(ArrowColumnarVariable, VarcharBuffersBuilt)
{
    const int32_t numPartitions = 2;
    const int32_t numCols = 2;  // INT + VARCHAR
    const int32_t numRows = 10;

    int32_t inputVecTypeIds[] = {OMNI_INT, OMNI_VARCHAR};
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[numCols]{};
    inputDataTypes.inputDataScales = new uint32_t[numCols]{};

    long splitterAddr = Test_splitter_nativeMake("hash", numPartitions, inputDataTypes, numCols,
        1024, "uncompressed", tmpShuffleFilePath + "_buffers", 0, "/tmp/shuffleTests");

    // Build VectorBatch: PID + INT + VARCHAR
    using namespace omniruntime::vec;
    auto pidVec = new Vector<int32_t>(numRows);
    auto intVec = new Vector<int32_t>(numRows);
    using VarcharVector = Vector<LargeStringContainer<std::string_view>>;
    auto varcharVec = new VarcharVector(numRows);

    std::string strData[] = {"hello", "world", "foo", "bar", "baz", "qux", "alpha", "beta", "gamma", "delta"};
    for (int32_t i = 0; i < numRows; ++i) {
        pidVec->SetValue(i, i % numPartitions);
        intVec->SetValue(i, i * 100);
        std::string_view sv(strData[i].data(), strData[i].length());
        varcharVec->SetValue(i, sv);
    }

    auto vb = new VectorBatch(numRows);
    vb->Append(pidVec);
    vb->Append(intVec);
    vb->Append(varcharVec);

    Test_splitter_split(splitterAddr, vb);
    Test_splitter_stop(splitterAddr);

    auto* splitter = reinterpret_cast<Splitter*>(splitterAddr);

    // 检查 partition_arrow_batch_ 中每个非空批的变长列 buffer 结构
    // Schema: INT(2 buf) + VARCHAR(3 buf)
    // buffers[0]=validity(int), buffers[1]=values(int)
    // buffers[2]=validity(varchar), buffers[3]=offsets(varchar), buffers[4]=values(varchar)
    bool foundVarchar = false;
    for (const auto& batches : splitter->ArrowCachedBatches()) {
        for (const auto& b : batches) {
            if (b.rowCount > 0) {
                ASSERT_GE(b.buffers.size(), 5u) << "Expected at least 5 buffers (2 INT + 3 VARCHAR)";

                // 全有效 → VARCHAR validity 应为 nullptr 哨兵
                EXPECT_EQ(b.buffers[2], nullptr) << "Varchar validity should be nullptr (all valid)";

                // offsets buffer: int32 数组，(rowCount+1) × 4 字节
                auto offsets = b.buffers[3];
                ASSERT_NE(offsets, nullptr) << "Varchar offsets buffer should not be null";
                EXPECT_EQ(offsets->size(), static_cast<int64_t>(b.rowCount + 1) * sizeof(int32_t));

                // values buffer: 串体字节应有内容
                auto values = b.buffers[4];
                ASSERT_NE(values, nullptr) << "Varchar values buffer should not be null";
                EXPECT_GT(values->size(), 0) << "Varchar values should have content";

                foundVarchar = true;
            }
        }
    }
    EXPECT_TRUE(foundVarchar) << "At least one cached batch should contain varchar buffers";

    Test_splitter_close(splitterAddr);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
}

// ==================================================================================================
// TEST 2: INT + VARCHAR（VARCHAR 含 null）→ 验证变长列 validity bitmap 非空
// ==================================================================================================
TEST(ArrowColumnarVariable, VarcharWithNull)
{
    const int32_t numPartitions = 2;
    const int32_t numCols = 2;  // INT + VARCHAR
    const int32_t numRows = 10;

    int32_t inputVecTypeIds[] = {OMNI_INT, OMNI_VARCHAR};
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[numCols]{};
    inputDataTypes.inputDataScales = new uint32_t[numCols]{};

    long splitterAddr = Test_splitter_nativeMake("hash", numPartitions, inputDataTypes, numCols,
        1024, "uncompressed", tmpShuffleFilePath + "_null", 0, "/tmp/shuffleTests");

    // Build VectorBatch: PID + INT + VARCHAR（VARCHAR 含 null）
    using namespace omniruntime::vec;
    auto pidVec = new Vector<int32_t>(numRows);
    auto intVec = new Vector<int32_t>(numRows);
    using VarcharVector = Vector<LargeStringContainer<std::string_view>>;
    auto varcharVec = new VarcharVector(numRows);

    std::string strData[] = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};
    for (int32_t i = 0; i < numRows; ++i) {
        pidVec->SetValue(i, i % numPartitions);
        intVec->SetValue(i, i * 10);
        std::string_view sv(strData[i].data(), strData[i].length());
        varcharVec->SetValue(i, sv);
    }
    // 设置几行为 null
    varcharVec->SetNull(0);
    varcharVec->SetNull(3);
    varcharVec->SetNull(7);

    auto vb = new VectorBatch(numRows);
    vb->Append(pidVec);
    vb->Append(intVec);
    vb->Append(varcharVec);

    Test_splitter_split(splitterAddr, vb);
    Test_splitter_stop(splitterAddr);

    auto* splitter = reinterpret_cast<Splitter*>(splitterAddr);

    // 含 null 变长列：buffers[2] = validity bitmap 应非空
    // Schema: INT(2 buf) + VARCHAR(3 buf)
    // buffers[0]=validity(int), buffers[1]=values(int)
    // buffers[2]=validity(varchar), buffers[3]=offsets(varchar), buffers[4]=values(varchar)
    bool foundValidity = false;
    for (const auto& batches : splitter->ArrowCachedBatches()) {
        for (const auto& b : batches) {
            if (b.rowCount > 0 && b.buffers.size() >= 3 && b.buffers[2] != nullptr) {
                // validity bitmap size 应为 (rowCount + 7) / 8 字节
                int32_t expectedByteCount = (b.rowCount + 7) / 8;
                EXPECT_EQ(b.buffers[2]->size(), expectedByteCount);
                foundValidity = true;
            }
        }
    }
    EXPECT_TRUE(foundValidity) << "Varchar validity bitmap should be present when column has nulls";

    Test_splitter_close(splitterAddr);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
}
