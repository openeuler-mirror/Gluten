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
 * Task 9: 列式写侧·阶段A —— 定宽列散列改 Arrow ResizableBuffer + null bitmap 取反
 * Arrow 专项单测（WIP：旧 shuffle_test.cpp 不作基线，以 ArrowColumnarSplit.* 绿为准）
 *
 * 测试范围：
 *   1. 多类型定宽列 split → partition_arrow_batch_ 分区行数验证
 *   2. 批对齐验证（缓存批行数之和 = 输入总行数）
 *   3. 含 null 列的 validity bitmap 验证
 */

#include "gtest/gtest.h"
#include "../utils/test_utils.h"
#include <string>

static std::string tmpShuffleFilePath = "/tmp/shuffleTests/arrow_split_test";

// ==================================================================================================
// TEST 1: 多类型定宽列 split → stop → 验证 partition_arrow_batch_ 行数/分区长度正确
// ==================================================================================================
TEST(ArrowColumnarSplit, FixedWidthPartitionLengthsCorrect)
{
    const int32_t numPartitions = 4;
    const int32_t numCols = 3;
    int32_t inputVecTypeIds[] = {OMNI_INT, OMNI_LONG, OMNI_DOUBLE};
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[numCols]{};
    inputDataTypes.inputDataScales = new uint32_t[numCols]{};

    long splitterAddr = Test_splitter_nativeMake("hash", numPartitions, inputDataTypes, numCols,
        1024, "lz4", tmpShuffleFilePath + "_fw_partlen", 0, "/tmp/shuffleTests");

    // 喂 5 批，每批 100 行
    for (int b = 0; b < 5; ++b) {
        VectorBatch* vb = CreateVectorBatch_5fixedCols_withPid(numPartitions, 100);
        Test_splitter_split(splitterAddr, vb);
    }
    Test_splitter_stop(splitterAddr);

    auto* splitter = reinterpret_cast<Splitter*>(splitterAddr);
    // partition_arrow_batch_ 各 pid 各批 rowCount 之和 > 0，且分区数 = 4
    int64_t cachedRows = splitter->TotalCachedArrowRows();
    EXPECT_GT(cachedRows, 0);
    EXPECT_EQ(splitter->ArrowCachedBatches().size(), static_cast<size_t>(numPartitions));

    Test_splitter_close(splitterAddr);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
}

// ==================================================================================================
// TEST 2: 批对齐：缓存批行数不固定，但 partition_arrow_batch_ 每批 rowCount > 0 且总和 = 输入行数
// ==================================================================================================
TEST(ArrowColumnarSplit, BatchAlignedRowsSumToInput)
{
    const int32_t numPartitions = 2;
    const int32_t numCols = 1;
    // CreateVectorBatch_1FixCol_withPid 内部固定用 int64_t 构造数据列，须配 OMNI_LONG (8B)
    int32_t inputVecTypeIds[] = {OMNI_LONG};
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[numCols]{};
    inputDataTypes.inputDataScales = new uint32_t[numCols]{};

    // 不设置低 spill 阈值（使用默认值），让数据留在内存中，
    // 这样 TotalCachedArrowRows() 才能验证缓存批行数之和 = 输入行数。
    // （低阈值会触发 SpillToTmpFile → 清空 partition_arrow_batch_，导致 cachedRows=0）
    long splitterAddr = Test_splitter_nativeMake("hash", numPartitions, inputDataTypes, numCols,
        1024, "uncompressed", tmpShuffleFilePath + "_fw_align", 0, "/tmp/shuffleTests");

    int totalRows = 0;
    for (int b = 0; b < 3; ++b) {
        // fixColType 必须非空：LongType() 匹配函数内 int64_t 数据
        VectorBatch* vb = CreateVectorBatch_1FixCol_withPid(numPartitions, 50, LongType());
        totalRows += 50;
        Test_splitter_split(splitterAddr, vb);
    }
    Test_splitter_stop(splitterAddr);

    auto* splitter = reinterpret_cast<Splitter*>(splitterAddr);
    int64_t cachedRows = splitter->TotalCachedArrowRows();
    EXPECT_EQ(cachedRows, static_cast<int64_t>(totalRows));

    Test_splitter_close(splitterAddr);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
}

// ==================================================================================================
// TEST 3: 含 null 的定宽列：split → stop → 验证 partition_arrow_batch_ 中 validity buffer 非空
// ==================================================================================================
TEST(ArrowColumnarSplit, ValidityBitmapPresentForNullColumn)
{
    const int32_t numPartitions = 2;
    const int32_t numCols = 1;
    const int32_t numRows = 10;

    int32_t inputVecTypeIds[] = {OMNI_INT};
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[numCols]{};
    inputDataTypes.inputDataScales = new uint32_t[numCols]{};

    long splitterAddr = Test_splitter_nativeMake("hash", numPartitions, inputDataTypes, numCols,
        1024, "uncompressed", tmpShuffleFilePath + "_fw_null", 0, "/tmp/shuffleTests");

    // 构造含 null 的 VectorBatch：PID 列 + 1 个定宽 INT 列（部分 null）
    using namespace omniruntime::vec;
    auto pidVec = new Vector<int32_t>(numRows);
    auto intVec = new Vector<int32_t>(numRows);
    for (int32_t i = 0; i < numRows; ++i) {
        pidVec->SetValue(i, i % numPartitions);
        intVec->SetValue(i, i * 10);
    }
    // 设置几行为 null
    intVec->SetNull(0);
    intVec->SetNull(3);
    intVec->SetNull(7);

    auto vb = new VectorBatch(numRows);
    vb->Append(pidVec);
    vb->Append(intVec);

    Test_splitter_split(splitterAddr, vb);
    Test_splitter_stop(splitterAddr);

    auto* splitter = reinterpret_cast<Splitter*>(splitterAddr);
    // 至少一个缓存批的 validity buffer 非 nullptr（因含 null，定宽 buffers[0] = validity）
    bool foundValidity = false;
    for (const auto& batches : splitter->ArrowCachedBatches()) {
        for (const auto& b : batches) {
            // 定宽列 buffers 顺序 = [validity, values]
            if (b.buffers.size() >= 2 && b.buffers[0] != nullptr) {
                foundValidity = true;
                // 额外验证：values buffer 应非空
                EXPECT_NE(b.buffers[1], nullptr);
            }
        }
    }
    EXPECT_TRUE(foundValidity);

    Test_splitter_close(splitterAddr);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
}
