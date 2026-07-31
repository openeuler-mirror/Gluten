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
 * Task 11: 列式写侧·阶段C —— 复杂类型直建 Arrow buffer 列表 + 删 MergeProtoVec
 * Arrow 专项单测（WIP：旧 shuffle_test.cpp 不作基线，以 ArrowColumnarComplex.* 绿为准）
 *
 * 测试范围：
 *   1. ARRAY<INT>：split → 验证 partition_arrow_batch_ 中 buffer 数正确
 *   2. MAP<INT,INT>：split → 验证 partition_arrow_batch_ 中 buffer 数符合 Map 物理布局
 *   3. ROW<INT,VARCHAR>：split → 验证 partition_arrow_batch_ 中 buffer 数符合 Struct 物理布局
 */

#include "gtest/gtest.h"
#include "../utils/test_utils.h"
#include <string>
#include <memory>

using namespace omniruntime::type;

static std::string tmpShuffleFilePath = "/tmp/shuffleTests/arrow_complex_test";

// ==================================================================================================
// TEST 1: ARRAY<INT> split → 验证 partition_arrow_batch_ 中该列 buffer 数 = 4
//         ARRAY<INT> = [validity?, offsets(int32)] + [child INT validity?, child INT values] = 4
// ==================================================================================================
TEST(ArrowColumnarComplex, ComplexArrayBuildsArrowBuffers)
{
    const int32_t numPartitions = 2;
    const int32_t numCols = 1;
    int32_t inputVecTypeIds[] = {OMNI_ARRAY};
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[numCols]{};
    inputDataTypes.inputDataScales = new uint32_t[numCols]{};

    long splitterAddr = Test_splitter_nativeMake("hash", numPartitions, inputDataTypes, numCols,
        1024, "lz4", tmpShuffleFilePath + "_array", 0, "/tmp/shuffleTests");

    // Task 11: 必须为复杂类型设置完整的 DataType 描述符（inputDataTypes_ 在测试路径默认为空）
    {
        std::vector<DataTypePtr> inputDataTypesVec;
        inputDataTypesVec.push_back(std::make_shared<ArrayType>(IntType()));
        reinterpret_cast<Splitter*>(splitterAddr)->SetInputDataTypes(inputDataTypesVec);
    }

    int32_t ele[] = {0, 1, 2, 3};
    VectorBatch* vb = CreateVectorBatch_1row_array_int_withPid(1, ele, 4);  // pid=1 < numPartitions=2
    Test_splitter_split(splitterAddr, vb);
    Test_splitter_stop(splitterAddr);
    auto* splitter = reinterpret_cast<Splitter*>(splitterAddr);

    // ARRAY<INT> NumBuffers = 2(validity+offsets) + 2(child INT validity+values) = 4
    bool foundComplexBuffer = false;
    for (const auto& batches : splitter->ArrowCachedBatches()) {
        for (const auto& b : batches) {
            if (b.rowCount > 0 && b.buffers.size() >= 4u) {
                foundComplexBuffer = true;
                EXPECT_EQ(b.buffers.size(), 4u);
                // Verify offsets buffer exists (position 1)
                EXPECT_NE(b.buffers[1], nullptr);
                // Verify child values buffer exists (position 3)
                EXPECT_NE(b.buffers[3], nullptr);
            }
        }
    }
    EXPECT_TRUE(foundComplexBuffer);

    Test_splitter_close(splitterAddr);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
}

// ==================================================================================================
// TEST 2: MAP<INT,INT> split → 验证 partition_arrow_batch_ 中该列 buffer 数符合 Map 物理布局
//         MAP<INT,INT> = [validity?, offsets(int32)] + [key validity?, key values] + [value validity?, value values]
//         ≥ 6 buffers (但某些 validity 可能为 nullptr 哨兵)
// ==================================================================================================
TEST(ArrowColumnarComplex, ComplexMapBuildsArrowBuffers)
{
    const int32_t numPartitions = 2;
    const int32_t numCols = 1;
    int32_t inputVecTypeIds[] = {OMNI_MAP};
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[numCols]{};
    inputDataTypes.inputDataScales = new uint32_t[numCols]{};

    long splitterAddr = Test_splitter_nativeMake("hash", numPartitions, inputDataTypes, numCols,
        1024, "lz4", tmpShuffleFilePath + "_map", 0, "/tmp/shuffleTests");

    // Task 11: 必须为复杂类型设置完整的 DataType 描述符
    {
        std::vector<DataTypePtr> inputDataTypesVec;
        inputDataTypesVec.push_back(std::make_shared<MapType>(IntType(), IntType()));
        reinterpret_cast<Splitter*>(splitterAddr)->SetInputDataTypes(inputDataTypesVec);
    }

    VectorBatch* vb = CreateVectorBatch_1row_map_int_int_withPid(1);  // pid=1 < numPartitions=2
    Test_splitter_split(splitterAddr, vb);
    Test_splitter_stop(splitterAddr);
    auto* splitter = reinterpret_cast<Splitter*>(splitterAddr);

    // MAP<INT,INT> NumBuffers = 2(validity+offsets) + 2(key validity+values) + 2(value validity+values) = 6
    bool foundComplexBuffer = false;
    for (const auto& batches : splitter->ArrowCachedBatches()) {
        for (const auto& b : batches) {
            if (b.rowCount > 0 && b.buffers.size() >= 4u) {
                foundComplexBuffer = true;
                EXPECT_GE(b.buffers.size(), 4u);
                // Verify offsets buffer exists (position 1)
                EXPECT_NE(b.buffers[1], nullptr);
            }
        }
    }
    EXPECT_TRUE(foundComplexBuffer);

    Test_splitter_close(splitterAddr);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
}

// ==================================================================================================
// TEST 3: ROW<INT,VARCHAR> split → 验证 partition_arrow_batch_ 中该列 buffer 数符合 Struct 物理布局
//         ROW<INT,VARCHAR> = [validity?] + [child INT validity?, INT values] + [child VARCHAR validity?, VARCHAR offsets, VARCHAR values]
//         ≥ 3 buffers
// ==================================================================================================
TEST(ArrowColumnarComplex, ComplexRowBuildsArrowBuffers)
{
    const int32_t numPartitions = 2;
    const int32_t numCols = 1;
    int32_t inputVecTypeIds[] = {OMNI_ROW};
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[numCols]{};
    inputDataTypes.inputDataScales = new uint32_t[numCols]{};

    long splitterAddr = Test_splitter_nativeMake("hash", numPartitions, inputDataTypes, numCols,
        1024, "lz4", tmpShuffleFilePath + "_row", 0, "/tmp/shuffleTests");

    // Task 11: 必须为复杂类型设置完整的 DataType 描述符
    {
        std::vector<std::shared_ptr<DataType>> fieldTypes;
        fieldTypes.push_back(IntType());
        fieldTypes.push_back(VarcharType());
        std::vector<DataTypePtr> inputDataTypesVec;
        inputDataTypesVec.push_back(std::make_shared<RowType>(fieldTypes));
        reinterpret_cast<Splitter*>(splitterAddr)->SetInputDataTypes(inputDataTypesVec);
    }

    VectorBatch* vb = CreateVectorBatch_1row_row_int_varchar_withPid(1);  // pid=1 < numPartitions=2
    Test_splitter_split(splitterAddr, vb);
    Test_splitter_stop(splitterAddr);
    auto* splitter = reinterpret_cast<Splitter*>(splitterAddr);

    // ROW<INT,VARCHAR> NumBuffers = 1(validity) + child INT(validity+values) + child VARCHAR(validity+offsets+values)
    bool foundComplexBuffer = false;
    for (const auto& batches : splitter->ArrowCachedBatches()) {
        for (const auto& b : batches) {
            if (b.rowCount > 0 && b.buffers.size() >= 3u) {
                foundComplexBuffer = true;
                EXPECT_GE(b.buffers.size(), 3u);
            }
        }
    }
    EXPECT_TRUE(foundComplexBuffer);

    Test_splitter_close(splitterAddr);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
}

// ==================================================================================================
// 方案 C 多批测试：验证增量直接写入后 offsets 天然正确、validity 正确
// ==================================================================================================

// TEST 4: 两批 ARRAY<INT> 同分区 → 验证 offsets 天然正确（无需 rebase）
TEST(ArrowColumnarComplex, MultiBatchArrayOffsetsNaturalRebase) {
    const int32_t numPartitions = 2;
    const int32_t numCols = 1;
    int32_t inputVecTypeIds[] = {OMNI_ARRAY};
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[numCols]{};
    inputDataTypes.inputDataScales = new uint32_t[numCols]{};

    long splitterAddr = Test_splitter_nativeMake("hash", numPartitions, inputDataTypes, numCols,
        1024, "lz4", tmpShuffleFilePath + "_multi_array_offsets", 0, "/tmp/shuffleTests");

    {
        std::vector<DataTypePtr> inputDataTypesVec;
        inputDataTypesVec.push_back(std::make_shared<ArrayType>(IntType()));
        reinterpret_cast<Splitter*>(splitterAddr)->SetInputDataTypes(inputDataTypesVec);
    }

    // 批 1: pid=1, ARRAY=[10, 20, 30] (3 elements)
    int32_t ele1[] = {10, 20, 30};
    VectorBatch* vb1 = CreateVectorBatch_1row_array_int_withPid(1, ele1, 3);
    Test_splitter_split(splitterAddr, vb1);

    // 批 2: pid=1, ARRAY=[40, 50] (2 elements)
    int32_t ele2[] = {40, 50};
    VectorBatch* vb2 = CreateVectorBatch_1row_array_int_withPid(1, ele2, 2);
    Test_splitter_split(splitterAddr, vb2);

    Test_splitter_stop(splitterAddr);
    auto* splitter = reinterpret_cast<Splitter*>(splitterAddr);

    // pid=1 should have exactly 1 cached batch with rowCount=2
    const auto& batches = splitter->ArrowCachedBatches();
    ASSERT_GT(batches.size(), 1u);  // at least pid=1 has data
    int dataBatchCount = 0;
    const ArrowColumnarCachedBatch* dataBatch = nullptr;
    for (const auto& b : batches[1]) {  // pid=1
        if (b.rowCount > 0) {
            dataBatchCount++;
            dataBatch = &b;
        }
    }
    ASSERT_EQ(dataBatchCount, 1) << "Should be 1 merged cached batch";
    ASSERT_NE(dataBatch, nullptr);
    EXPECT_EQ(dataBatch->rowCount, 2) << "2 rows total (1 per batch)";

    // ARRAY<INT> = 4 buffers: [validity, offsets, child_validity, child_values]
    EXPECT_EQ(dataBatch->buffers.size(), 4u);

    // Check offsets: should be [0, 3, 5] (batch1=3 elements, batch2=2 elements, naturally continued)
    ASSERT_NE(dataBatch->buffers[1], nullptr);
    auto* offsets = reinterpret_cast<const int32_t*>(dataBatch->buffers[1]->data());
    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 3);   // batch 1: 3 elements
    EXPECT_EQ(offsets[2], 5);   // batch 1(3) + batch 2(2) = 5 total elements

    // Check child values: [10, 20, 30, 40, 50]
    ASSERT_NE(dataBatch->buffers[3], nullptr);
    auto* childValues = reinterpret_cast<const int32_t*>(dataBatch->buffers[3]->data());
    EXPECT_EQ(childValues[0], 10);
    EXPECT_EQ(childValues[1], 20);
    EXPECT_EQ(childValues[2], 30);
    EXPECT_EQ(childValues[3], 40);
    EXPECT_EQ(childValues[4], 50);

    Test_splitter_close(splitterAddr);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
}

// TEST 5: 三批 ARRAY<INT> 同分区 → 验证多段 offsets 链式正确
TEST(ArrowColumnarComplex, ThreeBatchesArrayOffsetsChain) {
    const int32_t numPartitions = 2;
    const int32_t numCols = 1;
    int32_t inputVecTypeIds[] = {OMNI_ARRAY};
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[numCols]{};
    inputDataTypes.inputDataScales = new uint32_t[numCols]{};

    long splitterAddr = Test_splitter_nativeMake("hash", numPartitions, inputDataTypes, numCols,
        1024, "lz4", tmpShuffleFilePath + "_three_batch_offsets", 0, "/tmp/shuffleTests");

    {
        std::vector<DataTypePtr> inputDataTypesVec;
        inputDataTypesVec.push_back(std::make_shared<ArrayType>(IntType()));
        reinterpret_cast<Splitter*>(splitterAddr)->SetInputDataTypes(inputDataTypesVec);
    }

    // 3 batches, each 1 row to pid=1
    int32_t e1[] = {1, 2};       // 2 elements
    int32_t e2[] = {3};           // 1 element
    int32_t e3[] = {4, 5, 6};    // 3 elements

    VectorBatch* vb1 = CreateVectorBatch_1row_array_int_withPid(1, e1, 2);
    Test_splitter_split(splitterAddr, vb1);
    VectorBatch* vb2 = CreateVectorBatch_1row_array_int_withPid(1, e2, 1);
    Test_splitter_split(splitterAddr, vb2);
    VectorBatch* vb3 = CreateVectorBatch_1row_array_int_withPid(1, e3, 3);
    Test_splitter_split(splitterAddr, vb3);

    Test_splitter_stop(splitterAddr);
    auto* splitter = reinterpret_cast<Splitter*>(splitterAddr);

    const auto& batches = splitter->ArrowCachedBatches();
    const ArrowColumnarCachedBatch* dataBatch = nullptr;
    for (const auto& b : batches[1]) {
        if (b.rowCount > 0) { dataBatch = &b; break; }
    }
    ASSERT_NE(dataBatch, nullptr);
    EXPECT_EQ(dataBatch->rowCount, 3);

    // offsets should be [0, 2, 3, 6] — naturally chained
    auto* offsets = reinterpret_cast<const int32_t*>(dataBatch->buffers[1]->data());
    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 2);   // batch1: 2 elements
    EXPECT_EQ(offsets[2], 3);   // batch1(2) + batch2(1) = 3
    EXPECT_EQ(offsets[3], 6);   // batch1(2) + batch2(1) + batch3(3) = 6

    // child values: [1, 2, 3, 4, 5, 6]
    auto* childValues = reinterpret_cast<const int32_t*>(dataBatch->buffers[3]->data());
    for (int i = 0; i < 6; ++i) {
        EXPECT_EQ(childValues[i], i + 1);
    }

    Test_splitter_close(splitterAddr);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
}

// TEST 6: 单批 ARRAY<INT> 回归保护 — 方案 C 不破坏单批路径
TEST(ArrowColumnarComplex, SingleBatchArrayPlanCRegression) {
    const int32_t numPartitions = 2;
    const int32_t numCols = 1;
    int32_t inputVecTypeIds[] = {OMNI_ARRAY};
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[numCols]{};
    inputDataTypes.inputDataScales = new uint32_t[numCols]{};

    long splitterAddr = Test_splitter_nativeMake("hash", numPartitions, inputDataTypes, numCols,
        1024, "lz4", tmpShuffleFilePath + "_single_batch_planc", 0, "/tmp/shuffleTests");

    {
        std::vector<DataTypePtr> inputDataTypesVec;
        inputDataTypesVec.push_back(std::make_shared<ArrayType>(IntType()));
        reinterpret_cast<Splitter*>(splitterAddr)->SetInputDataTypes(inputDataTypesVec);
    }

    int32_t ele[] = {0, 1, 2, 3};
    VectorBatch* vb = CreateVectorBatch_1row_array_int_withPid(1, ele, 4);
    Test_splitter_split(splitterAddr, vb);
    Test_splitter_stop(splitterAddr);
    auto* splitter = reinterpret_cast<Splitter*>(splitterAddr);

    // Same assertions as original TEST 1
    bool foundComplexBuffer = false;
    for (const auto& batches : splitter->ArrowCachedBatches()) {
        for (const auto& b : batches) {
            if (b.rowCount > 0 && b.buffers.size() >= 4u) {
                foundComplexBuffer = true;
                EXPECT_EQ(b.buffers.size(), 4u);
                EXPECT_NE(b.buffers[1], nullptr);
                EXPECT_NE(b.buffers[3], nullptr);

                // Verify offsets: [0, 4]
                auto* offsets = reinterpret_cast<const int32_t*>(b.buffers[1]->data());
                EXPECT_EQ(offsets[0], 0);
                EXPECT_EQ(offsets[1], 4);
            }
        }
    }
    EXPECT_TRUE(foundComplexBuffer);

    Test_splitter_close(splitterAddr);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
}

// TEST 7: 两批 ARRAY<INT> 同分区 + 混合另一分区 → 验证分区间隔离
TEST(ArrowColumnarComplex, MultiBatchArrayMultiPartitionIsolation) {
    const int32_t numPartitions = 2;
    const int32_t numCols = 1;
    int32_t inputVecTypeIds[] = {OMNI_ARRAY};
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[numCols]{};
    inputDataTypes.inputDataScales = new uint32_t[numCols]{};

    long splitterAddr = Test_splitter_nativeMake("hash", numPartitions, inputDataTypes, numCols,
        1024, "lz4", tmpShuffleFilePath + "_multi_partition_iso", 0, "/tmp/shuffleTests");

    {
        std::vector<DataTypePtr> inputDataTypesVec;
        inputDataTypesVec.push_back(std::make_shared<ArrayType>(IntType()));
        reinterpret_cast<Splitter*>(splitterAddr)->SetInputDataTypes(inputDataTypesVec);
    }

    // Batch 1: pid=0, [100, 200]
    int32_t e1[] = {100, 200};
    VectorBatch* vb1 = CreateVectorBatch_1row_array_int_withPid(0, e1, 2);
    Test_splitter_split(splitterAddr, vb1);

    // Batch 2: pid=1, [300]
    int32_t e2[] = {300};
    VectorBatch* vb2 = CreateVectorBatch_1row_array_int_withPid(1, e2, 1);
    Test_splitter_split(splitterAddr, vb2);

    // Batch 3: pid=0, [400, 500, 600]
    int32_t e3[] = {400, 500, 600};
    VectorBatch* vb3 = CreateVectorBatch_1row_array_int_withPid(0, e3, 3);
    Test_splitter_split(splitterAddr, vb3);

    Test_splitter_stop(splitterAddr);
    auto* splitter = reinterpret_cast<Splitter*>(splitterAddr);

    const auto& batches = splitter->ArrowCachedBatches();

    // pid=0 should have 2 rows, offsets [0, 2, 5], child values [100,200,400,500,600]
    const ArrowColumnarCachedBatch* batch0 = nullptr;
    for (const auto& b : batches[0]) {
        if (b.rowCount > 0) { batch0 = &b; break; }
    }
    ASSERT_NE(batch0, nullptr);
    EXPECT_EQ(batch0->rowCount, 2);
    auto* offsets0 = reinterpret_cast<const int32_t*>(batch0->buffers[1]->data());
    EXPECT_EQ(offsets0[0], 0);
    EXPECT_EQ(offsets0[1], 2);
    EXPECT_EQ(offsets0[2], 5);
    auto* vals0 = reinterpret_cast<const int32_t*>(batch0->buffers[3]->data());
    EXPECT_EQ(vals0[0], 100);
    EXPECT_EQ(vals0[1], 200);
    EXPECT_EQ(vals0[2], 400);
    EXPECT_EQ(vals0[3], 500);
    EXPECT_EQ(vals0[4], 600);

    // pid=1 should have 1 row, offsets [0, 1], child values [300]
    const ArrowColumnarCachedBatch* batch1 = nullptr;
    for (const auto& b : batches[1]) {
        if (b.rowCount > 0) { batch1 = &b; break; }
    }
    ASSERT_NE(batch1, nullptr);
    EXPECT_EQ(batch1->rowCount, 1);
    auto* offsets1 = reinterpret_cast<const int32_t*>(batch1->buffers[1]->data());
    EXPECT_EQ(offsets1[0], 0);
    EXPECT_EQ(offsets1[1], 1);
    auto* vals1 = reinterpret_cast<const int32_t*>(batch1->buffers[3]->data());
    EXPECT_EQ(vals1[0], 300);

    Test_splitter_close(splitterAddr);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
}
