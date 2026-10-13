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
#include <arrow/buffer.h>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <memory>
#include <vector>

#include "../utils/test_utils.h"
#include "shuffle/arrow_frame.h"
#include "shuffle/splitter.h"
#include "shuffle/arrow_row_deserializer.h"
#include "io/SparkFile.hh"
#include <vector/vector_common.h>

using namespace omniruntime::vec;
using namespace omniruntime::type;

namespace {

// Helper: read entire file into a byte vector
std::vector<uint8_t> ReadFileBytes(const std::string& path) {
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    if (!f.is_open()) {
        throw std::runtime_error("Failed to open file: " + path);
    }
    int64_t fileSize = static_cast<int64_t>(f.tellg());
    f.seekg(0);
    std::vector<uint8_t> fileBytes(static_cast<size_t>(fileSize));
    f.read(reinterpret_cast<char*>(fileBytes.data()), fileSize);
    f.close();
    return fileBytes;
}

}  // anonymous namespace

// ============================================================================
// Test 1: EndToEndRowCount — SplitByRow → StopByRow → 读回行数对账
// ============================================================================
class PartitionedRowRoundTripTest : public testing::TestWithParam<bool> {};

TEST_P(PartitionedRowRoundTripTest, PreservesRowsAcrossPartitionsAndSpillThreshold)
{
    const char* tmpPath = "/tmp/shuffleTests/arrow_rt_row";
    DeletePathAll(tmpPath);

    // PID is vector column 0 for hash shuffle, but not part of the serialized schema.
    int32_t inputVecTypeIds[] = {OMNI_VARCHAR, OMNI_INT};
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[2]{};
    inputDataTypes.inputDataScales = new uint32_t[2]{};

    long splitterAddr = Test_splitter_nativeMake(
        "hash", 2, inputDataTypes, 2, 1024,
        "uncompressed", tmpPath, 0, "/tmp/shuffleTests",
        GetParam() ? 1 : 2ULL * 1024 * 1024 * 1024);

    int totalRows = 0;
    for (int b = 0; b < 5; ++b) {
        VectorBatch* vb = CreateVectorBatch_2column_1row_withPid(
            b % 2,            // pid alternates between 0 and 1
            "test_" + std::to_string(b),  // VARCHAR
            b * 10);                      // INT
        totalRows += vb->GetRowCount();
        Test_splitter_splitbyrow(splitterAddr, vb);
        if (GetParam()) {
            EXPECT_EQ(reinterpret_cast<Splitter *>(splitterAddr)->reclaimableBytes(), 0u);
        }
    }
    Test_splitter_stopbyrow(splitterAddr);
    if (GetParam()) EXPECT_GT(reinterpret_cast<Splitter *>(splitterAddr)->TotalBytesSpilled(), 0);
    Test_splitter_close(splitterAddr);

    // 读回：文件格式为 [4B 大端 size][文件头][row batch帧] × N（每批前有 4B 大端 size 前缀）
    auto fileBytes = ReadFileBytes(tmpPath);
    ASSERT_GT(fileBytes.size(), 0u);

    int64_t offset = 0;
    int64_t fileSize = static_cast<int64_t>(fileBytes.size());
    int64_t rowsRead = 0;
    std::vector<bool> seen(totalRows, false);
    while (offset < fileSize) {
        // 读 4B 大端 size 前缀 = payload 大小（headerSize + batchFrameSize）
        ASSERT_GE(fileSize - offset, 4);
        uint32_t payloadSize = 0;
        std::memcpy(&payloadSize, fileBytes.data() + offset, 4);
        // 大端转小端
        payloadSize = ((payloadSize & 0xFF) << 24) | ((payloadSize & 0xFF00) << 8) |
                      ((payloadSize & 0xFF0000) >> 8) | ((payloadSize & 0xFF000000) >> 24);
        offset += 4;

        // payload = [文件头][row batch帧]，按单批交给 RowShuffleParseInit（与 reduce 端一致）
        auto ctxR = RowShuffleParseInit(fileBytes.data() + offset,
                                        static_cast<int64_t>(payloadSize));
        ASSERT_TRUE(ctxR.ok()) << ctxR.status().ToString();
        auto ctx = std::move(*ctxR);
        ASSERT_NE(ctx, nullptr);
        EXPECT_EQ(ctx->header.layout, ShuffleLayout::ROW);

        auto st = RowShuffleParseNextBatch(*ctx);
        ASSERT_TRUE(st.ok()) << st.ToString();
        auto strings = std::make_unique<Vector<LargeStringContainer<std::string_view>>>(ctx->rowCnt);
        auto integers = std::make_unique<Vector<int32_t>>(ctx->rowCnt);
        BaseVector *decoded[] = {strings.get(), integers.get()};
        RowShuffleParseBatch(*ctx, decoded);
        for (int32_t row = 0; row < ctx->rowCnt; ++row) {
            ASSERT_FALSE(strings->IsNull(row));
            ASSERT_FALSE(integers->IsNull(row));
            const auto value = integers->GetValue(row);
            ASSERT_EQ(value % 10, 0);
            const auto id = value / 10;
            ASSERT_GE(id, 0);
            ASSERT_LT(id, totalRows);
            EXPECT_FALSE(seen[id]);
            seen[id] = true;
            EXPECT_EQ(strings->GetValue(row), "test_" + std::to_string(id));
        }
        rowsRead += ctx->rowCnt;

        RowShuffleParseClose(std::move(ctx));
        offset += static_cast<int64_t>(payloadSize);
    }
    EXPECT_EQ(rowsRead, totalRows);
    EXPECT_TRUE(std::all_of(seen.begin(), seen.end(), [](bool value) { return value; }));

    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
    if (IsFileExist(tmpPath)) {
        remove(tmpPath);
    }
}

// ============================================================================
// Test 2: SinglePartitionRowRoundTrip — 单分区行式 round-trip
// 迁移自: Split_Fixed_SinglePartition_SomeNullRow/SomeNullCol
// ============================================================================
class SinglePartitionRowRoundTripTest : public testing::TestWithParam<int> {};

TEST_P(SinglePartitionRowRoundTripTest, PreservesRowsWithAndWithoutReclaim)
{
    const char* tmpPath = "/tmp/shuffleTests/arrow_rt_row_single";
    DeletePathAll(tmpPath);

    int32_t inputVecTypeIds[] = {OMNI_VARCHAR, OMNI_INT};
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[2]{};
    inputDataTypes.inputDataScales = new uint32_t[2]{};

    // num_partitions=1 → singlePartitionFlag 路径
    long splitterAddr = Test_splitter_nativeMake(
        "single", 1, inputDataTypes, 2, 1024,
        "uncompressed", tmpPath, 0, "/tmp/shuffleTests",
        GetParam() == 2 ? 1 : 2ULL * 1024 * 1024 * 1024);

    int totalRows = 0;
    for (int b = 0; b < 5; ++b) {
        DataTypes batchTypes(std::vector<DataTypePtr>({VarcharType(), IntType()}));
        constexpr int rowsPerBatch = 1024;
        std::vector<std::string> varcharValue(rowsPerBatch);
        std::vector<int32_t> intValue(rowsPerBatch);
        for (int row = 0; row < rowsPerBatch; ++row) {
            varcharValue[row] = "single_" + std::to_string(b) + "_" + std::to_string(row);
            intValue[row] = b * rowsPerBatch + row;
        }
        VectorBatch* vb = CreateVectorBatch(batchTypes, rowsPerBatch, varcharValue.data(), intValue.data());
        totalRows += vb->GetRowCount();
        Test_splitter_splitbyrow(splitterAddr, vb);
        if (GetParam() == 1) {
            auto *splitter = reinterpret_cast<Splitter *>(splitterAddr);
            ASSERT_GT(splitter->reclaimableBytes(), 0u);
            EXPECT_GT(splitter->reclaim(1), 0);
            EXPECT_EQ(splitter->reclaimableBytes(), 0u);
        } else if (GetParam() == 2) {
            // The threshold must spill before Stop and without explicit reclaim.
            EXPECT_EQ(reinterpret_cast<Splitter *>(splitterAddr)->reclaimableBytes(), 0u);
        }
    }
    Test_splitter_stopbyrow(splitterAddr);
    if (GetParam() == 2) EXPECT_GT(reinterpret_cast<Splitter *>(splitterAddr)->TotalBytesSpilled(), 0);
    Test_splitter_close(splitterAddr);

    // 读回并验证
    auto fileBytes = ReadFileBytes(tmpPath);
    ASSERT_GT(fileBytes.size(), 0u);

    int64_t offset = 0;
    int64_t fileSize = static_cast<int64_t>(fileBytes.size());
    int64_t rowsRead = 0;
    std::vector<bool> seen(totalRows, false);
    while (offset < fileSize) {
        // 读 4B 大端 size 前缀 = payload 大小（headerSize + batchFrameSize）
        ASSERT_GE(fileSize - offset, 4);
        uint32_t payloadSize = 0;
        std::memcpy(&payloadSize, fileBytes.data() + offset, 4);
        // 大端转小端
        payloadSize = ((payloadSize & 0xFF) << 24) | ((payloadSize & 0xFF00) << 8) |
                      ((payloadSize & 0xFF0000) >> 8) | ((payloadSize & 0xFF000000) >> 24);
        offset += 4;

        // payload = [文件头][row batch帧]，按单批交给 RowShuffleParseInit（与 reduce 端一致）
        auto ctxR = RowShuffleParseInit(fileBytes.data() + offset,
                                        static_cast<int64_t>(payloadSize));
        ASSERT_TRUE(ctxR.ok()) << ctxR.status().ToString();
        auto ctx = std::move(*ctxR);
        ASSERT_NE(ctx, nullptr);
        EXPECT_EQ(ctx->header.layout, ShuffleLayout::ROW);

        auto st = RowShuffleParseNextBatch(*ctx);
        ASSERT_TRUE(st.ok()) << st.ToString();
        auto strings = std::make_unique<Vector<LargeStringContainer<std::string_view>>>(ctx->rowCnt);
        auto integers = std::make_unique<Vector<int32_t>>(ctx->rowCnt);
        BaseVector *decoded[] = {strings.get(), integers.get()};
        RowShuffleParseBatch(*ctx, decoded);
        for (int32_t row = 0; row < ctx->rowCnt; ++row) {
            ASSERT_FALSE(integers->IsNull(row));
            ASSERT_FALSE(strings->IsNull(row));
            const int32_t id = integers->GetValue(row);
            ASSERT_GE(id, 0);
            ASSERT_LT(id, totalRows);
            EXPECT_FALSE(seen[id]);
            seen[id] = true;
            const auto expected = "single_" + std::to_string(id / 1024) + "_" +
                std::to_string(id % 1024);
            EXPECT_EQ(strings->GetValue(row), expected);
        }
        rowsRead += ctx->rowCnt;

        RowShuffleParseClose(std::move(ctx));
        offset += static_cast<int64_t>(payloadSize);
    }
    EXPECT_EQ(rowsRead, totalRows);
    EXPECT_TRUE(std::all_of(seen.begin(), seen.end(), [](bool value) { return value; }));

    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
    if (IsFileExist(tmpPath)) {
        remove(tmpPath);
    }
}

INSTANTIATE_TEST_SUITE_P(ReclaimModes, SinglePartitionRowRoundTripTest, testing::Values(0, 1, 2));

INSTANTIATE_TEST_SUITE_P(ThresholdModes, PartitionedRowRoundTripTest, testing::Bool());
