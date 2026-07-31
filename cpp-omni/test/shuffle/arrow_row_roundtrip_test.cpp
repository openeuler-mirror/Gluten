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
#include <cstdint>
#include <cstring>
#include <fstream>
#include <memory>
#include <vector>

#include "../utils/test_utils.h"
#include "shuffle/arrow_frame.h"
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
TEST(ArrowRowRoundTrip, EndToEndRowCount)
{
    const char* tmpPath = "/tmp/shuffleTests/arrow_rt_row";
    DeletePathAll(tmpPath);

    int32_t inputVecTypeIds[] = {OMNI_INT, OMNI_VARCHAR, OMNI_INT};
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[3]{};
    inputDataTypes.inputDataScales = new uint32_t[3]{};

    long splitterAddr = Test_splitter_nativeMake(
        "hash", 2, inputDataTypes, 3, 1024,
        "uncompressed", tmpPath, 0, "/tmp/shuffleTests");

    int totalRows = 0;
    for (int b = 0; b < 5; ++b) {
        VectorBatch* vb = CreateVectorBatch_2column_1row_withPid(
            b % 2,            // pid alternates between 0 and 1
            "test_" + std::to_string(b),  // VARCHAR
            b * 10);                      // INT
        totalRows += vb->GetRowCount();
        Test_splitter_splitbyrow(splitterAddr, vb);
    }
    Test_splitter_stopbyrow(splitterAddr);
    Test_splitter_close(splitterAddr);

    // 读回：文件格式为 [4B 大端 size][文件头][row batch帧] × N（每批前有 4B 大端 size 前缀）
    auto fileBytes = ReadFileBytes(tmpPath);
    ASSERT_GT(fileBytes.size(), 0u);

    int64_t offset = 0;
    int64_t fileSize = static_cast<int64_t>(fileBytes.size());
    int64_t rowsRead = 0;
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
        rowsRead += ctx->rowCnt;

        RowShuffleParseClose(std::move(ctx));
        offset += static_cast<int64_t>(payloadSize);
    }
    EXPECT_EQ(rowsRead, totalRows);

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
TEST(ArrowRowRoundTrip, SinglePartitionRowRoundTrip)
{
    const char* tmpPath = "/tmp/shuffleTests/arrow_rt_row_single";
    DeletePathAll(tmpPath);

    int32_t inputVecTypeIds[] = {OMNI_INT, OMNI_VARCHAR, OMNI_INT};
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[3]{};
    inputDataTypes.inputDataScales = new uint32_t[3]{};

    // num_partitions=1 → singlePartitionFlag 路径
    long splitterAddr = Test_splitter_nativeMake(
        "single", 1, inputDataTypes, 3, 1024,
        "uncompressed", tmpPath, 0, "/tmp/shuffleTests");

    int totalRows = 0;
    for (int b = 0; b < 5; ++b) {
        VectorBatch* vb = CreateVectorBatch_2column_1row_withPid(
            0, "single_" + std::to_string(b), b * 10);
        totalRows += vb->GetRowCount();
        Test_splitter_splitbyrow(splitterAddr, vb);
    }
    Test_splitter_stopbyrow(splitterAddr);
    Test_splitter_close(splitterAddr);

    // 读回并验证
    auto fileBytes = ReadFileBytes(tmpPath);
    ASSERT_GT(fileBytes.size(), 0u);

    int64_t offset = 0;
    int64_t fileSize = static_cast<int64_t>(fileBytes.size());
    int64_t rowsRead = 0;
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
        rowsRead += ctx->rowCnt;

        RowShuffleParseClose(std::move(ctx));
        offset += static_cast<int64_t>(payloadSize);
    }
    EXPECT_EQ(rowsRead, totalRows);

    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
    if (IsFileExist(tmpPath)) {
        remove(tmpPath);
    }
}
