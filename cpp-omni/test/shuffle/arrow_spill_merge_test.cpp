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
#include "shuffle/arrow_type_bridge.h"
#include "shuffle/arrow_columnar_deserializer.h"
#include "io/SparkFile.hh"
#include <vector/vector_common.h>

using namespace omniruntime::vec;
using namespace omniruntime::type;

namespace {

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
// Test 1: SpillMerge_Uncompressed — explicit spill via TestForceSpill(),
//         merge via MergeSpilled() (mmap ReadAt), verify readback correct.
// ============================================================================
TEST(ArrowSpillMerge, SpillMerge_Uncompressed)
{
    const char* tmpPath = "/tmp/shuffleTests/arrow_spill_uncomp";
    DeletePathAll(tmpPath);

    int32_t inputVecTypeIds[] = {OMNI_INT, OMNI_LONG};
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[2]{};
    inputDataTypes.inputDataScales = new uint32_t[2]{};

    long splitterAddr = Test_splitter_nativeMake(
        "hash", 2, inputDataTypes, 2, 1024,
        "uncompressed", tmpPath, 0, "/tmp/shuffleTests");

    // Feed data
    int totalInputRows = 0;
    for (int b = 0; b < 20; ++b) {
        VectorBatch* vb = CreateVectorBatch_4col_withPid(2, 100);
        totalInputRows += vb->GetRowCount();
        Test_splitter_split(splitterAddr, vb);
    }

    auto* splitter = reinterpret_cast<Splitter*>(splitterAddr);

    // Explicitly force spill → sets isSpill=true, so Stop() calls MergeSpilled()
    splitter->TestForceSpill();
    EXPECT_GT(splitter->TotalBytesSpilled(), 0)
        << "TestForceSpill should have spilled data to temp files";

    Test_splitter_stop(splitterAddr);   // → MergeSpilled (because isSpill=true)
    Test_splitter_close(splitterAddr);

    // Read back and verify row count
    auto fileBytes = ReadFileBytes(tmpPath);
    ASSERT_GT(fileBytes.size(), 0u);

    int64_t consumed = 0;
    // 跳过第一个 batch 的 4B 大端 size 前缀
    consumed += 4;
    int64_t headerConsumed = 0;
    auto headerR = ReadFileHeader(fileBytes.data() + consumed,
                                  static_cast<int64_t>(fileBytes.size()) - consumed,
                                  &headerConsumed);
    ASSERT_TRUE(headerR.ok()) << headerR.status().ToString();
    auto header = *headerR;
    consumed += headerConsumed;

    EXPECT_EQ(header.version, kArrowShuffleVersion);
    EXPECT_EQ(header.layout, ShuffleLayout::COLUMNAR);

    int64_t totalRowsRead = 0;
    int64_t fileSize = static_cast<int64_t>(fileBytes.size());
    while (consumed < fileSize) {
        int64_t batchConsumed = 0;
        auto batchR = ReadColumnarBatch(
            fileBytes.data() + consumed,
            fileSize - consumed,
            header.schema,
            &batchConsumed);
        ASSERT_TRUE(batchR.ok()) << batchR.status().ToString();
        consumed += batchConsumed;
        totalRowsRead += (*batchR).rowCount;
        // 跳过下一个 batch 的 4B 大端 size 前缀 + 文件头
        if (consumed < fileSize) {
            // 读 4B 大端 size
            ASSERT_GE(fileSize - consumed, 4);
            uint32_t nextSize = 0;
            std::memcpy(&nextSize, fileBytes.data() + consumed, 4);
            // 大端转小端（payloadSize = headerSize + batchFrameSize，此处仅跳过，不依赖其值）
            nextSize = ((nextSize & 0xFF) << 24) | ((nextSize & 0xFF00) << 8) |
                       ((nextSize & 0xFF0000) >> 8) | ((nextSize & 0xFF000000) >> 24);
            (void)nextSize;
            consumed += 4;
            int64_t skipHeaderConsumed = 0;
            auto skipHeaderR = ReadFileHeader(fileBytes.data() + consumed,
                                              fileSize - consumed,
                                              &skipHeaderConsumed);
            ASSERT_TRUE(skipHeaderR.ok()) << "Skip header failed: " << skipHeaderR.status().ToString();
            consumed += skipHeaderConsumed;
        }
    }

    EXPECT_GT(totalRowsRead, 0);
    EXPECT_EQ(totalRowsRead, totalInputRows)
        << "Rows read from merged output should match total input rows";

    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
    DeletePathAll(tmpPath);
}

// ============================================================================
// Test 2: SpillMerge_Lz4 — explicit spill with lz4 compression, merge readback.
// ============================================================================
TEST(ArrowSpillMerge, SpillMerge_Lz4)
{
    const char* tmpPath = "/tmp/shuffleTests/arrow_spill_lz4";
    DeletePathAll(tmpPath);

    int32_t inputVecTypeIds[] = {OMNI_INT, OMNI_LONG};
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[2]{};
    inputDataTypes.inputDataScales = new uint32_t[2]{};

    long splitterAddr = Test_splitter_nativeMake(
        "hash", 2, inputDataTypes, 2, 4096,
        "lz4", tmpPath, 0, "/tmp/shuffleTests");

    int totalInputRows = 0;
    for (int b = 0; b < 20; ++b) {
        VectorBatch* vb = CreateVectorBatch_4col_withPid(2, 100);
        totalInputRows += vb->GetRowCount();
        Test_splitter_split(splitterAddr, vb);
    }

    auto* splitter = reinterpret_cast<Splitter*>(splitterAddr);

    splitter->TestForceSpill();
    EXPECT_GT(splitter->TotalBytesSpilled(), 0);

    Test_splitter_stop(splitterAddr);
    Test_splitter_close(splitterAddr);

    auto fileBytes = ReadFileBytes(tmpPath);
    ASSERT_GT(fileBytes.size(), 0u);

    // 压缩模式下文件是压缩格式（3B 帧头 + 压缩块），无法直接解析 Arrow 帧。
    // 单元测试无 JNI 环境无法创建 ShuffleReaderDeserializer 解压流，
    // 因此仅验证文件非空且 spill 发生（已由 TotalBytesSpilled > 0 验证）。
    // 压缩模式的完整 round-trip 验证依赖 TPCDS 集成测试。

    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
    DeletePathAll(tmpPath);
}
