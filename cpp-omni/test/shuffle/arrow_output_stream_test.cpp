/**
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

#include <arrow/status.h>
#include <gtest/gtest.h>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "io/ArrowOutputStream.h"
#include "io/Compression.hh"
#include "io/OutputStream.hh"
#include "io/SparkFile.hh"

static std::string TmpPath(const char* tag) {
    return std::string("/tmp/arrow_out_") + tag + std::to_string(::getpid());
}

// 非压缩：单次 Write + Flush → 读回逐字节对账
TEST(ArrowOutputStream, WriteAndFlushToRawFile) {
    std::string path = TmpPath("raw");
    auto raw = spark::writeLocalFile(path);
    ArrowOutputStream out(std::make_unique<spark::BufferedOutputStream>(
        *spark::getDefaultPool(), raw.get(), 4096, 4096));

    const char* hello = "HELLO_ARROW";
    ASSERT_TRUE(out.Write(hello, 11).ok());
    ASSERT_TRUE(out.Flush().ok());
    ASSERT_TRUE(out.Close().ok());
    raw->close();

    FILE* f = std::fopen(path.c_str(), "rb");
    ASSERT_NE(f, nullptr);
    char buf[16] = {0};
    size_t n = std::fread(buf, 1, sizeof(buf), f);
    std::fclose(f);
    EXPECT_EQ(n, 11u);
    EXPECT_EQ(std::string(buf, n), std::string("HELLO_ARROW"));
    std::remove(path.c_str());
}

// 非压缩：多次小 Write 累积后 Flush → 读回逐字节对账
TEST(ArrowOutputStream, MultipleWritesAccumulateBeforeFlush) {
    std::string path = TmpPath("multi");
    auto raw = spark::writeLocalFile(path);
    ArrowOutputStream out(std::make_unique<spark::BufferedOutputStream>(
        *spark::getDefaultPool(), raw.get(), 4096, 4096));

    std::vector<uint8_t> expected;
    for (int i = 0; i < 100; ++i) {
        uint8_t b = static_cast<uint8_t>(i);
        ASSERT_TRUE(out.Write(&b, 1).ok());
        expected.push_back(b);
    }
    ASSERT_TRUE(out.Flush().ok());
    ASSERT_TRUE(out.Close().ok());
    raw->close();

    FILE* f = std::fopen(path.c_str(), "rb");
    ASSERT_NE(f, nullptr);
    std::vector<uint8_t> got(expected.size());
    size_t n = std::fread(got.data(), 1, got.size(), f);
    std::fclose(f);
    EXPECT_EQ(n, expected.size());
    EXPECT_EQ(got, expected);
    std::remove(path.c_str());
}

// 非压缩：较大数据块 Write → 读回逐字节对账（4096 字节，跨 buffer 块边界）
TEST(ArrowOutputStream, LargeBlockWriteAndVerify) {
    std::string path = TmpPath("large");
    auto raw = spark::writeLocalFile(path);
    ArrowOutputStream out(std::make_unique<spark::BufferedOutputStream>(
        *spark::getDefaultPool(), raw.get(), 4096, 4096));

    std::vector<uint8_t> expected(4096);
    for (size_t i = 0; i < expected.size(); ++i) {
        expected[i] = static_cast<uint8_t>(i & 0xFF);
    }
    ASSERT_TRUE(out.Write(expected.data(), static_cast<int64_t>(expected.size())).ok());
    ASSERT_TRUE(out.Flush().ok());
    ASSERT_TRUE(out.Close().ok());
    raw->close();

    FILE* f = std::fopen(path.c_str(), "rb");
    ASSERT_NE(f, nullptr);
    std::vector<uint8_t> got(expected.size());
    size_t n = std::fread(got.data(), 1, got.size(), f);
    std::fclose(f);
    EXPECT_EQ(n, expected.size());
    EXPECT_EQ(got, expected);
    std::remove(path.c_str());
}

// 非压缩：空写入 → 读回验证文件存在但为空
TEST(ArrowOutputStream, EmptyWriteProducesEmptyFile) {
    std::string path = TmpPath("empty");
    auto raw = spark::writeLocalFile(path);
    ArrowOutputStream out(std::make_unique<spark::BufferedOutputStream>(
        *spark::getDefaultPool(), raw.get(), 4096, 4096));

    ASSERT_TRUE(out.Flush().ok());
    ASSERT_TRUE(out.Close().ok());
    raw->close();

    FILE* f = std::fopen(path.c_str(), "rb");
    ASSERT_NE(f, nullptr);
    std::fseek(f, 0, SEEK_END);
    long size = std::ftell(f);
    std::fclose(f);
    EXPECT_EQ(size, 0L);
    std::remove(path.c_str());
}

// 压缩路径（lz4/zstd/snappy/zlib）：验证 Write/Flush/Close 成功 + 文件非空
// 注：内容正确性由后续读侧 Task 或集成 Task 12 负责验证
TEST(ArrowOutputStream, CompressedWriteSmoke) {
    struct Case {
        spark::CompressionKind kind;
        const char* name;
    };
    const Case cases[] = {
        {spark::CompressionKind_LZ4,    "lz4"},
        {spark::CompressionKind_ZSTD,   "zstd"},
        {spark::CompressionKind_SNAPPY, "snappy"},
        {spark::CompressionKind_ZLIB,   "zlib"},
    };

    for (const auto& c : cases) {
        std::string path = TmpPath(c.name);
        auto raw = spark::writeLocalFile(path);
        auto out = ArrowOutputStream::Make(
            raw.get(), c.kind, spark::CompressionStrategy_SPEED, 4096, 4096, *spark::getDefaultPool());
        ASSERT_NE(out, nullptr) << "Make returned null for " << c.name;

        std::vector<uint8_t> data(4096);
        for (size_t i = 0; i < data.size(); ++i) {
            data[i] = static_cast<uint8_t>(i & 0xFF);
        }
        ASSERT_TRUE(out->Write(data.data(), static_cast<int64_t>(data.size())).ok())
            << "Write failed for " << c.name;
        ASSERT_TRUE(out->Flush().ok()) << "Flush failed for " << c.name;
        ASSERT_TRUE(out->Close().ok()) << "Close failed for " << c.name;
        raw->close();

        FILE* f = std::fopen(path.c_str(), "rb");
        ASSERT_NE(f, nullptr) << "Cannot open output file for " << c.name;
        std::fseek(f, 0, SEEK_END);
        long fileSize = std::ftell(f);
        std::fclose(f);
        EXPECT_GT(fileSize, 0L) << "Compressed file should not be empty for " << c.name;
        std::remove(path.c_str());
    }
}

// 非压缩通过 Make 工厂创建 → 与直接构造 BufferedOutputStream 等价，逐字节对账
TEST(ArrowOutputStream, MakeWithNoCompressionRoundTrip) {
    std::string path = TmpPath("make_none");
    auto raw = spark::writeLocalFile(path);
    auto out = ArrowOutputStream::Make(
        raw.get(), spark::CompressionKind_NONE, spark::CompressionStrategy_SPEED, 4096, 4096, *spark::getDefaultPool());
    ASSERT_NE(out, nullptr);

    std::vector<uint8_t> expected(2048);
    for (size_t i = 0; i < expected.size(); ++i) {
        expected[i] = static_cast<uint8_t>(i & 0xFF);
    }
    ASSERT_TRUE(out->Write(expected.data(), static_cast<int64_t>(expected.size())).ok());
    ASSERT_TRUE(out->Flush().ok());
    ASSERT_TRUE(out->Close().ok());
    raw->close();

    FILE* f = std::fopen(path.c_str(), "rb");
    ASSERT_NE(f, nullptr);
    std::vector<uint8_t> got(expected.size());
    size_t n = std::fread(got.data(), 1, got.size(), f);
    std::fclose(f);
    EXPECT_EQ(n, expected.size());
    EXPECT_EQ(got, expected);
    std::remove(path.c_str());
}

// 写后 Close 再写应报错
TEST(ArrowOutputStream, WriteAfterCloseReturnsError) {
    std::string path = TmpPath("afterclose");
    auto raw = spark::writeLocalFile(path);
    ArrowOutputStream out(std::make_unique<spark::BufferedOutputStream>(
        *spark::getDefaultPool(), raw.get(), 4096, 4096));

    ASSERT_TRUE(out.Close().ok());
    EXPECT_TRUE(out.closed());

    const char data[] = "should_fail";
    auto status = out.Write(data, sizeof(data));
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.IsInvalid());

    raw->close();
    std::remove(path.c_str());
}
