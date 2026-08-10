/**
 * Copyright (C) 2025-2025. Huawei Technologies Co., Ltd. All rights reserved.
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
#include "shuffle/arrow_frame.h"
#include "shuffle/arrow_type_bridge.h"
#include "shuffle/arrow_columnar_deserializer.h"
#include "shuffle/arrow_row_deserializer.h"
#include "shuffle/type.h"

#include <cstring>
#include <vector>

using namespace omniruntime::type;

// ============================================================
// 双读分流测试：version/layout 识别 + 非法配对必报错
// ============================================================

// 测试1：Arrow 列式文件头 → ReadFileHeader 成功识别 COLUMNAR layout
TEST(ArrowDualRead, ReadsArrowColumnarByHeader)
{
    ArrowFileHeader hdr;
    hdr.version = kArrowShuffleVersion;
    hdr.layout = ShuffleLayout::COLUMNAR;
    hdr.schema.push_back(DataTypeToDescriptor(IntDataType::Instance()));
    hdr.schema.push_back(DataTypeToDescriptor(LongDataType::Instance()));
    hdr.schema.push_back(DataTypeToDescriptor(VarcharDataType::Instance()));

    auto w = WriteFileHeader(hdr);
    ASSERT_TRUE(w.ok()) << w.status().ToString();
    auto buf = *w;

    // Verify magic bytes
    ASSERT_GE(buf->size(), 6);
    EXPECT_EQ(buf->data()[0], 'O');
    EXPECT_EQ(buf->data()[1], 'M');
    EXPECT_EQ(buf->data()[2], 'S');
    EXPECT_EQ(buf->data()[3], 'A');
    EXPECT_EQ(buf->data()[4], kArrowShuffleVersion);

    // Verify layout byte (offset 5 after magic+version)
    EXPECT_EQ(static_cast<int>(buf->data()[5]),
              static_cast<int>(ShuffleLayout::COLUMNAR));

    // Full ReadFileHeader round-trip
    int64_t consumed = 0;
    auto r = ReadFileHeader(buf->data(), buf->size(), &consumed);
    ASSERT_TRUE(r.ok()) << r.status().ToString();
    auto back = *r;
    EXPECT_EQ(back.version, kArrowShuffleVersion);
    EXPECT_EQ(back.layout, ShuffleLayout::COLUMNAR);
    ASSERT_EQ(back.schema.size(), 3u);
    EXPECT_EQ(back.schema[0].typeId, OMNI_INT);
    EXPECT_EQ(back.schema[1].typeId, OMNI_LONG);
    EXPECT_EQ(back.schema[2].typeId, OMNI_VARCHAR);
}

// 测试2：Arrow 行式文件头 → ReadFileHeader 成功识别 ROW layout
TEST(ArrowDualRead, ReadsArrowRowByHeader)
{
    ArrowFileHeader hdr;
    hdr.version = kArrowShuffleVersion;
    hdr.layout = ShuffleLayout::ROW;
    hdr.schema.push_back(DataTypeToDescriptor(IntDataType::Instance()));
    hdr.schema.push_back(DataTypeToDescriptor(VarcharDataType::Instance()));

    auto w = WriteFileHeader(hdr);
    ASSERT_TRUE(w.ok()) << w.status().ToString();
    auto buf = *w;

    // Verify layout byte is ROW
    ASSERT_GE(buf->size(), 6);
    EXPECT_EQ(static_cast<int>(buf->data()[5]),
              static_cast<int>(ShuffleLayout::ROW));

    int64_t consumed = 0;
    auto r = ReadFileHeader(buf->data(), buf->size(), &consumed);
    ASSERT_TRUE(r.ok()) << r.status().ToString();
    auto back = *r;
    EXPECT_EQ(back.layout, ShuffleLayout::ROW);
}

// 测试3：非法 magic（无 OMSA 前缀）→ ReadFileHeader 报错
// 模拟"写 Arrow 读 proto"场景：proto 路径看到非 proto 数据也解析不了，
// 但 Arrow 路径看到非 OMSA 数据应直接报错。
TEST(ArrowDualRead, RejectsNonArrowMagic)
{
    // 构造无 magic 的随机字节（模拟旧 proto 数据从 Arrow 路径读取）
    uint8_t nonArrow[] = {0x08, 0x01, 0x10, 0x02, 0x00, 0x00, 0x00, 0x00,
                          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

    int64_t consumed = 0;
    auto r = ReadFileHeader(nonArrow, sizeof(nonArrow), &consumed);
    EXPECT_FALSE(r.ok()) << "Non-Arrow data should be rejected by ReadFileHeader";
}

// 测试4：正确的 magic 但非法 version → 报错
TEST(ArrowDualRead, RejectsUnsupportedArrowVersion)
{
    uint8_t bad[] = {'O', 'M', 'S', 'A', 99 /* unsupported version */, 0,
                     0, 0, 0, 0, 0, 0, 0, 0};

    int64_t consumed = 0;
    auto r = ReadFileHeader(bad, sizeof(bad), &consumed);
    EXPECT_FALSE(r.ok()) << "Unsupported Arrow version should be rejected";
}

// 测试5：正确的 magic + version 但非法 layout → 报错
TEST(ArrowDualRead, RejectsBadLayout)
{
    uint8_t bad[] = {'O', 'M', 'S', 'A', 1, 9 /* bad layout byte */,
                     0, 0, 0, 0, 0, 0, 0, 0};

    int64_t consumed = 0;
    auto r = ReadFileHeader(bad, sizeof(bad), &consumed);
    EXPECT_FALSE(r.ok()) << "Bad layout byte should be rejected";
}

// 测试6：RowShuffleParseInit 拒绝 COLUMNAR layout
// 模拟 JNI 层检测到 layout 不匹配时的报错行为
TEST(ArrowDualRead, RowShuffleParseInitRejectsColumnarLayout)
{
    ArrowFileHeader hdr;
    hdr.version = kArrowShuffleVersion;
    hdr.layout = ShuffleLayout::COLUMNAR;  // 故意写成 COLUMNAR，但走行式入口
    hdr.schema.push_back(DataTypeToDescriptor(IntDataType::Instance()));

    auto w = WriteFileHeader(hdr);
    ASSERT_TRUE(w.ok()) << w.status().ToString();
    auto buf = *w;

    // 用行式入口解析 → 应报错（layout 不符）
    auto ctxResult = RowShuffleParseInit(buf->data(), buf->size());
    EXPECT_FALSE(ctxResult.ok()) << "ROW init should reject COLUMNAR layout";
}

// 测试7：短数据（不足 6 字节）→ ReadFileHeader 报错
TEST(ArrowDualRead, RejectsTooShortData)
{
    uint8_t short_[] = {'O', 'M', 'S'};  // 不足 6 字节

    int64_t consumed = 0;
    auto r = ReadFileHeader(short_, sizeof(short_), &consumed);
    EXPECT_FALSE(r.ok()) << "Too-short data should be rejected";
}
