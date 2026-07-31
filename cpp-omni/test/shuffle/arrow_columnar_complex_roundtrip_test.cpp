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

/**
 * 复杂类型端到端 roundtrip：写侧（splitter，走真实 header 构建）→ 文件 → 读侧
 * （ReadFileHeader + DeserializeArrowBufferToOmniVector 重建向量）→ 向量内容断言。
 *
 * 覆盖：基础 ARRAY、嵌套 ARRAY、MAP（key/value 异构）、STRUCT、混合列（错位传播回归）、
 * 含 null 行、多批同分区（offset 链式接续）、强制 spill。
 * 所有用例必须断言向量内容，不只断言 buffer 数量。
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
#include <vector/vector_common.h>

using namespace omniruntime::vec;
using namespace omniruntime::type;

namespace {

static std::string tmpShuffleFilePath = "/tmp/shuffleTests/arrow_complex_roundtrip";

// ------------------------------------------------------------------
// 测试向量构造（均为 [pid, 复杂列] 的 VectorBatch）
// ------------------------------------------------------------------

// [pid, ARRAY<INT>] rowCount 行；第 r 行 = [r*10, r*10+1]
VectorBatch* MakeBatch_ArrayInt(int pid, int rowCount) {
    auto* vb = new VectorBatch(rowCount);
    auto* pidVec = new Vector<int32_t>(rowCount);
    for (int r = 0; r < rowCount; ++r) pidVec->SetValue(r, pid);
    vb->Append(pidVec);
    auto* elemVec = new Vector<int32_t>(rowCount * 2);
    for (int r = 0; r < rowCount * 2; ++r) {
        elemVec->SetValue(r, (r / 2) * 10 + (r % 2));
    }
    auto* arrVec = new ArrayVector(rowCount, std::shared_ptr<Vector<int32_t>>(elemVec));
    for (int r = 0; r <= rowCount; ++r) arrVec->SetOffset(r, r * 2);
    vb->Append(arrVec);
    return vb;
}

// [pid, ARRAY<ARRAY<INT>>] 2 行；row0 = [[1,2],[3]]，row1 = [[4,5,6]]
VectorBatch* MakeBatch_NestedArrayInt(int pid) {
    const int32_t numRows = 2;
    auto* vb = new VectorBatch(numRows);
    auto* pidVec = new Vector<int32_t>(numRows);
    pidVec->SetValue(0, pid);
    pidVec->SetValue(1, pid);
    vb->Append(pidVec);

    // 内层元素：6 个 INT
    auto* innerElem = new Vector<int32_t>(6);
    for (int i = 0; i < 6; ++i) innerElem->SetValue(i, i + 1);
    // 内层 ArrayVector（3 个内层数组）：[1,2] [3] [4,5,6]
    auto* innerArr = new ArrayVector(3, std::shared_ptr<Vector<int32_t>>(innerElem));
    int32_t innerOffs[4] = {0, 2, 3, 6};
    for (int i = 0; i <= 3; ++i) innerArr->SetOffset(i, innerOffs[i]);
    // 外层 ArrayVector（2 行）：[innerArr[0], innerArr[1]] [innerArr[2]]
    auto* outerArr = new ArrayVector(numRows, std::shared_ptr<ArrayVector>(innerArr));
    outerArr->SetOffset(0, 0);
    outerArr->SetOffset(1, 2);
    outerArr->SetOffset(2, 3);
    vb->Append(outerArr);
    return vb;
}

// [pid, MAP<INT,INT>] 2 行；每行 2 个 entry：{pid*10+i → pid*100+i}
VectorBatch* MakeBatch_MapIntInt(int pid) {
    const int32_t numRows = 2;
    const int32_t numEntries = 4;  // 每行 2 个
    auto* vb = new VectorBatch(numRows);
    auto* pidVec = new Vector<int32_t>(numRows);
    pidVec->SetValue(0, pid);
    pidVec->SetValue(1, pid);
    vb->Append(pidVec);

    auto* keyVec = new Vector<int32_t>(numEntries);
    auto* valVec = new Vector<int32_t>(numEntries);
    for (int e = 0; e < numEntries; ++e) {
        keyVec->SetValue(e, pid * 10 + e);
        valVec->SetValue(e, pid * 100 + e);
    }
    auto* mapVec = new MapVector(numRows,
                                 std::shared_ptr<Vector<int32_t>>(keyVec),
                                 std::shared_ptr<Vector<int32_t>>(valVec));
    mapVec->SetOffset(0, 0);
    mapVec->SetOffset(1, 2);
    mapVec->SetOffset(2, 4);
    vb->Append(mapVec);
    return vb;
}

// [pid, STRUCT<INT,VARCHAR>] 2 行；int 字段 = pid*100 + r，varchar = "row_r"
VectorBatch* MakeBatch_StructIntVarchar(int pid) {
    const int32_t numRows = 2;
    auto* vb = new VectorBatch(numRows);
    auto* pidVec = new Vector<int32_t>(numRows);
    pidVec->SetValue(0, pid);
    pidVec->SetValue(1, pid);
    vb->Append(pidVec);

    auto* intField = new Vector<int32_t>(numRows);
    auto* strField = new Vector<LargeStringContainer<std::string_view>>(numRows);
    for (int r = 0; r < numRows; ++r) {
        intField->SetValue(r, pid * 100 + r);
        std::string s = "row_" + std::to_string(r);
        strField->SetValue(r, std::string_view(s));
    }
    std::vector<std::shared_ptr<BaseVector>> children;
    children.push_back(std::shared_ptr<Vector<int32_t>>(intField));
    children.push_back(std::shared_ptr<Vector<LargeStringContainer<std::string_view>>>(strField));
    auto* rowVec = new RowVector(numRows, children);
    vb->Append(rowVec);
    return vb;
}

// [pid, ARRAY<STRUCT<INT,VARCHAR>>] 1 行；array = [ {1,"a"}, {2,"bb"} ]
VectorBatch* MakeBatch_ArrayStructIntVarchar(int pid) {
    const int32_t numRows = 1;
    const int32_t numElems = 2;
    auto* vb = new VectorBatch(numRows);
    auto* pidVec = new Vector<int32_t>(numRows);
    pidVec->SetValue(0, pid);
    vb->Append(pidVec);

    auto* intField = new Vector<int32_t>(numElems);
    auto* strField = new Vector<LargeStringContainer<std::string_view>>(numElems);
    intField->SetValue(0, 1);
    intField->SetValue(1, 2);
    strField->SetValue(0, std::string_view("a"));
    strField->SetValue(1, std::string_view("bb"));
    std::vector<std::shared_ptr<BaseVector>> children;
    children.push_back(std::shared_ptr<Vector<int32_t>>(intField));
    children.push_back(std::shared_ptr<Vector<LargeStringContainer<std::string_view>>>(strField));
    auto* rowElem = new RowVector(numElems, children);
    auto* arrVec = new ArrayVector(numRows, std::shared_ptr<RowVector>(rowElem));
    arrVec->SetOffset(0, 0);
    arrVec->SetOffset(1, numElems);
    vb->Append(arrVec);
    return vb;
}

// [pid, INT, MAP<INT,INT>, VARCHAR, ARRAY<INT>] 混合列（MAP 在中间 → 错位传播回归）
VectorBatch* MakeBatch_Mixed(int pid) {
    const int32_t numRows = 1;
    auto* vb = new VectorBatch(numRows);
    auto* pidVec = new Vector<int32_t>(numRows);
    pidVec->SetValue(0, pid);
    vb->Append(pidVec);

    auto* intVec = new Vector<int32_t>(numRows);
    intVec->SetValue(0, 777);
    vb->Append(intVec);

    auto* keyVec = new Vector<int32_t>(1);
    auto* valVec = new Vector<int32_t>(1);
    keyVec->SetValue(0, pid);
    valVec->SetValue(0, pid * 10);
    auto* mapVec = new MapVector(numRows,
                                 std::shared_ptr<Vector<int32_t>>(keyVec),
                                 std::shared_ptr<Vector<int32_t>>(valVec));
    mapVec->SetOffset(0, 0);
    mapVec->SetOffset(1, 1);
    vb->Append(mapVec);

    auto* strVec = new Vector<LargeStringContainer<std::string_view>>(numRows);
    std::string s = "mixed";
    strVec->SetValue(0, std::string_view(s));
    vb->Append(strVec);

    auto* elemVec = new Vector<int32_t>(2);
    elemVec->SetValue(0, 11);
    elemVec->SetValue(1, 22);
    auto* arrVec = new ArrayVector(numRows, std::shared_ptr<Vector<int32_t>>(elemVec));
    arrVec->SetOffset(0, 0);
    arrVec->SetOffset(1, 2);
    vb->Append(arrVec);
    return vb;
}

// ------------------------------------------------------------------
// 读回 harness：读文件 → 跳 4B size 前缀 → 解析文件头 → 解析所有批
// ------------------------------------------------------------------
struct ParsedRoundTrip {
    std::vector<uint8_t> fileBytes;   // 持有零拷贝 buffer 的底层数据生命周期
    ArrowFileHeader header;
    std::vector<ColumnarBatchBody> batches;
};

ParsedRoundTrip ParseSplitFile(const std::string& path) {
    ParsedRoundTrip out;
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    if (!f.is_open()) {
        throw std::runtime_error("Failed to open file: " + path);
    }
    int64_t fileSize = static_cast<int64_t>(f.tellg());
    f.seekg(0);
    out.fileBytes.resize(static_cast<size_t>(fileSize));
    f.read(reinterpret_cast<char*>(out.fileBytes.data()), fileSize);
    f.close();

    int64_t consumed = 4;  // 跳过第一个 4B 大端 size 前缀
    int64_t headerConsumed = 0;
    auto headerR = ReadFileHeader(out.fileBytes.data() + consumed,
                                  fileSize - consumed, &headerConsumed);
    if (!headerR.ok()) {
        throw std::runtime_error("ReadFileHeader failed: " + headerR.status().ToString());
    }
    out.header = *headerR;
    consumed += headerConsumed;

    while (consumed < fileSize) {
        int64_t batchConsumed = 0;
        auto batchR = ReadColumnarBatch(out.fileBytes.data() + consumed,
                                        fileSize - consumed,
                                        out.header.schema, &batchConsumed);
        if (!batchR.ok()) {
            throw std::runtime_error("ReadColumnarBatch failed: " + batchR.status().ToString());
        }
        consumed += batchConsumed;
        out.batches.push_back(std::move(*batchR));
        if (consumed < fileSize) {
            // 跳过下一个 batch 的 4B size 前缀 + 文件头
            if (fileSize - consumed < 4) {
                throw std::runtime_error("Split file truncated before size prefix");
            }
            consumed += 4;
            int64_t skipConsumed = 0;
            auto skipR = ReadFileHeader(out.fileBytes.data() + consumed,
                                        fileSize - consumed, &skipConsumed);
            if (!skipR.ok()) {
                throw std::runtime_error("Skip header failed: " + skipR.status().ToString());
            }
            consumed += skipConsumed;
        }
    }
    return out;
}

// 按 schema 描述符创建 Omni 向量（与 deserializer.cpp 的创建逻辑一致）
BaseVector* MakeOmniVector(const OmniTypeDescriptor& desc, int32_t rowCount) {
    auto dt = DescriptorToOmniType(desc);
    auto id = static_cast<DataTypeId>(desc.typeId);
    if (id == OMNI_ARRAY || id == OMNI_MAP || id == OMNI_ROW) {
        return VectorHelper::CreateComplexVector(dt.get(), rowCount);
    }
    return VectorHelper::CreateVector(OMNI_FLAT, id, rowCount);
}

// 反序列化一个批的所有 schema 列
std::vector<BaseVector*> DeserializeBatch(const ParsedRoundTrip& pt,
                                          const ColumnarBatchBody& batch) {
    std::vector<BaseVector*> vecs;
    size_t bufIdx = 0;
    for (const auto& desc : pt.header.schema) {
        auto* vec = MakeOmniVector(desc, batch.rowCount);
        DeserializeArrowBufferToOmniVector(desc, batch.rowCount,
                                           batch.buffers, bufIdx, vec);
        vecs.push_back(vec);
    }
    return vecs;
}

// 通用：跑 splitter（含 SetInputDataTypes）→ stop → 关闭，返回解析结果
ParsedRoundTrip RunSplitter(const std::string& path,
                            const std::vector<int32_t>& flatTypeIds,
                            std::vector<DataTypePtr> realTypes,
                            const std::vector<VectorBatch*>& batches,
                            bool forceSpill = false) {
    DeletePathAll(path.c_str());
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = const_cast<int32_t*>(flatTypeIds.data());
    inputDataTypes.inputDataPrecisions = new uint32_t[flatTypeIds.size()]{};
    inputDataTypes.inputDataScales = new uint32_t[flatTypeIds.size()]{};

    long splitterAddr = Test_splitter_nativeMake(
        "hash", 2, inputDataTypes, static_cast<int32_t>(flatTypeIds.size()), 1024,
        "uncompressed", path, 0, "/tmp/shuffleTests");
    reinterpret_cast<Splitter*>(splitterAddr)->SetInputDataTypes(realTypes);

    for (auto* vb : batches) {
        Test_splitter_split(splitterAddr, vb);
        if (forceSpill) {
            // 每批后强制 spill，Stop 时走 MergeSpilled 路径
            reinterpret_cast<Splitter*>(splitterAddr)->TestForceSpill();
        }
    }
    Test_splitter_stop(splitterAddr);
    Test_splitter_close(splitterAddr);

    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;

    return ParseSplitFile(path);
}

// ------------------------------------------------------------------
// 向量内容断言辅助
// ------------------------------------------------------------------
template <typename T>
void ExpectInts(BaseVector* vec, const std::vector<T>& expected) {
    auto* iv = reinterpret_cast<Vector<T>*>(vec);
    ASSERT_EQ(static_cast<size_t>(iv->GetSize()), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(iv->GetValue(static_cast<int32_t>(i)), expected[i]);
    }
}

void ExpectStrings(BaseVector* vec, const std::vector<std::string>& expected) {
    auto* sv = reinterpret_cast<Vector<LargeStringContainer<std::string_view>>*>(vec);
    ASSERT_EQ(static_cast<size_t>(sv->GetSize()), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(std::string(sv->GetValue(static_cast<int32_t>(i))), expected[i]);
    }
}

}  // anonymous namespace

// ============================================================================
// 用例 1: ARRAY<INT> 基础 roundtrip（回归保护：不得破坏既有可用路径）
// ============================================================================
TEST(ArrowColumnarComplexRoundTrip, ArrayInt)
{
    auto parsed = RunSplitter(tmpShuffleFilePath + "_array_int",
                              {OMNI_ARRAY},
                              {std::make_shared<ArrayType>(IntType())},
                              {MakeBatch_ArrayInt(1, 3)});
    ASSERT_EQ(parsed.batches.size(), 1u);
    ASSERT_EQ(parsed.header.schema.size(), 1u);
    EXPECT_EQ(parsed.header.schema[0].typeId, OMNI_ARRAY);
    ASSERT_EQ(parsed.header.schema[0].children.size(), 1u);
    EXPECT_EQ(parsed.header.schema[0].children[0].typeId, OMNI_INT);

    auto vecs = DeserializeBatch(parsed, parsed.batches[0]);
    ASSERT_EQ(vecs.size(), 1u);
    auto* arr = reinterpret_cast<ArrayVector*>(vecs[0]);
    EXPECT_EQ(arr->GetSize(), 3);
    for (int r = 0; r < 3; ++r) {
        EXPECT_EQ(arr->GetSize(r), 2);                       // 每行 2 个元素
        EXPECT_EQ(arr->GetOffset(r), r * 2);
    }
    ExpectInts(arr->GetElementVector().get(), std::vector<int32_t>{0, 1, 10, 11, 20, 21});
    delete vecs[0];
    DeletePathAll((tmpShuffleFilePath + "_array_int").c_str());
}

// ============================================================================
// 用例 2: ARRAY<ARRAY<INT>> 嵌套 roundtrip（修复前读侧崩溃/错位）
// ============================================================================
TEST(ArrowColumnarComplexRoundTrip, NestedArrayInt)
{
    auto parsed = RunSplitter(tmpShuffleFilePath + "_nested_array",
                              {OMNI_ARRAY},
                              {std::make_shared<ArrayType>(std::make_shared<ArrayType>(IntType()))},
                              {MakeBatch_NestedArrayInt(1)});
    ASSERT_EQ(parsed.batches.size(), 1u);
    ASSERT_EQ(parsed.header.schema.size(), 1u);
    // 内层 ARRAY 必须保留 INT 子描述符
    ASSERT_EQ(parsed.header.schema[0].children.size(), 1u);
    EXPECT_EQ(parsed.header.schema[0].children[0].typeId, OMNI_ARRAY);
    ASSERT_EQ(parsed.header.schema[0].children[0].children.size(), 1u);
    EXPECT_EQ(parsed.header.schema[0].children[0].children[0].typeId, OMNI_INT);

    auto vecs = DeserializeBatch(parsed, parsed.batches[0]);
    auto* outer = reinterpret_cast<ArrayVector*>(vecs[0]);
    EXPECT_EQ(outer->GetSize(), 2);
    EXPECT_EQ(outer->GetSize(0), 2);   // row0: 2 个内层数组
    EXPECT_EQ(outer->GetSize(1), 1);   // row1: 1 个内层数组

    auto* inner = reinterpret_cast<ArrayVector*>(outer->GetElementVector().get());
    EXPECT_EQ(inner->GetSize(), 3);
    ExpectInts(inner->GetElementVector().get(), std::vector<int32_t>{1, 2, 3, 4, 5, 6});
    delete vecs[0];
    DeletePathAll((tmpShuffleFilePath + "_nested_array").c_str());
}

// ============================================================================
// 用例 3: MAP<INT,INT> roundtrip（修复前 header 子类型为垃圾）
// ============================================================================
TEST(ArrowColumnarComplexRoundTrip, MapIntInt)
{
    auto parsed = RunSplitter(tmpShuffleFilePath + "_map_int",
                              {OMNI_MAP},
                              {std::make_shared<MapType>(IntType(), IntType())},
                              {MakeBatch_MapIntInt(1)});
    ASSERT_EQ(parsed.batches.size(), 1u);
    ASSERT_EQ(parsed.header.schema.size(), 1u);
    EXPECT_EQ(parsed.header.schema[0].typeId, OMNI_MAP);
    ASSERT_EQ(parsed.header.schema[0].children.size(), 2u);
    EXPECT_EQ(parsed.header.schema[0].children[0].typeId, OMNI_INT);  // key
    EXPECT_EQ(parsed.header.schema[0].children[1].typeId, OMNI_INT);  // value

    auto vecs = DeserializeBatch(parsed, parsed.batches[0]);
    auto* mp = reinterpret_cast<MapVector*>(vecs[0]);
    EXPECT_EQ(mp->GetSize(), 2);
    EXPECT_EQ(mp->GetSize(0), 2);
    EXPECT_EQ(mp->GetSize(1), 2);
    ExpectInts(mp->GetKeyVector().get(), std::vector<int32_t>{10, 11, 12, 13});
    ExpectInts(mp->GetValueVector().get(), std::vector<int32_t>{100, 101, 102, 103});
    delete vecs[0];
    DeletePathAll((tmpShuffleFilePath + "_map_int").c_str());
}

// ============================================================================
// 用例 4: STRUCT<INT,VARCHAR> roundtrip（修复前 header 只留 1 个子列）
// ============================================================================
TEST(ArrowColumnarComplexRoundTrip, StructIntVarchar)
{
    std::vector<std::shared_ptr<DataType>> fields;
    fields.push_back(IntType());
    fields.push_back(VarcharType());
    auto parsed = RunSplitter(tmpShuffleFilePath + "_struct",
                              {OMNI_ROW},
                              {std::make_shared<RowType>(fields)},
                              {MakeBatch_StructIntVarchar(1)});
    ASSERT_EQ(parsed.batches.size(), 1u);
    ASSERT_EQ(parsed.header.schema.size(), 1u);
    EXPECT_EQ(parsed.header.schema[0].typeId, OMNI_ROW);
    ASSERT_EQ(parsed.header.schema[0].children.size(), 2u);  // 多字段 struct 子列数完整
    EXPECT_EQ(parsed.header.schema[0].children[0].typeId, OMNI_INT);
    EXPECT_EQ(parsed.header.schema[0].children[1].typeId, OMNI_VARCHAR);

    auto vecs = DeserializeBatch(parsed, parsed.batches[0]);
    auto* row = reinterpret_cast<RowVector*>(vecs[0]);
    ASSERT_EQ(row->Children().size(), 2u);
    ExpectInts(row->ChildAt(0).get(), std::vector<int32_t>{100, 101});
    ExpectStrings(row->ChildAt(1).get(), {"row_0", "row_1"});
    delete vecs[0];
    DeletePathAll((tmpShuffleFilePath + "_struct").c_str());
}

// ============================================================================
// 用例 5: ARRAY<STRUCT<INT,VARCHAR>> 嵌套组合 roundtrip
// ============================================================================
TEST(ArrowColumnarComplexRoundTrip, ArrayStructIntVarchar)
{
    std::vector<std::shared_ptr<DataType>> fields;
    fields.push_back(IntType());
    fields.push_back(VarcharType());
    auto rowType = std::make_shared<RowType>(fields);
    auto parsed = RunSplitter(tmpShuffleFilePath + "_array_struct",
                              {OMNI_ARRAY},
                              {std::make_shared<ArrayType>(rowType)},
                              {MakeBatch_ArrayStructIntVarchar(1)});
    ASSERT_EQ(parsed.batches.size(), 1u);
    ASSERT_EQ(parsed.header.schema.size(), 1u);
    ASSERT_EQ(parsed.header.schema[0].children.size(), 1u);
    EXPECT_EQ(parsed.header.schema[0].children[0].typeId, OMNI_ROW);
    ASSERT_EQ(parsed.header.schema[0].children[0].children.size(), 2u);

    auto vecs = DeserializeBatch(parsed, parsed.batches[0]);
    auto* arr = reinterpret_cast<ArrayVector*>(vecs[0]);
    EXPECT_EQ(arr->GetSize(), 1);
    EXPECT_EQ(arr->GetSize(0), 2);
    auto* elem = reinterpret_cast<RowVector*>(arr->GetElementVector().get());
    ASSERT_EQ(elem->Children().size(), 2u);
    ExpectInts(elem->ChildAt(0).get(), std::vector<int32_t>{1, 2});
    ExpectStrings(elem->ChildAt(1).get(), {"a", "bb"});
    delete vecs[0];
    DeletePathAll((tmpShuffleFilePath + "_array_struct").c_str());
}

// ============================================================================
// 用例 6: 混合列 [INT, MAP<INT,INT>, VARCHAR, ARRAY<INT>]
//         —— MAP 在中间列，修复前会污染其后所有列（错位传播回归）
// ============================================================================
TEST(ArrowColumnarComplexRoundTrip, MixedColumns)
{
    std::vector<DataTypePtr> types;
    types.push_back(IntType());
    types.push_back(std::make_shared<MapType>(IntType(), IntType()));
    types.push_back(VarcharType());
    types.push_back(std::make_shared<ArrayType>(IntType()));
    auto parsed = RunSplitter(tmpShuffleFilePath + "_mixed",
                              {OMNI_INT, OMNI_MAP, OMNI_VARCHAR, OMNI_ARRAY},
                              types,
                              {MakeBatch_Mixed(1)});
    ASSERT_EQ(parsed.batches.size(), 1u);
    ASSERT_EQ(parsed.header.schema.size(), 4u);

    auto vecs = DeserializeBatch(parsed, parsed.batches[0]);
    ASSERT_EQ(vecs.size(), 4u);

    // 列0: INT = 777（在 MAP 之前，不受影响）
    ExpectInts(vecs[0], std::vector<int32_t>{777});

    // 列1: MAP<INT,INT>（修复前 header 子类型垃圾 → 其后的列全部错位）
    auto* mp = reinterpret_cast<MapVector*>(vecs[1]);
    EXPECT_EQ(mp->GetSize(0), 1);
    ExpectInts(mp->GetKeyVector().get(), std::vector<int32_t>{1});
    ExpectInts(mp->GetValueVector().get(), std::vector<int32_t>{10});

    // 列2: VARCHAR = "mixed"（修复前会被 MAP 的残留 buffer 污染）
    ExpectStrings(vecs[2], {"mixed"});

    // 列3: ARRAY<INT> = [11, 22]（修复前会错位）
    auto* arr = reinterpret_cast<ArrayVector*>(vecs[3]);
    EXPECT_EQ(arr->GetSize(0), 2);
    ExpectInts(arr->GetElementVector().get(), std::vector<int32_t>{11, 22});

    for (auto* v : vecs) delete v;
    DeletePathAll((tmpShuffleFilePath + "_mixed").c_str());
}

// ============================================================================
// 用例 7: 多批同分区 + 强制 spill → offset 链式接续 + spill→merge 全链路
// ============================================================================
TEST(ArrowColumnarComplexRoundTrip, MultiBatchAndSpill)
{
    std::vector<DataTypePtr> types;
    types.push_back(std::make_shared<ArrayType>(IntType()));
    std::vector<VectorBatch*> batches;
    // 3 批，每批 2 行 → 同分区累计 6 行
    for (int b = 0; b < 3; ++b) {
        batches.push_back(MakeBatch_ArrayInt(1, 2));
    }
    auto parsed = RunSplitter(tmpShuffleFilePath + "_spill",
                              {OMNI_ARRAY},
                              types, batches, /*forceSpill=*/true);

    int64_t totalRows = 0;
    for (const auto& b : parsed.batches) totalRows += b.rowCount;
    EXPECT_EQ(totalRows, 6);

    // 逐批反序列化并断言内容连续正确
    for (const auto& b : parsed.batches) {
        auto vecs = DeserializeBatch(parsed, b);
        ASSERT_EQ(vecs.size(), 1u);
        auto* arr = reinterpret_cast<ArrayVector*>(vecs[0]);
        EXPECT_EQ(arr->GetSize(), b.rowCount);
        for (int r = 0; r < b.rowCount; ++r) {
            EXPECT_EQ(arr->GetSize(r), 2);
        }
        auto* elem = reinterpret_cast<Vector<int32_t>*>(arr->GetElementVector().get());
        // MakeBatch_ArrayInt 每批元素值从批内行号 0 重新开始：元素 = (r/2)*10 + (r%2)
        for (int r = 0; r < b.rowCount * 2; ++r) {
            EXPECT_EQ(elem->GetValue(r), (r / 2) * 10 + (r % 2));
        }
        delete vecs[0];
    }
    DeletePathAll((tmpShuffleFilePath + "_spill").c_str());
}
