#include "gtest/gtest.h"
#include "shuffle/arrow_frame.h"
#include "shuffle/arrow_type_bridge.h"
#include "shuffle/type.h"

using namespace omniruntime::type;

// 新版 Arrow 的 Buffer::Copy 签名已变更，用 BufferBuilder 替代
static std::shared_ptr<arrow::Buffer> MakeBuffer(const void* data, int64_t size) {
    arrow::BufferBuilder bb;
    bb.Append(data, size);
    return bb.Finish().ValueOrDie();
}

TEST(ArrowFrameHeader, WriteAndReadBackFixedCols) {
    ArrowFileHeader hdr;
    hdr.version = kArrowShuffleVersion;
    hdr.layout = ShuffleLayout::COLUMNAR;
    hdr.schema.push_back(DataTypeToDescriptor(IntDataType::Instance()));
    hdr.schema.push_back(DataTypeToDescriptor(LongDataType::Instance()));
    hdr.schema.push_back(DataTypeToDescriptor(VarcharDataType::Instance()));

    auto w = WriteFileHeader(hdr);
    ASSERT_TRUE(w.ok()) << w.status().ToString();
    auto buf = *w;
    ASSERT_GE(buf->size(), 10);  // magic4 + version1 + layout1 + numCols4 至少

    int64_t consumed = 0;
    auto r = ReadFileHeader(buf->data(), buf->size(), &consumed);
    ASSERT_TRUE(r.ok()) << r.status().ToString();
    EXPECT_EQ(consumed, buf->size());
    auto back = *r;
    EXPECT_EQ(back.version, kArrowShuffleVersion);
    EXPECT_EQ(back.layout, ShuffleLayout::COLUMNAR);
    ASSERT_EQ(back.schema.size(), 3u);
    EXPECT_EQ(back.schema[0].typeId, OMNI_INT);
    EXPECT_EQ(back.schema[1].typeId, OMNI_LONG);
    EXPECT_EQ(back.schema[2].typeId, OMNI_VARCHAR);
}

TEST(ArrowFrameHeader, RejectsBadMagic) {
    uint8_t bad[] = {'X', 'X', 'X', 'X', 1, 0, 0, 0, 0, 0};
    int64_t consumed = 0;
    auto r = ReadFileHeader(bad, sizeof(bad), &consumed);
    EXPECT_FALSE(r.ok());
    EXPECT_EQ(consumed, 0);
}

TEST(ArrowFrameHeader, RejectsUnsupportedVersion) {
    uint8_t bad[] = {'O', 'M', 'S', 'A', 99 /* version */, 0, 0, 0, 0, 0};
    int64_t consumed = 0;
    auto r = ReadFileHeader(bad, sizeof(bad), &consumed);
    EXPECT_FALSE(r.ok());
}

TEST(ArrowFrameHeader, RejectsBadLayout) {
    uint8_t bad[] = {'O', 'M', 'S', 'A', 1, 9 /* bad layout */, 0, 0, 0, 0};
    int64_t consumed = 0;
    auto r = ReadFileHeader(bad, sizeof(bad), &consumed);
    EXPECT_FALSE(r.ok());
}

TEST(ArrowFrameHeader, RecursiveSchemaRoundTrip) {
    // ARRAY<INT>：由 DataType 树直接构建递归描述符
    ArrowFileHeader hdr{kArrowShuffleVersion, ShuffleLayout::COLUMNAR,
                        {DataTypeToDescriptor(std::make_shared<ArrayType>(IntType()))}};
    auto w = WriteFileHeader(hdr);
    ASSERT_TRUE(w.ok()) << w.status().ToString();
    int64_t consumed = 0;
    auto back = *ReadFileHeader((*w)->data(), (*w)->size(), &consumed);
    ASSERT_EQ(back.schema.size(), 1u);
    EXPECT_EQ(back.schema[0].typeId, OMNI_ARRAY);
    ASSERT_EQ(back.schema[0].children.size(), 1u);
    EXPECT_EQ(back.schema[0].children[0].typeId, OMNI_INT);
}

// ============================================================
// 批体编解码测试
// ============================================================

TEST(ArrowFrameBatch, ColumnarBatchRoundTrip)
{
    // schema: INT(2 buffers) + VARCHAR(3 buffers)
    ArrowFileHeader hdr{kArrowShuffleVersion, ShuffleLayout::COLUMNAR,
        {DataTypeToDescriptor(IntDataType::Instance()),
         DataTypeToDescriptor(VarcharDataType::Instance())}};

    ColumnarBatchBody body;
    body.partitionId = 2;
    body.rowCount = 3;
    // INT: validity(nullptr->sentinel) + values(3×4=12B)
    body.buffers.push_back(nullptr);   // 全有效
    uint8_t vals[12] = {0,0,0,10, 0,0,0,20, 0,0,0,30};
    body.buffers.push_back(MakeBuffer(vals, 12));
    // VARCHAR: validity(sentinel) + offsets(4×4=16B) + values("AA""BB""CCC"=6B)
    body.buffers.push_back(nullptr);
    int32_t offs[4] = {0, 2, 4, 7};
    body.buffers.push_back(MakeBuffer(offs, 16));
    uint8_t strv[7] = {'A','A','B','B','C','C','C'};
    body.buffers.push_back(MakeBuffer(strv, 7));

    auto w = WriteColumnarBatch(body);
    ASSERT_TRUE(w.ok()) << w.status().ToString();
    auto buf = *w;

    int64_t consumed = 0;
    auto r = ReadColumnarBatch(buf->data(), buf->size(), hdr.schema, &consumed);
    ASSERT_TRUE(r.ok()) << r.status().ToString();
    EXPECT_EQ(consumed, buf->size());
    auto back = *r;
    EXPECT_EQ(back.partitionId, 2);
    EXPECT_EQ(back.rowCount, 3);
    ASSERT_EQ(back.buffers.size(), 5u);   // 2 + 3
    // validity sentinels -> nullptr
    EXPECT_EQ(back.buffers[0], nullptr);
    EXPECT_EQ(back.buffers[2], nullptr);
    ASSERT_NE(back.buffers[1], nullptr);
    EXPECT_EQ(back.buffers[1]->size(), 12);
    EXPECT_EQ(back.buffers[3]->size(), 16);
    EXPECT_EQ(back.buffers[4]->size(), 7);
    EXPECT_EQ(memcmp(back.buffers[4]->data(), "AABBCCC", 7), 0);
}

TEST(ArrowFrameBatch, RowBatchRoundTrip)
{
    RowBatchBody body;
    body.partitionId = 1;
    body.rowCount = 2;
    uint8_t rows[5] = {'R','0','R','1','X'};
    int32_t offs[3] = {0, 2, 4};
    body.rows = MakeBuffer(rows, 5);
    body.offsets = MakeBuffer(offs, 12);

    auto w = WriteRowBatch(body);
    ASSERT_TRUE(w.ok()) << w.status().ToString();
    auto buf = *w;
    int64_t consumed = 0;
    auto r = ReadRowBatch(buf->data(), buf->size(), &consumed);
    ASSERT_TRUE(r.ok()) << r.status().ToString();
    EXPECT_EQ(consumed, buf->size());
    EXPECT_EQ(r->partitionId, 1);
    EXPECT_EQ(r->rowCount, 2);
    ASSERT_NE(r->rows, nullptr);
    EXPECT_EQ(r->rows->size(), 5);
    EXPECT_EQ(memcmp(r->rows->data(), "R0R1X", 5), 0);
    EXPECT_EQ(r->offsets->size(), 12);
}

TEST(ArrowFrameBatch, NullBufferSentinelForValidity)
{
    // 全有效列：validity 写 kNullBufferSentinel(-1)，读回 nullptr
    ArrowFileHeader hdr{kArrowShuffleVersion, ShuffleLayout::COLUMNAR,
                        {DataTypeToDescriptor(LongDataType::Instance())}};
    ColumnarBatchBody body;
    body.partitionId = 0; body.rowCount = 1;
    body.buffers.push_back(nullptr);   // validity 全有效
    int64_t oneVal = 42;
    body.buffers.push_back(MakeBuffer(&oneVal, 8));

    auto w = WriteColumnarBatch(body);
    auto buf = *w;
    // 哨兵应在批体中：第 8B 前缀 + partitionId4 + rowCount4 之后第一个 bufLen 为 -1
    int64_t off = 8 + 4 + 4;
    int64_t sentinel; std::memcpy(&sentinel, buf->data() + off, 8);
    EXPECT_EQ(sentinel, kNullBufferSentinel);

    int64_t consumed = 0;
    auto back = *ReadColumnarBatch(buf->data(), buf->size(), hdr.schema, &consumed);
    EXPECT_EQ(back.buffers[0], nullptr);   // 哨兵还原为 nullptr
}
