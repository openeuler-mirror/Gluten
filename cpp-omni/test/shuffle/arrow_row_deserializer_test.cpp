#include "gtest/gtest.h"
#include "shuffle/arrow_row_deserializer.h"
#include "shuffle/arrow_frame.h"
#include "shuffle/arrow_type_bridge.h"

#include <arrow/buffer_builder.h>
#include <cstring>
#include <vector>

using namespace omniruntime::type;

namespace {

// Arrow Buffer::Copy 新版签名已变更，使用 BufferBuilder 构造 Buffer
std::shared_ptr<arrow::Buffer> MakeBuffer(const void* data, int64_t size) {
    arrow::BufferBuilder bb;
    bb.Append(data, size);
    return bb.Finish().ValueOrDie();
}

// 拼接多个 Buffer 为一个连续字节 Buffer
std::shared_ptr<arrow::Buffer> ConcatBuffers(
    const std::vector<std::shared_ptr<arrow::Buffer>>& bufs) {
    int64_t totalSize = 0;
    for (const auto& b : bufs) {
        totalSize += b->size();
    }
    arrow::BufferBuilder bb;
    bb.Reserve(totalSize);
    for (const auto& b : bufs) {
        bb.Append(b->data(), b->size());
    }
    return bb.Finish().ValueOrDie();
}

}  // namespace

// ============================================================
// 基本测试：构造单列 INT 行式帧，验证 ParseInit + ParseNextBatch
// ============================================================
TEST(ArrowRowDeserialize, ParseInitAndNextBatch) {
    // 1) 构造文件头：单列 INT，layout=ROW
    OmniTypeDescriptor intDesc;
    intDesc.typeId = OMNI_INT;
    intDesc.numChildren = 0;

    ArrowFileHeader hdr;
    hdr.version = kArrowShuffleVersion;
    hdr.layout = ShuffleLayout::ROW;
    hdr.schema.push_back(intDesc);

    auto hdrBuf = WriteFileHeader(hdr);
    ASSERT_TRUE(hdrBuf.ok()) << hdrBuf.status().ToString();

    // 2) 构造行式批体：2 行，每行 4 字节 INT
    //    行数据：row0 = {10,0,0,0}, row1 = {20,0,0,0}（小端：10 和 20）
    uint8_t rowData[8] = {10, 0, 0, 0, 20, 0, 0, 0};
    int32_t offsets[3] = {0, 4, 8};

    RowBatchBody body;
    body.partitionId = 0;
    body.rowCount = 2;
    body.rows = MakeBuffer(rowData, 8);
    body.offsets = MakeBuffer(offsets, sizeof(offsets));

    auto batchBuf = WriteRowBatch(body);
    ASSERT_TRUE(batchBuf.ok()) << batchBuf.status().ToString();

    // 3) 拼接文件头 + 批体为完整文件字节
    auto fileBuf =
        ConcatBuffers({*hdrBuf, *batchBuf});

    // 4) ParseInit：解析文件头
    auto ctxResult =
        RowShuffleParseInit(fileBuf->data(), fileBuf->size());
    ASSERT_TRUE(ctxResult.ok()) << ctxResult.status().ToString();
    auto ctx = std::move(*ctxResult);

    EXPECT_EQ(ctx->vecCnt, 1);
    ASSERT_EQ(ctx->typeIds.size(), 1u);
    EXPECT_EQ(ctx->typeIds[0], OMNI_INT);
    EXPECT_GT(ctx->remaining, 0);
    EXPECT_NE(ctx->cursor, nullptr);

    // 5) ParseNextBatch：读取第一批
    auto st = RowShuffleParseNextBatch(*ctx);
    ASSERT_TRUE(st.ok()) << st.ToString();

    EXPECT_EQ(ctx->rowCnt, 2);
    ASSERT_NE(ctx->rowsPtr, nullptr);
    ASSERT_NE(ctx->offsetsPtr, nullptr);
    EXPECT_EQ(ctx->offsetsPtr[0], 0);
    EXPECT_EQ(ctx->offsetsPtr[1], 4);
    EXPECT_EQ(ctx->offsetsPtr[2], 8);

    // 6) 释放
    RowShuffleParseClose(std::move(ctx));
}

// ============================================================
// EOF 测试：单批读完后再次 ParseNextBatch 应返回 EOF
// ============================================================
TEST(ArrowRowDeserialize, ParseNextBatchEOF) {
    OmniTypeDescriptor intDesc;
    intDesc.typeId = OMNI_INT;
    intDesc.numChildren = 0;

    ArrowFileHeader hdr;
    hdr.version = kArrowShuffleVersion;
    hdr.layout = ShuffleLayout::ROW;
    hdr.schema.push_back(intDesc);

    auto hdrBuf = WriteFileHeader(hdr);
    ASSERT_TRUE(hdrBuf.ok());

    // 空批体（0 行）
    RowBatchBody body;
    body.partitionId = 0;
    body.rowCount = 0;
    body.rows = MakeBuffer(nullptr, 0);
    body.offsets = MakeBuffer(nullptr, 0);

    auto batchBuf = WriteRowBatch(body);
    ASSERT_TRUE(batchBuf.ok());

    auto fileBuf = ConcatBuffers({*hdrBuf, *batchBuf});

    auto ctxResult =
        RowShuffleParseInit(fileBuf->data(), fileBuf->size());
    ASSERT_TRUE(ctxResult.ok());
    auto ctx = std::move(*ctxResult);

    // 第一批成功（即使 0 行也应能解析）
    auto st1 = RowShuffleParseNextBatch(*ctx);
    ASSERT_TRUE(st1.ok()) << st1.ToString();
    EXPECT_EQ(ctx->rowCnt, 0);

    // 第二批应返回 EOF
    auto st2 = RowShuffleParseNextBatch(*ctx);
    EXPECT_FALSE(st2.ok());
    EXPECT_TRUE(st2.IsIOError()) << st2.ToString();

    RowShuffleParseClose(std::move(ctx));
}

// ============================================================
// Layout 校验：COLUMNAR layout 应被拒绝
// ============================================================
TEST(ArrowRowDeserialize, RejectColumnarLayout) {
    OmniTypeDescriptor intDesc;
    intDesc.typeId = OMNI_INT;
    intDesc.numChildren = 0;

    ArrowFileHeader hdr;
    hdr.version = kArrowShuffleVersion;
    hdr.layout = ShuffleLayout::COLUMNAR;  // 非 ROW
    hdr.schema.push_back(intDesc);

    auto hdrBuf = WriteFileHeader(hdr);
    ASSERT_TRUE(hdrBuf.ok());

    auto ctxResult =
        RowShuffleParseInit((*hdrBuf)->data(), (*hdrBuf)->size());

    EXPECT_FALSE(ctxResult.ok());
    EXPECT_TRUE(ctxResult.status().IsInvalid())
        << ctxResult.status().ToString();
}

// ============================================================
// 空输入测试：nullptr 或 size=0 应被拒绝
// ============================================================
TEST(ArrowRowDeserialize, RejectEmptyInput) {
    auto ctxResult = RowShuffleParseInit(nullptr, 100);
    EXPECT_FALSE(ctxResult.ok());
    EXPECT_TRUE(ctxResult.status().IsInvalid());

    uint8_t dummy = 0;
    ctxResult = RowShuffleParseInit(&dummy, 0);
    EXPECT_FALSE(ctxResult.ok());
    EXPECT_TRUE(ctxResult.status().IsInvalid());
}

// ============================================================
// 多列 Schema 测试：INT + VARCHAR 两列
// ============================================================
TEST(ArrowRowDeserialize, MultiColumnSchema) {
    OmniTypeDescriptor intDesc;
    intDesc.typeId = OMNI_INT;
    intDesc.numChildren = 0;

    OmniTypeDescriptor varcharDesc;
    varcharDesc.typeId = OMNI_VARCHAR;
    varcharDesc.numChildren = 0;

    ArrowFileHeader hdr;
    hdr.version = kArrowShuffleVersion;
    hdr.layout = ShuffleLayout::ROW;
    hdr.schema.push_back(intDesc);
    hdr.schema.push_back(varcharDesc);

    auto hdrBuf = WriteFileHeader(hdr);
    ASSERT_TRUE(hdrBuf.ok());

    auto ctxResult =
        RowShuffleParseInit((*hdrBuf)->data(), (*hdrBuf)->size());
    ASSERT_TRUE(ctxResult.ok()) << ctxResult.status().ToString();
    auto ctx = std::move(*ctxResult);

    EXPECT_EQ(ctx->vecCnt, 2);
    ASSERT_EQ(ctx->typeIds.size(), 2u);
    EXPECT_EQ(ctx->typeIds[0], OMNI_INT);
    EXPECT_EQ(ctx->typeIds[1], OMNI_VARCHAR);

    RowShuffleParseClose(std::move(ctx));
}
