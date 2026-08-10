#include "shuffle/arrow_row_deserializer.h"

#include <arrow/buffer.h>
#include <stdexcept>

#include "common/debug.h"
#include "shuffle/arrow_frame.h"
#include "vector/omni_row.h"

namespace {

using omniruntime::type::DataTypeId;
using omniruntime::vec::BaseVector;

}  // namespace

arrow::Result<std::unique_ptr<RowShuffleBatchContext>> RowShuffleParseInit(
    const uint8_t* data, int64_t size) {
    if (data == nullptr || size <= 0) {
        LogsError("RowShuffleParseInit null or empty input data: data=%p size=%lld",
                  static_cast<const void*>(data), static_cast<long long>(size));
        return arrow::Status::Invalid(
            "arrow_row_deserializer: null or empty input data");
    }

    // 读取文件头
    int64_t consumed = 0;
    auto hdrResult = ReadFileHeader(data, size, &consumed);
    if (!hdrResult.ok()) {
        LogsError("RowShuffleParseInit ReadFileHeader failed: size=%lld msg=%s",
                  static_cast<long long>(size), hdrResult.status().ToString().c_str());
        return hdrResult.status();
    }

    ArrowFileHeader header = std::move(*hdrResult);

    // 校验 layout 必须为 ROW
    if (header.layout != ShuffleLayout::ROW) {
        LogsError("RowShuffleParseInit layout mismatch: expected ROW(%d) got %d, "
                  "numCols=%zu version=%d",
                  static_cast<int>(ShuffleLayout::ROW), static_cast<int>(header.layout),
                  header.schema.size(), header.version);
        return arrow::Status::Invalid(
            "arrow_row_deserializer: expected layout ROW, got ",
            std::to_string(static_cast<uint8_t>(header.layout)));
    }

    // 构造上下文
    auto ctx = std::make_unique<RowShuffleBatchContext>();
    ctx->fileBuffer = arrow::Buffer::Wrap(data, size);
    ctx->cursor = data + consumed;
    ctx->remaining = size - consumed;
    ctx->header = std::move(header);
    ctx->vecCnt = static_cast<int32_t>(ctx->header.schema.size());

    // 从 schema 提取 typeIds
    ctx->typeIds.reserve(ctx->header.schema.size());
    for (const auto& desc : ctx->header.schema) {
        ctx->typeIds.push_back(static_cast<DataTypeId>(desc.typeId));
    }

    return ctx;
}

arrow::Status RowShuffleParseNextBatch(RowShuffleBatchContext& ctx) {
    if (ctx.remaining <= 0) {
        LogsError("RowShuffleParseNextBatch EOF, no more batches: remaining=%lld",
                  static_cast<long long>(ctx.remaining));
        return arrow::Status::IOError(
            "arrow_row_deserializer: EOF, no more batches");
    }

    int64_t consumed = 0;
    auto bodyResult = ReadRowBatch(ctx.cursor, ctx.remaining, &consumed);
    if (!bodyResult.ok()) {
        LogsError("RowShuffleParseNextBatch ReadRowBatch failed: remaining=%lld msg=%s",
                  static_cast<long long>(ctx.remaining), bodyResult.status().ToString().c_str());
        return bodyResult.status();
    }

    RowBatchBody body = std::move(*bodyResult);
    ctx.rowCnt = body.rowCount;
    ctx.rowsPtr = reinterpret_cast<const char*>(body.rows->data());
    ctx.offsetsPtr = reinterpret_cast<const int32_t*>(body.offsets->data());

    ctx.cursor += consumed;
    ctx.remaining -= consumed;

    return arrow::Status::OK();
}

void RowShuffleParseBatch(const RowShuffleBatchContext& ctx,
                          BaseVector** vecs) {
    if (ctx.rowsPtr == nullptr || ctx.offsetsPtr == nullptr) {
        LogsError("RowShuffleParseBatch null pointer: rowsPtr=%p offsetsPtr=%p rowCnt=%d",
                  static_cast<const void*>(ctx.rowsPtr),
                  static_cast<const void*>(ctx.offsetsPtr), ctx.rowCnt);
        throw std::runtime_error("RowShuffleParseBatch: rowsPtr or offsetsPtr is null");
    }
    omniruntime::vec::RowParser parser(ctx.typeIds);
    for (int32_t i = 0; i < ctx.rowCnt; ++i) {
        char* rowPtr =
            const_cast<char*>(ctx.rowsPtr) + ctx.offsetsPtr[i];
        parser.ParseOneRow(reinterpret_cast<uint8_t*>(rowPtr), vecs, i);
    }
}

void RowShuffleParseClose(std::unique_ptr<RowShuffleBatchContext> ctx) {
    // unique_ptr 析构自动释放所有资源
    (void)ctx;
}
