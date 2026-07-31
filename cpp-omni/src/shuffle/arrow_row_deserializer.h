#ifndef CPP_ARROW_ROW_DESERIALIZER_H
#define CPP_ARROW_ROW_DESERIALIZER_H

#include <arrow/buffer.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <cstdint>
#include <memory>
#include <vector>

#include "shuffle/arrow_frame.h"
#include <vector/vector_common.h>

struct RowShuffleBatchContext {
    std::shared_ptr<arrow::Buffer> fileBuffer;     // 持有原始数据生命周期
    const uint8_t* cursor = nullptr;                // 当前读取位置
    int64_t remaining = 0;                          // 剩余可读字节数
    ArrowFileHeader header;                         // 文件头（含 schema）
    int32_t vecCnt = 0;                             // 列数（= schema.size()）
    std::vector<omniruntime::type::DataTypeId> typeIds;  // 各列类型
    int32_t rowCnt = 0;                             // 当前批行数
    const char* rowsPtr = nullptr;                  // 当前批 rows 起始
    const int32_t* offsetsPtr = nullptr;            // 当前批 offsets 起始
};

// 从原始字节数据初始化行式读侧上下文。
// 读取文件头（magic/version/layout/schema），校验 layout == ROW，
// 记录 cursor 指向首批数据起始。fileBuffer 为 Wrap 零拷贝视图，
// 调用方须保证 data 在上下文生命周期内有效。
arrow::Result<std::unique_ptr<RowShuffleBatchContext>> RowShuffleParseInit(
    const uint8_t* data, int64_t size);

// 读取下一个行式批体头，记录 rowsPtr/offsetsPtr/rowCnt。
// 无更多批时返回 IOError("EOF")。
arrow::Status RowShuffleParseNextBatch(RowShuffleBatchContext& ctx);

// 使用 RowParser::ParseOneRow 逐行解析当前批到 vecs（原语不变）。
// 调用前须已通过 ParseNextBatch 加载了当前批数据。
void RowShuffleParseBatch(const RowShuffleBatchContext& ctx,
                          omniruntime::vec::BaseVector** vecs);

// 释放上下文资源（unique_ptr 析构即释放）。
void RowShuffleParseClose(std::unique_ptr<RowShuffleBatchContext> ctx);

#endif  // CPP_ARROW_ROW_DESERIALIZER_H
