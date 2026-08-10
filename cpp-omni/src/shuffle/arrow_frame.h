#ifndef CPP_ARROW_FRAME_H
#define CPP_ARROW_FRAME_H

#include <cstdint>
#include <vector>
#include <memory>
#include <arrow/buffer.h>
#include <arrow/buffer_builder.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include "shuffle/arrow_type_bridge.h"

constexpr char kArrowShuffleMagic[4] = {'O', 'M', 'S', 'A'};  // Omni Shuffle Arrow
constexpr uint8_t kArrowShuffleVersion = 1;
constexpr int64_t kNullBufferSentinel = -1;  // validity 缺省哨兵（全有效列）

enum class ShuffleLayout : uint8_t {
    COLUMNAR = 0,
    ROW = 1,
};

struct ArrowFileHeader {
    uint8_t version = kArrowShuffleVersion;
    ShuffleLayout layout = ShuffleLayout::COLUMNAR;
    std::vector<OmniTypeDescriptor> schema;
};

// 文件头序列化：magic(4) + version(1) + layout(1) + numCols(4 LE) + schema 递归
arrow::Result<std::shared_ptr<arrow::Buffer>> WriteFileHeader(const ArrowFileHeader& hdr);

// 解析文件头；consumed 输出已消费字节数。非法 magic/version/layout 返回 Invalid。
arrow::Result<ArrowFileHeader> ReadFileHeader(const uint8_t* data, int64_t size, int64_t* consumed);

// schema 节点读写（供批体/读侧复用）
void AppendDescriptor(arrow::BufferBuilder& bb, const OmniTypeDescriptor& d);
arrow::Result<OmniTypeDescriptor> ReadDescriptor(const uint8_t* data, int64_t size, int64_t* consumed);

// ============================================================
// 批体编解码（列式 / 行式）
// ============================================================

// 列式批体：按列 schema 顺序、复杂类型递归展开的 buffer 列表。
// nullptr 表示 validity 缺省（全有效），帧中以 kNullBufferSentinel 写出。
struct ColumnarBatchBody {
    int32_t partitionId = 0;
    int32_t rowCount = 0;
    std::vector<std::shared_ptr<arrow::Buffer>> buffers;
};

// 行式批体：rows 为 RowBuffer 打包的连续行字节，offsets 为 int32 行偏移数组。
struct RowBatchBody {
    int32_t partitionId = 0;
    int32_t rowCount = 0;
    std::shared_ptr<arrow::Buffer> rows;
    std::shared_ptr<arrow::Buffer> offsets;
};

// 列式批体序列化：写 [8B LE batch_byte_size][partitionId 4B LE][rowCount 4B LE]
//                + Σ([bufLen 8B LE][buf bytes])，bufLen=-1 即 kNullBufferSentinel。
arrow::Result<std::shared_ptr<arrow::Buffer>> WriteColumnarBatch(const ColumnarBatchBody& body);

// 行式批体序列化：写 [8B LE batch_byte_size][partitionId 4B LE][rowCount 4B LE]
//                + [rowsLen 8B LE][offsetsLen 4B LE][rows bytes][offsets bytes]。
arrow::Result<std::shared_ptr<arrow::Buffer>> WriteRowBatch(const RowBatchBody& body);

// 列式批体反序列化：据文件头 schema 还原各 buffer（buffer 数由 schema 递归确定）。
// consumed 输出已消费字节数。buffer 为引用输入 data 的零拷贝视图，调用方须保证生命周期。
arrow::Result<ColumnarBatchBody> ReadColumnarBatch(const uint8_t* data, int64_t size,
                                                   const std::vector<OmniTypeDescriptor>& schema,
                                                   int64_t* consumed);

// 行式批体反序列化：consumed 输出已消费字节数。
// rows/offsets 为引用输入 data 的零拷贝视图，调用方须保证生命周期。
arrow::Result<RowBatchBody> ReadRowBatch(const uint8_t* data, int64_t size, int64_t* consumed);

#endif  // CPP_ARROW_FRAME_H
