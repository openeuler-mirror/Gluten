#include "shuffle/arrow_frame.h"
#include <arrow/buffer_builder.h>
#include <cstring>
#include <stdexcept>

#include "common/debug.h"

namespace {

// 小端写入辅助（本机 aarch64 / x86_64 均为小端，直接 memcpy）
void PutU8(arrow::BufferBuilder& bb, uint8_t v) {
    bb.Append(&v, 1);
}

void PutI32LE(arrow::BufferBuilder& bb, int32_t v) {
    bb.Append(&v, 4);
}

void PutI64LE(arrow::BufferBuilder& bb, int64_t v) {
    bb.Append(&v, 8);
}

int32_t ReadI32LE(const uint8_t* p) {
    int32_t v;
    std::memcpy(&v, p, 4);
    return v;
}

int64_t ReadI64LE(const uint8_t* p) {
    int64_t v;
    std::memcpy(&v, p, 8);
    return v;
}

}  // namespace

void AppendDescriptor(arrow::BufferBuilder& bb, const OmniTypeDescriptor& d) {
    PutI32LE(bb, d.typeId);
    PutI32LE(bb, d.precision);
    PutI32LE(bb, d.scale);
    PutI32LE(bb, d.numChildren);
    for (const auto& c : d.children) {
        AppendDescriptor(bb, c);
    }
}

arrow::Result<OmniTypeDescriptor> ReadDescriptor(const uint8_t* data, int64_t size,
                                                  int64_t* consumed) {
    OmniTypeDescriptor d;
    int64_t need = 16;  // 4 × int32
    if (*consumed + need > size) {
        LogsError("arrow_frame ReadDescriptor truncated: consumed=%lld need=%lld size=%lld",
                  static_cast<long long>(*consumed), static_cast<long long>(need),
                  static_cast<long long>(size));
        return arrow::Status::Invalid("arrow_frame: truncated schema descriptor");
    }
    const uint8_t* p = data + *consumed;
    d.typeId = ReadI32LE(p);
    d.precision = ReadI32LE(p + 4);
    d.scale = ReadI32LE(p + 8);
    d.numChildren = ReadI32LE(p + 12);
    *consumed += need;
    for (int i = 0; i < d.numChildren; ++i) {
        ARROW_ASSIGN_OR_RAISE(auto child, ReadDescriptor(data, size, consumed));
        d.children.push_back(std::move(child));
    }
    return d;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> WriteFileHeader(const ArrowFileHeader& hdr) {
    arrow::BufferBuilder bb;
    bb.Append(kArrowShuffleMagic, 4);
    PutU8(bb, hdr.version);
    PutU8(bb, static_cast<uint8_t>(hdr.layout));
    PutI32LE(bb, static_cast<int32_t>(hdr.schema.size()));
    for (const auto& d : hdr.schema) {
        AppendDescriptor(bb, d);
    }
    return bb.Finish();
}

arrow::Result<ArrowFileHeader> ReadFileHeader(const uint8_t* data, int64_t size,
                                               int64_t* consumed) {
    *consumed = 0;
    if (size < 10) {
        LogsError("arrow_frame ReadFileHeader header too short: size=%lld", static_cast<long long>(size));
        return arrow::Status::Invalid("arrow_frame: header too short");
    }
    if (std::memcmp(data, kArrowShuffleMagic, 4) != 0) {
        LogsError("arrow_frame ReadFileHeader bad magic: size=%lld", static_cast<long long>(size));
        return arrow::Status::Invalid("arrow_frame: bad magic");
    }
    *consumed = 4;
    ArrowFileHeader hdr;
    hdr.version = data[(*consumed)++];
    if (hdr.version != kArrowShuffleVersion) {
        LogsError("arrow_frame ReadFileHeader unsupported version: got=%d expected=%d",
                  hdr.version, kArrowShuffleVersion);
        return arrow::Status::Invalid("arrow_frame: unsupported version ",
                                      std::to_string(hdr.version));
    }
    uint8_t layoutByte = data[(*consumed)++];
    if (layoutByte != static_cast<uint8_t>(ShuffleLayout::COLUMNAR) &&
        layoutByte != static_cast<uint8_t>(ShuffleLayout::ROW)) {
        LogsError("arrow_frame ReadFileHeader bad layout: layoutByte=%d", layoutByte);
        return arrow::Status::Invalid("arrow_frame: bad layout ",
                                      std::to_string(layoutByte));
    }
    hdr.layout = static_cast<ShuffleLayout>(layoutByte);
    int32_t numCols = ReadI32LE(data + *consumed);
    *consumed += 4;
    if (numCols < 0) {
        LogsError("arrow_frame ReadFileHeader negative numCols: numCols=%d", numCols);
        return arrow::Status::Invalid("arrow_frame: negative numCols");
    }
    for (int i = 0; i < numCols; ++i) {
        ARROW_ASSIGN_OR_RAISE(auto d, ReadDescriptor(data, size, consumed));
        hdr.schema.push_back(std::move(d));
    }
    return hdr;
}

// ============================================================
// 批体编解码实现
// ============================================================

namespace {

// 计算列式批体字节数（不含 8B 前缀）：partitionId(4) + rowCount(4) + Σ(8 + buflen)
int64_t ColumnarBodyBytes(const ColumnarBatchBody& body)
{
    int64_t n = 4 + 4;
    for (const auto& b : body.buffers) {
        n += 8 + (b ? b->size() : 0);   // 哨兵也占 8B
    }
    return n;
}

// 计算行式批体字节数（不含 8B 前缀）
int64_t RowBodyBytes(const RowBatchBody& body)
{
    int64_t rowsLen = body.rows ? body.rows->size() : 0;
    int64_t offsetsLen = body.offsets ? body.offsets->size() : 0;
    return 4 + 4 + 8 + 4 + rowsLen + offsetsLen;   // partitionId + rowCount + rowsLen + offsetsLen + bytes
}

}  // namespace

arrow::Result<std::shared_ptr<arrow::Buffer>> WriteColumnarBatch(const ColumnarBatchBody& body)
{
    int64_t bodyBytes = ColumnarBodyBytes(body);
    arrow::BufferBuilder bb;
    bb.Reserve(8 + bodyBytes);
    PutI64LE(bb, bodyBytes);                       // 8B 前缀
    PutI32LE(bb, body.partitionId);
    PutI32LE(bb, body.rowCount);
    for (const auto& b : body.buffers) {
        if (b == nullptr) {
            PutI64LE(bb, kNullBufferSentinel);      // validity 缺省
        } else {
            PutI64LE(bb, static_cast<int64_t>(b->size()));
            bb.Append(b->data(), static_cast<int64_t>(b->size()));
        }
    }
    return bb.Finish();
}

arrow::Result<ColumnarBatchBody> ReadColumnarBatch(const uint8_t* data, int64_t size,
                                                   const std::vector<OmniTypeDescriptor>& schema,
                                                   int64_t* consumed)
{
    *consumed = 0;
    if (size < 8) {
        LogsError("arrow_frame ReadColumnarBatch batch too short: size=%lld schemaCols=%zu",
                  static_cast<long long>(size), schema.size());
        return arrow::Status::Invalid("arrow_frame: batch too short");
    }
    int64_t bodyBytes = ReadI64LE(data);
    *consumed = 8;
    if (8 + bodyBytes > size) {
        LogsError("arrow_frame ReadColumnarBatch batch body truncated: bodyBytes=%lld size=%lld",
                  static_cast<long long>(bodyBytes), static_cast<long long>(size));
        return arrow::Status::Invalid("arrow_frame: batch body truncated");
    }
    ColumnarBatchBody body;
    if (*consumed + 8 > size) {
        LogsError("arrow_frame ReadColumnarBatch batch header truncated: consumed=%lld size=%lld",
                  static_cast<long long>(*consumed), static_cast<long long>(size));
        return arrow::Status::Invalid("arrow_frame: batch header truncated");
    }
    body.partitionId = ReadI32LE(data + *consumed);  *consumed += 4;
    body.rowCount    = ReadI32LE(data + *consumed);  *consumed += 4;
    // buffer 数由 schema 递归确定
    int totalBuffers = 0;
    for (const auto& d : schema) {
        totalBuffers += NumBuffers(d);
    }
    for (int i = 0; i < totalBuffers; ++i) {
        if (*consumed + 8 > size) {
            LogsError("arrow_frame ReadColumnarBatch bufLen truncated: i=%d totalBuffers=%d "
                      "consumed=%lld size=%lld partitionId=%d rowCount=%d",
                      i, totalBuffers, static_cast<long long>(*consumed),
                      static_cast<long long>(size), body.partitionId, body.rowCount);
            return arrow::Status::Invalid("arrow_frame: bufLen truncated");
        }
        int64_t bufLen = ReadI64LE(data + *consumed);  *consumed += 8;
        if (bufLen == kNullBufferSentinel) {
            body.buffers.push_back(nullptr);
        } else {
            if (bufLen < 0 || *consumed + bufLen > size) {
                LogsError("arrow_frame ReadColumnarBatch buf bytes truncated: i=%d bufLen=%lld "
                          "consumed=%lld size=%lld partitionId=%d rowCount=%d",
                          i, static_cast<long long>(bufLen), static_cast<long long>(*consumed),
                          static_cast<long long>(size), body.partitionId, body.rowCount);
                return arrow::Status::Invalid("arrow_frame: buf bytes truncated");
            }
            auto wrapped = arrow::Buffer::Wrap(data, size);
            body.buffers.push_back(arrow::SliceBuffer(wrapped, *consumed, bufLen));
            *consumed += bufLen;
        }
    }
    return body;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> WriteRowBatch(const RowBatchBody& body)
{
    int64_t bodyBytes = RowBodyBytes(body);
    arrow::BufferBuilder bb;
    bb.Reserve(8 + bodyBytes);
    PutI64LE(bb, bodyBytes);
    PutI32LE(bb, body.partitionId);
    PutI32LE(bb, body.rowCount);
    int64_t rowsLen = body.rows ? body.rows->size() : 0;
    int32_t offsetsLen = body.offsets ? static_cast<int32_t>(body.offsets->size()) : 0;
    PutI64LE(bb, rowsLen);
    PutI32LE(bb, offsetsLen);
    if (body.rows)    bb.Append(body.rows->data(), rowsLen);
    if (body.offsets) bb.Append(body.offsets->data(), offsetsLen);
    return bb.Finish();
}

arrow::Result<RowBatchBody> ReadRowBatch(const uint8_t* data, int64_t size, int64_t* consumed)
{
    *consumed = 0;
    if (size < 8) {
        LogsError("arrow_frame ReadRowBatch row batch too short: size=%lld", static_cast<long long>(size));
        return arrow::Status::Invalid("arrow_frame: row batch too short");
    }
    int64_t bodyBytes = ReadI64LE(data);
    *consumed = 8;
    if (8 + bodyBytes > size) {
        LogsError("arrow_frame ReadRowBatch row batch body truncated: bodyBytes=%lld size=%lld",
                  static_cast<long long>(bodyBytes), static_cast<long long>(size));
        return arrow::Status::Invalid("arrow_frame: row batch body truncated");
    }
    RowBatchBody body;
    body.partitionId = ReadI32LE(data + *consumed);   *consumed += 4;
    body.rowCount    = ReadI32LE(data + *consumed);   *consumed += 4;
    int64_t rowsLen    = ReadI64LE(data + *consumed);  *consumed += 8;
    int32_t offsetsLen = ReadI32LE(data + *consumed);  *consumed += 4;
    if (rowsLen < 0 || offsetsLen < 0 || *consumed + rowsLen + offsetsLen > size) {
        LogsError("arrow_frame ReadRowBatch row batch payload truncated: rowsLen=%lld offsetsLen=%d "
                  "consumed=%lld size=%lld partitionId=%d rowCount=%d",
                  static_cast<long long>(rowsLen), offsetsLen, static_cast<long long>(*consumed),
                  static_cast<long long>(size), body.partitionId, body.rowCount);
        return arrow::Status::Invalid("arrow_frame: row batch payload truncated");
    }
    auto rowWrapped = arrow::Buffer::Wrap(data, size);
    body.rows = arrow::SliceBuffer(rowWrapped, *consumed, rowsLen);
    *consumed += rowsLen;
    auto offWrapped = arrow::Buffer::Wrap(data, size);
    body.offsets = arrow::SliceBuffer(offWrapped, *consumed, offsetsLen);
    *consumed += offsetsLen;
    return body;
}
