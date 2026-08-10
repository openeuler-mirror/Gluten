/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
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

#include "shuffle/arrow_row_serializer.h"
#include <stdexcept>
#include "common/debug.h"
#include "common/common.h"

// 写 4 字节大端 size 前缀（与 reduce 端 ShuffleReaderDeserializer::readSize 配对）。
static arrow::Status WriteBigEndianSizePrefix(ArrowOutputStream& out, int32_t size) {
    uint32_t beSize = reversebytes_uint32t(static_cast<uint32_t>(size));
    return out.Write(&beSize, sizeof(beSize));
}

int32_t ArrowWriteRowPartition(int32_t partition_id,
                               ArrowOutputStream& out,
                               const ArrowFileHeader& header,
                               const std::vector<std::vector<RowInfo*>>& partitionRows,
                               uint64_t spillBatchRowNum,
                               OmniMemoryPoolAdapter& pool,
                               bool headerAlreadyWritten)
{
    int32_t written = 0;

    if (partition_id < 0 || static_cast<size_t>(partition_id) >= partitionRows.size()) {
        LogsError("ArrowWriteRowPartition invalid partition_id: pid=%d partitions=%zu",
                  partition_id, partitionRows.size());
        throw std::runtime_error("ArrowWriteRowPartition: invalid partition_id: "
                                 + std::to_string(partition_id));
    }
    const auto& rows = partitionRows[partition_id];

    // 预先生成文件头 buffer（每批都需要，因为 reduce 端每次 readSize/decompress 周期
    // 解压后的 payload 必须以 magic "OMSA" 开头才能走 Arrow 解析路径）。
    auto headerBufR = WriteFileHeader(header);
    if (!headerBufR.ok()) {
        LogsError("ArrowWriteRowPartition WriteFileHeader failed: pid=%d msg=%s",
                  partition_id, headerBufR.status().ToString().c_str());
        throw std::runtime_error("ArrowWriteRowPartition: WriteFileHeader failed: "
                                 + headerBufR.status().ToString());
    }
    auto headerBuf = std::move(headerBufR).ValueOrDie();
    int32_t headerSize = static_cast<int32_t>(headerBuf->size());

    // 获取该分区的行数据
    uint64_t rowCount = rows.size();
    uint64_t batchCount = 0;

    // 按 spillBatchRowNum 分批写出。
    // 每批写出格式：[4B 大端 size = headerSize + batchFrameSize][文件头][row batch帧]
    while (rowCount > 0) {
        uint64_t onceCopyRow = (spillBatchRowNum > 0 && spillBatchRowNum < rowCount)
                               ? spillBatchRowNum : rowCount;

        // ① 计算 offsets（保留原有逻辑）
        uint64_t offset = batchCount * spillBatchRowNum;
        std::vector<int32_t> offsetVec(onceCopyRow + 1, 0);
        auto rowInfoPtr = rows.data() + offset;
        for (uint64_t i = 0; i < onceCopyRow; ++i) {
            RowInfo* rowInfo = rowInfoPtr[i];
            offsetVec[i + 1] = offsetVec[i] + rowInfo->length;
        }

        // ② 用 BufferBuilder 替换 std::string rows，纳入统一内存记账
        arrow::BufferBuilder rowsBuilder(&pool);
        auto reserveSt = rowsBuilder.Reserve(offsetVec[onceCopyRow]);
        if (!reserveSt.ok()) {
            LogsError("ArrowWriteRowPartition rows Reserve failed: pid=%d batch=%u reserve=%d msg=%s",
                      partition_id, batchCount, offsetVec[onceCopyRow], reserveSt.ToString().c_str());
            throw std::runtime_error("ArrowWriteRowPartition: BufferBuilder Reserve failed: "
                                     + reserveSt.ToString());
        }
        for (uint64_t i = 0; i < onceCopyRow; ++i) {
            RowInfo* rowInfo = rowInfoPtr[i];
            auto appendSt = rowsBuilder.Append(rowInfo->row, rowInfo->length);
            if (!appendSt.ok()) {
                LogsError("ArrowWriteRowPartition rows Append failed: pid=%d batch=%u row=%llu len=%d msg=%s",
                          partition_id, batchCount, static_cast<unsigned long long>(i),
                          rowInfo->length, appendSt.ToString().c_str());
                throw std::runtime_error("ArrowWriteRowPartition: BufferBuilder Append failed: "
                                         + appendSt.ToString());
            }
        }
        std::shared_ptr<arrow::Buffer> rowsBuf;
        auto finishSt = rowsBuilder.Finish(&rowsBuf);
        if (!finishSt.ok()) {
            LogsError("ArrowWriteRowPartition rows Finish failed: pid=%d batch=%u msg=%s",
                      partition_id, batchCount, finishSt.ToString().c_str());
            throw std::runtime_error("ArrowWriteRowPartition: BufferBuilder Finish failed: "
                                     + finishSt.ToString());
        }

        // ③ offsets 从临时 vector 通过 BufferBuilder 写入 Arrow buffer（临时 vector，不宜 Wrap）
        int64_t offsetsByteSize = static_cast<int64_t>(offsetVec.size()) * sizeof(int32_t);
        arrow::BufferBuilder offsetsBuilder(&pool);
        auto reserveSt2 = offsetsBuilder.Reserve(offsetsByteSize);
        if (!reserveSt2.ok()) {
            LogsError("ArrowWriteRowPartition offsets Reserve failed: pid=%d batch=%u reserve=%lld msg=%s",
                      partition_id, batchCount, static_cast<long long>(offsetsByteSize),
                      reserveSt2.ToString().c_str());
            throw std::runtime_error("ArrowWriteRowPartition: offsets BufferBuilder Reserve failed: "
                                     + reserveSt2.ToString());
        }
        auto appendSt2 = offsetsBuilder.Append(
            reinterpret_cast<const uint8_t*>(offsetVec.data()), offsetsByteSize);
        if (!appendSt2.ok()) {
            LogsError("ArrowWriteRowPartition offsets Append failed: pid=%d batch=%u msg=%s",
                      partition_id, batchCount, appendSt2.ToString().c_str());
            throw std::runtime_error("ArrowWriteRowPartition: offsets BufferBuilder Append failed: "
                                     + appendSt2.ToString());
        }
        std::shared_ptr<arrow::Buffer> offsetsBuf;
        auto finishSt2 = offsetsBuilder.Finish(&offsetsBuf);
        if (!finishSt2.ok()) {
            LogsError("ArrowWriteRowPartition offsets Finish failed: pid=%d batch=%u msg=%s",
                      partition_id, batchCount, finishSt2.ToString().c_str());
            throw std::runtime_error("ArrowWriteRowPartition: BufferBuilder Finish failed: "
                                     + finishSt2.ToString());
        }

        // ④ 构造行式批体并序列化写出
        RowBatchBody body;
        body.partitionId = partition_id;
        body.rowCount = static_cast<int32_t>(onceCopyRow);
        body.rows = std::move(rowsBuf);
        body.offsets = std::move(offsetsBuf);

        auto bb = WriteRowBatch(body);
        if (!bb.ok()) {
            LogsError("ArrowWriteRowPartition WriteRowBatch failed: pid=%d batch=%u rowCount=%llu msg=%s",
                      partition_id, batchCount, static_cast<unsigned long long>(onceCopyRow),
                      bb.status().ToString().c_str());
            throw std::runtime_error("ArrowWriteRowPartition: WriteRowBatch failed: "
                                     + bb.status().ToString());
        }
        int32_t batchFrameSize = static_cast<int32_t>((*bb)->size());
        int32_t payloadSize = headerSize + batchFrameSize;

        // 写 4B 大端 size 前缀
        auto sizeSt = WriteBigEndianSizePrefix(out, payloadSize);
        if (!sizeSt.ok()) {
            LogsError("ArrowWriteRowPartition write size prefix failed: pid=%d batch=%u payloadSize=%d msg=%s",
                      partition_id, batchCount, payloadSize, sizeSt.ToString().c_str());
            throw std::runtime_error("ArrowWriteRowPartition: write size prefix failed: "
                                     + sizeSt.ToString());
        }
        written += sizeof(uint32_t);

        // 写文件头
        auto hdrSt = out.Write(headerBuf->data(), headerSize);
        if (!hdrSt.ok()) {
            LogsError("ArrowWriteRowPartition write header failed: pid=%d batch=%u headerSize=%d msg=%s",
                      partition_id, batchCount, headerSize, hdrSt.ToString().c_str());
            throw std::runtime_error("ArrowWriteRowPartition: Write file header to output stream failed: "
                                     + hdrSt.ToString());
        }
        written += headerSize;

        // 写 row batch 帧
        auto st = out.Write((*bb)->data(), batchFrameSize);
        if (!st.ok()) {
            LogsError("ArrowWriteRowPartition write batch failed: pid=%d batch=%u frameSize=%d msg=%s",
                      partition_id, batchCount, batchFrameSize, st.ToString().c_str());
            throw std::runtime_error("ArrowWriteRowPartition: Write batch to output stream failed: "
                                     + st.ToString());
        }
        written += batchFrameSize;

        rowCount -= onceCopyRow;
        ++batchCount;
    }

    // Flush 并获取实际写入底层流的字节数（压缩后）。
    // partition_lengths_ 必须记录压缩后字节数（文件中实际字节数）。
    uint64_t flushedBytes = out.FlushAndCount();

    return static_cast<int32_t>(flushedBytes);
}
