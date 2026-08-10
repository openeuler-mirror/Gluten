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

#include "shuffle/arrow_columnar_serializer.h"
#include <stdexcept>
#include <cstring>
#include "common/debug.h"
#include "common/common.h"

// 写 4 字节大端 size 前缀（与 reduce 端 ShuffleReaderDeserializer::readSize 配对）。
// readSize 按 (b0<<24 | b1<<16 | b2<<8 | b3) 大端解析，故用 reversebytes_uint32t 转大端。
static arrow::Status WriteBigEndianSizePrefix(ArrowOutputStream& out, int32_t size) {
    uint32_t beSize = reversebytes_uint32t(static_cast<uint32_t>(size));
    return out.Write(&beSize, sizeof(beSize));
}

int32_t ArrowWriteColumnarPartition(int32_t partition_id,
                                    ArrowOutputStream& out,
                                    const ArrowFileHeader& header,
                                    const std::vector<std::vector<ArrowColumnarCachedBatch>>& partitionArrowBatch,
                                    bool headerAlreadyWritten)
{
    int32_t written = 0;

    if (partition_id < 0 || static_cast<size_t>(partition_id) >= partitionArrowBatch.size()) {
        LogsError("ArrowWriteColumnarPartition invalid partition_id: pid=%d partitions=%zu",
                  partition_id, partitionArrowBatch.size());
        throw std::runtime_error("ArrowWriteColumnarPartition: invalid partition_id: "
                                 + std::to_string(partition_id));
    }

    // 预先生成文件头 buffer（每批都需要，因为 reduce 端每次 readSize/decompress 周期
    // 解压后的 payload 必须以 magic "OMSA" 开头才能走 Arrow 解析路径）。
    auto headerBufR = WriteFileHeader(header);
    if (!headerBufR.ok()) {
        LogsError("ArrowWriteColumnarPartition WriteFileHeader failed: pid=%d msg=%s",
                  partition_id, headerBufR.status().ToString().c_str());
        throw std::runtime_error("ArrowWriteColumnarPartition: WriteFileHeader failed: "
                                 + headerBufR.status().ToString());
    }
    auto headerBuf = std::move(headerBufR).ValueOrDie();
    int32_t headerSize = static_cast<int32_t>(headerBuf->size());

    // 期望 buffer 数：由 header schema 递归确定（与读侧 ReadColumnarBatch 的 totalBuffers 一致）。
    // 写侧实际产出若与之不一致，说明 schema 与数据布局脱节（如复杂类型子结构丢失），
    // 必须快速失败，避免读侧按 schema 消费 buffer 导致后续列错位（静默数据损坏）。
    int64_t expectedTotalBuffers = 0;
    for (const auto& d : header.schema) {
        expectedTotalBuffers += NumBuffers(d);
    }

    // 遍历该 partition 的所有缓存批，逐帧序列化写出。
    // 每批写出格式：[4B 大端 size = headerSize + batchFrameSize][文件头][batch帧]
    // 这与 reduce 端 ShuffleReaderDeserializer::Next 的 readSize+decompress 协议配对：
    //   readSize 读 4B 大端 → dataSize; decompress 读 dataSize 字节 → 解压后 payload
    //   = [文件头][batch帧]，payload 开头 magic "OMSA" 触发 Arrow 解析路径。
    int32_t batchIdx = 0;
    for (const auto& batch : partitionArrowBatch[partition_id]) {
        if (static_cast<int64_t>(batch.buffers.size()) != expectedTotalBuffers) {
            LogsError("ArrowWriteColumnarPartition buffer count mismatch: pid=%d batchIdx=%d "
                      "expected=%lld actual=%zu schemaCols=%zu",
                      partition_id, batchIdx, static_cast<long long>(expectedTotalBuffers),
                      batch.buffers.size(), header.schema.size());
            throw std::runtime_error("ArrowWriteColumnarPartition: buffer count mismatch vs schema");
        }
        ColumnarBatchBody body;
        body.partitionId = partition_id;
        body.rowCount = batch.rowCount;
        body.buffers = batch.buffers;  // nullptr validity → WriteColumnarBatch 写哨兵 kNullBufferSentinel

        auto bb = WriteColumnarBatch(body);
        if (!bb.ok()) {
            LogsError("ArrowWriteColumnarPartition WriteColumnarBatch failed: pid=%d batchIdx=%d "
                      "rowCount=%d bufferNum=%zu msg=%s",
                      partition_id, batchIdx, batch.rowCount, batch.buffers.size(),
                      bb.status().ToString().c_str());
            throw std::runtime_error("ArrowWriteColumnarPartition: WriteColumnarBatch failed: "
                                     + bb.status().ToString());
        }
        int32_t batchFrameSize = static_cast<int32_t>((*bb)->size());
        int32_t payloadSize = headerSize + batchFrameSize;

        // 写 4B 大端 size 前缀
        auto sizeSt = WriteBigEndianSizePrefix(out, payloadSize);
        if (!sizeSt.ok()) {
            LogsError("ArrowWriteColumnarPartition write size prefix failed: pid=%d batchIdx=%d "
                      "payloadSize=%d msg=%s",
                      partition_id, batchIdx, payloadSize, sizeSt.ToString().c_str());
            throw std::runtime_error("ArrowWriteColumnarPartition: write size prefix failed: "
                                     + sizeSt.ToString());
        }
        written += sizeof(uint32_t);

        // 写文件头
        auto hdrSt = out.Write(headerBuf->data(), headerSize);
        if (!hdrSt.ok()) {
            LogsError("ArrowWriteColumnarPartition write header failed: pid=%d batchIdx=%d "
                      "headerSize=%d msg=%s",
                      partition_id, batchIdx, headerSize, hdrSt.ToString().c_str());
            throw std::runtime_error("ArrowWriteColumnarPartition: Write file header to output stream failed: "
                                     + hdrSt.ToString());
        }
        written += headerSize;

        // 写 batch 帧
        auto st = out.Write((*bb)->data(), batchFrameSize);
        if (!st.ok()) {
            LogsError("ArrowWriteColumnarPartition write batch failed: pid=%d batchIdx=%d "
                      "rowCount=%d frameSize=%d msg=%s",
                      partition_id, batchIdx, batch.rowCount, batchFrameSize, st.ToString().c_str());
            throw std::runtime_error("ArrowWriteColumnarPartition: Write batch to output stream failed: "
                                     + st.ToString());
        }
        written += batchFrameSize;
        ++batchIdx;
    }

    // Flush 并获取实际写入底层流的字节数（压缩后）。
    // partition_lengths_ 必须记录压缩后字节数（文件中实际字节数），
    // 否则 Spark shuffle block 定位会偏移，导致 reduce 端从错误位置读取。
    uint64_t flushedBytes = out.FlushAndCount();

    return static_cast<int32_t>(flushedBytes);
}
