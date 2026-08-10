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

#ifndef CPP_ARROW_OUTPUT_STREAM_H
#define CPP_ARROW_OUTPUT_STREAM_H

#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <memory>
#include "OutputStream.hh"
#include "Compression.hh"
#include "MemoryPool.hh"
#include "Common.hh"

// 把 spark::BufferedOutputStream（压缩或非压缩）包装成 arrow::io::OutputStream，
// 供 Arrow 帧逐 buffer 顺序 Write。压缩块格式复用现有 createCompressor，对上层透明。
class ArrowOutputStream : public arrow::io::OutputStream {
public:
    explicit ArrowOutputStream(std::unique_ptr<spark::BufferedOutputStream> buf) : buf_(std::move(buf)) {}

    // 工厂：按 compressionKind 选压缩/非压缩，包装底层 raw OutputStream。
    // strategy 控制压缩策略：COMPRESSION（注重压缩比）或 SPEED（注重速度）。
    // 注意：必须使用 CompressionStrategy_COMPRESSION，
    // 否则 LZ4 会因 LZ4_ACCELERATION_MAX=65537 几乎不压缩，导致 Shuffle Write 量暴增。
    static std::shared_ptr<ArrowOutputStream> Make(spark::OutputStream* raw,
                                                    spark::CompressionKind kind,
                                                    spark::CompressionStrategy strategy,
                                                    uint64_t capacity,
                                                    uint64_t blockSize,
                                                    spark::MemoryPool& pool);

    arrow::Status Write(const void* data, int64_t nbytes) override;
    arrow::Status Write(const std::shared_ptr<arrow::Buffer>& data) override;
    arrow::Status Flush() override;
    arrow::Status Close() override;
    arrow::Result<int64_t> Tell() const override;

    bool closed() const override { return closed_; }

    // Flush 并返回实际写入底层流的字节数（压缩后）。
    // partition_lengths_ 必须记录压缩后字节数（文件中实际字节数），
    // 而非未压缩的逻辑字节数，否则 Spark shuffle block 定位会偏移。
    uint64_t FlushAndCount() {
        if (closed_) { return 0; }
        return buf_->flush();
    }

private:
    std::unique_ptr<spark::BufferedOutputStream> buf_;
    bool closed_ = false;
};

#endif // CPP_ARROW_OUTPUT_STREAM_H
