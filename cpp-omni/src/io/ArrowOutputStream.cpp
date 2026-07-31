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

#include "io/ArrowOutputStream.h"
#include <algorithm>
#include <cstring>
#include <arrow/status.h>

#include "common/debug.h"

arrow::Status ArrowOutputStream::Write(const void* data, int64_t nbytes) {
    if (closed_) {
        LogsError("ArrowOutputStream Write after close: nbytes=%lld", static_cast<long long>(nbytes));
        return arrow::Status::Invalid("ArrowOutputStream: write after close");
    }
    if (nbytes <= 0) {
        return arrow::Status::OK();
    }
    // 用 Next() 获取可写区指针，再 memcpy 写入。
    // 必须用 Next() 而非 NextNBytes()：压缩流(CompressionStream)重写了 Next()，
    // 在 Next() 中触发 doStreamingCompression 把 rawInputBuffer 数据压缩输出；
    // NextNBytes() 直接写 dataBuffer 绕过了压缩逻辑，导致压缩模式下数据未被压缩。
    // 对于非压缩流(BufferedOutputStream)，Next() 和 NextNBytes() 行为等价（都写 dataBuffer）。
    const char* src = static_cast<const char*>(data);
    int64_t remaining = nbytes;
    while (remaining > 0) {
        void* dst = nullptr;
        int dstSize = 0;
        if (!buf_->Next(&dst, &dstSize)) {
            LogsError("ArrowOutputStream Next failed: remaining=%lld (buffer allocation failure)",
                      static_cast<long long>(remaining));
            return arrow::Status::IOError("ArrowOutputStream: Next failed");
        }
        int toCopy = static_cast<int>(std::min(static_cast<int64_t>(dstSize), remaining));
        std::memcpy(dst, src, static_cast<size_t>(toCopy));
        if (toCopy < dstSize) {
            buf_->BackUp(dstSize - toCopy);
        }
        src += toCopy;
        remaining -= toCopy;
    }
    return arrow::Status::OK();
}

arrow::Status ArrowOutputStream::Write(const std::shared_ptr<arrow::Buffer>& data) {
    if (data == nullptr) {
        return arrow::Status::OK();
    }
    return Write(data->data(), data->size());
}

arrow::Status ArrowOutputStream::Flush() {
    if (closed_) {
        return arrow::Status::OK();
    }
    buf_->flush();
    return arrow::Status::OK();
}

arrow::Status ArrowOutputStream::Close() {
    if (!closed_) {
        buf_->flush();
        closed_ = true;
    }
    return arrow::Status::OK();
}

arrow::Result<int64_t> ArrowOutputStream::Tell() const {
    return static_cast<int64_t>(buf_->getSize());
}

std::shared_ptr<ArrowOutputStream> ArrowOutputStream::Make(
        spark::OutputStream* raw,
        spark::CompressionKind kind,
        spark::CompressionStrategy strategy,
        uint64_t capacity,
        uint64_t blockSize,
        spark::MemoryPool& pool) {
    std::unique_ptr<spark::BufferedOutputStream> buf;
    if (kind == spark::CompressionKind_NONE) {
        buf = std::make_unique<spark::BufferedOutputStream>(pool, raw, capacity, blockSize);
    } else {
        buf = spark::createCompressor(kind, raw, strategy,
                                      capacity, blockSize, pool);
    }
    return std::make_shared<ArrowOutputStream>(std::move(buf));
}
