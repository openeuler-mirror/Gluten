/**
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

#include "splitter.h"
#include "utils.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <string>
#include <utility>
#include "shuffle/arrow_columnar_serializer.h"
#include "shuffle/arrow_row_serializer.h"
#include "shuffle/arrow_frame.h"
#include "io/ArrowOutputStream.h"
#include "io/SparkFile.hh"
#include "io/MemoryPool.hh"
#include "shuffle/omni_rss_push_client.h"
#include <arrow/io/file.h>

// Full pool definition for
// getBoundAllocator() called from Split_Init. Resolves via this TU's
// installed Omni include tree (OMNI_HOME/lib/include).
#include "memory/omni_task_memory_pool.h"
#include "operator/reclaim_context.h"
#include "util/omni_exception.h"

namespace {
// Growable in-memory sink for ArrowOutputStream: the compressed partition
// segments of a memory spill land here in the spill-file layout.
class VectorOutputStream : public spark::OutputStream {
public:
    uint64_t getLength() const override { return buffer_.size(); }
    uint64_t getNaturalWriteSize() const override { return 128 * 1024; }
    void write(const void *buf, size_t length) override
    {
        const auto *bytes = static_cast<const uint8_t *>(buf);
        buffer_.insert(buffer_.end(), bytes, bytes + length);
    }
    const std::string &getName() const override { return name_; }
    void close() override {}
    std::vector<uint8_t> Release() { return std::move(buffer_); }

private:
    std::vector<uint8_t> buffer_;
    std::string name_{"memory-spill"};
};
} // namespace

using namespace omniruntime::vec;
using namespace omniruntime::vec::unsafe;
using namespace omniruntime::type;

namespace {
// First capacity, in rows, of a growable partition buffer.
constexpr int32_t kInitialGrowableBufferRows = 64;

bool IsEnvironmentSwitchEnabled(const char *name)
{
    const char *value = std::getenv(name);
    return value != nullptr && value[0] != '\0' && !(value[0] == '0' && value[1] == '\0');
}

bool IsOperatorVerboseEnabled()
{
    static const bool enabled = IsEnvironmentSwitchEnabled("OMNI_OPERATOR_VERBOSE");
    return enabled;
}

bool IsReclaimSuccessTelemetryEnabled()
{
    return IsEnvironmentSwitchEnabled("OMNI_RECLAIM_VERBOSE") ||
        IsEnvironmentSwitchEnabled("OMNI_RECLAIM_VERBOSE_SUCCESS");
}
} // namespace

// OmniStlAllocator thunks (declared in common/OmniStlAllocator.h). This TU
// already has the full Allocator definition, so route vector storage through
// the task-bound allocator while retaining the no-pool fallback used by tests.
void *OmniStlAllocBytes(omniruntime::mem::Allocator *alloc, std::size_t bytes)
{
    if (bytes == 0) {
        return nullptr;
    }
    if (bytes > static_cast<std::size_t>(std::numeric_limits<int64_t>::max())) {
        throw std::bad_alloc();
    }
    if (alloc != nullptr) {
        return alloc->Alloc(static_cast<int64_t>(bytes));
    }
    void *ptr = std::malloc(bytes);
    if (ptr == nullptr) {
        throw std::bad_alloc();
    }
    return ptr;
}

void OmniStlFreeBytes(omniruntime::mem::Allocator *alloc, void *ptr, std::size_t bytes) noexcept
{
    if (ptr == nullptr) {
        return;
    }
    if (alloc != nullptr) {
        alloc->Free(ptr, static_cast<int64_t>(bytes));
        return;
    }
    std::free(ptr);
}

SplitOptions SplitOptions::Defaults() { return SplitOptions(); }

void Splitter::BuildPartition2Row(int32_t num_rows)
{
    row_offset_row_id_.resize(num_rows);
    partition_row_offset_base_.resize(num_partitions_ + 1);
    for (auto pid = 1; pid <= num_partitions_; ++pid) {
        partition_row_offset_base_[pid] = partition_row_offset_base_[pid - 1] + partition_id_cnt_cur_[pid - 1];
    }
    for (auto row = 0; row < num_rows; ++row) {
        auto pid = partition_id_[row];
        row_offset_row_id_[partition_row_offset_base_[pid]++] = row;
    }
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        partition_row_offset_base_[pid] -= partition_id_cnt_cur_[pid];
    }
    partition_used_.clear();
    for (auto pid = 0; pid != num_partitions_; ++pid) {
        if (partition_id_cnt_cur_[pid] > 0) {
            partition_used_.push_back(pid);
        }
    }
}

// 计算分区id,每个batch初始化
int Splitter::ComputeAndCountPartitionId(VectorBatch& vb, bool updateCachedCounts) {
    auto num_rows = vb.GetRowCount();
    memset(partition_id_cnt_cur_, 0, num_partitions_ * sizeof(int32_t));
    partition_id_.resize(num_rows);

    if (singlePartitionFlag) {
        partition_id_cnt_cur_[0] = num_rows;
        if (updateCachedCounts) {
            partition_id_cnt_cache_[0] += num_rows;
        }
        for (auto i = 0; i < num_rows; ++i) {
            partition_id_[i] = 0;
        }
    } else {
        auto pidVector = vb.Get(0);
        if (pidVector->GetEncoding() == OMNI_ENCODING_CONST) {
            int32_t constPid = reinterpret_cast<ConstVector<int32_t> *>(pidVector)->GetConstValue();
            if (constPid < 0 || constPid >= num_partitions_) {
                LogsError(" Illegal pid Value: %d >= partition number %d .", constPid, num_partitions_);
                throw std::runtime_error("Shuffle pidVec Illegal pid Value!");
            }
            partition_id_cnt_cur_[constPid] += num_rows;
            if (updateCachedCounts) {
                partition_id_cnt_cache_[constPid] += num_rows;
            }
            for (auto i = 0; i < num_rows; ++i) {
                partition_id_[i] = constPid;
            }
        } else if (pidVector->GetEncoding() == OMNI_DICTIONARY) {
            auto hash_vct = reinterpret_cast<Vector<DictionaryContainer<int32_t>> *>(pidVector);
            for (auto i = 0; i < num_rows; ++i) {
                int32_t pid = hash_vct->GetValue(i);
                if (pid < 0 || pid >= num_partitions_) {
                    LogsError(" Illegal pid Value: %d >= partition number %d .", pid, num_partitions_);
                    throw std::runtime_error("Shuffle pidVec Illegal pid Value!");
                }
                partition_id_[i] = pid;
                partition_id_cnt_cur_[pid]++;
                if (updateCachedCounts) {
                    partition_id_cnt_cache_[pid]++;
                }
            }
        } else if (pidVector->GetEncoding() == OMNI_FLAT) {
            auto hash_vct = reinterpret_cast<Vector<int32_t> *>(pidVector);
            for (auto i = 0; i < num_rows; ++i) {
                // positive mod
                int32_t pid = hash_vct->GetValue(i);
                if (pid < 0 || pid >= num_partitions_) {
                    LogsError(" Illegal pid Value: %d >= partition number %d .", pid, num_partitions_);
                    throw std::runtime_error("Shuffle pidVec Illegal pid Value!");
                }
                partition_id_[i] = pid;
                partition_id_cnt_cur_[pid]++;
                if (updateCachedCounts) {
                    partition_id_cnt_cache_[pid]++;
                }
            }
        } else {
         	throw std::runtime_error(
         	    std::string("ComputeAndCountPartitionId(pid column): unsupported vector encoding ") +
         	    std::to_string(static_cast<int>(pidVector->GetEncoding())));
        }
    }
    return 0;
}

//分区信息内存分配
int Splitter::AllocatePartitionBuffers(int32_t partition_id, int32_t new_size) {
    int num_fields = column_type_id_.size();
    auto fixed_width_idx = 0;

    for (auto i = 0; i < num_fields; ++i) {
        switch (column_type_id_[i]) {
            case SHUFFLE_1BYTE:
            case SHUFFLE_2BYTE:
            case SHUFFLE_4BYTE:
            case SHUFFLE_8BYTE:
            case SHUFFLE_DECIMAL128: {
                int32_t type_size = (1 << column_type_id_[i]);
                int32_t needed_size = new_size * type_size;

                // 定宽 values 改用 Arrow ResizableBuffer（经 OmniMemoryPoolAdapter 统一记账）
                auto r = arrow::AllocateResizableBuffer(needed_size, arrow_pool_.get());
                if (!r.ok()) {
                    LogsError("AllocatePartitionBuffers Arrow alloc failed: partition_id=%d needed_size=%d "
                              "type_size=%d arrowPoolBytes=%lld msg=%s",
                              partition_id, needed_size, type_size,
                              static_cast<long long>(arrow_pool_->bytes_allocated()),
                              r.status().ToString().c_str());
                    throw std::runtime_error("AllocatePartitionBuffers Arrow alloc failed: " + r.status().ToString());
                }
                auto value_arrow_buf = std::move(*r);
                // 保留完整容量 needed_size 作为逻辑大小。
                // 不能用 Resize(0)（即使 capacity 不变），因为 OmniMemoryPoolAdapter::Reallocate
                // 对 new_size=0 会调 alloc_->Alloc(0) + Free(old)，Alloc(0) 返回的指针不可写，
                // 后续 SplitFixedWidthValueBuffer 向 mutable_data() 写入 needed_size 字节会 SIGSEGV。
                auto resizeSt = value_arrow_buf->Resize(needed_size);
                if (!resizeSt.ok()) {
                    LogsError("AllocatePartitionBuffers Arrow Resize failed: pid=%d fixedIdx=%d msg=%s",
                              partition_id, fixed_width_idx, resizeSt.ToString().c_str());
                    throw std::runtime_error("AllocatePartitionBuffers: Arrow Resize failed: "
                                             + resizeSt.ToString());
                }

                partition_fixed_width_arrow_buffers_[fixed_width_idx][partition_id] = std::move(value_arrow_buf);
                partition_fixed_width_value_addrs_[fixed_width_idx][partition_id] =
                        partition_fixed_width_arrow_buffers_[fixed_width_idx][partition_id]->mutable_data();
                partition_fixed_width_validity_addrs_[fixed_width_idx][partition_id] = nullptr;
                // partition_fixed_width_buffers_[fixed_width_idx][partition_id]:
                //   [0] = validity buffer (lazy allocation, 仍用 omni Allocator 逐字节散列)
                //   [1] = nullptr (values 已改用 Arrow buffer，存于 partition_fixed_width_arrow_buffers_)
                partition_fixed_width_buffers_[fixed_width_idx][partition_id] = {
                    nullptr, nullptr};
                fixed_width_idx++;
                break;
            }
            case SHUFFLE_BINARY:
            case SHUFFLE_LARGE_BINARY:
            case SHUFFLE_ARRAY:
            case SHUFFLE_MAP:
            case SHUFFLE_ROW:
            case SHUFFLE_NULL:
            default: {
                break;
            }
        }
    }

    partition_buffer_size_[partition_id] = new_size;
    return 0;
}

bool Splitter::GrowsInPlace(int32_t partition_id, int32_t rows) const
{
    return growPartitionBuffers_ && partition_buffer_size_[partition_id] > 0 &&
        partition_buffer_idx_base_[partition_id] + rows <= options_.buffer_size;
}

int32_t Splitter::NextPartitionBufferRows(int32_t partition_id, int32_t rows) const
{
    if (!growPartitionBuffers_) {
        return std::max(rows, options_.buffer_size);
    }
    if (GrowsInPlace(partition_id, rows)) {
        // Doubling keeps the copies linear in the rows a buffer ends up holding.
        const int64_t needed = int64_t{partition_buffer_idx_base_[partition_id]} + rows;
        const int64_t doubled = int64_t{2} * partition_buffer_size_[partition_id];
        return static_cast<int32_t>(std::min<int64_t>(options_.buffer_size, std::max(needed, doubled)));
    }
    return std::max(rows, std::min(kInitialGrowableBufferRows, options_.buffer_size));
}

int Splitter::GrowPartitionBuffers(int32_t partition_id, int32_t new_size) {
    const int32_t rows = partition_buffer_idx_base_[partition_id];
    for (size_t col = 0; col < fixed_width_array_idx_.size(); ++col) {
        const auto col_idx_schema = singlePartitionFlag ? fixed_width_array_idx_[col] : fixed_width_array_idx_[col] - 1;
        const int64_t type_size = int64_t{1} << column_type_id_[col_idx_schema];
        auto &values = partition_fixed_width_arrow_buffers_[col][partition_id];
        auto st = values->Resize(new_size * type_size, /*shrink_to_fit=*/false);
        if (!st.ok()) {
            throw std::runtime_error("GrowPartitionBuffers Arrow resize failed: " + st.ToString());
        }
        partition_fixed_width_value_addrs_[col][partition_id] = values->mutable_data();

        auto &validity = partition_fixed_width_buffers_[col][partition_id][0];
        if (validity != nullptr) {
            auto *grown = static_cast<uint8_t *>(options_.allocator->Alloc(new_size));
            if (grown == nullptr) {
                throw std::runtime_error("Allocator for ValidityBuffer Failed! ");
            }
            memcpy(grown, validity->data_, rows);
            memset(grown + rows, 0, new_size - rows);
            validity = std::make_shared<Buffer>(grown, rows, new_size, options_.allocator);
            partition_fixed_width_validity_addrs_[col][partition_id] = grown;
        }
    }
    partition_buffer_size_[partition_id] = new_size;
    return 0;
}

int Splitter::SplitFixedWidthValueBuffer(VectorBatch& vb) {
    const auto num_rows = vb.GetRowCount();
    // Accumulate size_ deltas locally to avoid repeated deep vector indirection
    std::vector<int64_t> size_delta(num_partitions_, 0);

    for (uint col = 0; col < fixed_width_array_idx_.size(); ++col) {
        memset(partition_buffer_idx_offset_, 0, num_partitions_ * sizeof(int32_t));
        std::fill(size_delta.begin(), size_delta.end(), 0);
        auto col_idx_vb = fixed_width_array_idx_[col];
        auto col_idx_schema = singlePartitionFlag ? col_idx_vb : (col_idx_vb - 1);
        const auto& dst_addrs =  partition_fixed_width_value_addrs_[col];
        if (vb.Get(col_idx_vb)->GetEncoding() == OMNI_ENCODING_CONST) {
            auto shuffleType = column_type_id_[col_idx_schema];
            const auto shuffle_size = (1 << shuffleType);
            uint8_t constValueBytes[16] = {}; // max size for Decimal128
            auto typeId = vb.Get(col_idx_vb)->GetTypeId();
            switch (typeId) {
                case OMNI_BYTE:
                case OMNI_BOOLEAN: {
                    auto v = reinterpret_cast<ConstVector<int8_t> *>(vb.Get(col_idx_vb))->GetConstValue();
                    memcpy(constValueBytes, &v, sizeof(v));
                    break;
                }
                case OMNI_SHORT: {
                    auto v = reinterpret_cast<ConstVector<int16_t> *>(vb.Get(col_idx_vb))->GetConstValue();
                    memcpy(constValueBytes, &v, sizeof(v));
                    break;
                }
                case OMNI_INT:
                case OMNI_DATE32: {
                    auto v = reinterpret_cast<ConstVector<int32_t> *>(vb.Get(col_idx_vb))->GetConstValue();
                    memcpy(constValueBytes, &v, sizeof(v));
                    break;
                }
                case OMNI_FLOAT: {
                    auto v = reinterpret_cast<ConstVector<float> *>(vb.Get(col_idx_vb))->GetConstValue();
                    memcpy(constValueBytes, &v, sizeof(v));
                    break;
                }
                case OMNI_LONG:
                case OMNI_TIMESTAMP:
                case OMNI_DATE64:
                case OMNI_DECIMAL64: {
                    auto v = reinterpret_cast<ConstVector<int64_t> *>(vb.Get(col_idx_vb))->GetConstValue();
                    memcpy(constValueBytes, &v, sizeof(v));
                    break;
                }
                case OMNI_DOUBLE: {
                    auto v = reinterpret_cast<ConstVector<double> *>(vb.Get(col_idx_vb))->GetConstValue();
                    memcpy(constValueBytes, &v, sizeof(v));
                    break;
                }
                case OMNI_DECIMAL128: {
                    auto v = reinterpret_cast<ConstVector<Decimal128> *>(vb.Get(col_idx_vb))->GetConstValue();
                    memcpy(constValueBytes, &v, sizeof(v));
                    break;
                }
                default: {
                    LogsError("SplitFixedWidthValueBuffer ConstVector unsupported DataTypeId: %d", typeId);
                    throw std::runtime_error("SplitFixedWidthValueBuffer ConstVector unsupported DataTypeId");
                }
            }
            for (auto &pid : partition_used_) {
                auto dst_offset = partition_buffer_idx_base_[pid] + partition_buffer_idx_offset_[pid];
                auto dstPidBase = dst_addrs[pid] + dst_offset * shuffle_size;
                auto pos = partition_row_offset_base_[pid];
                auto end = partition_row_offset_base_[pid + 1];
                auto count = end - pos;
                // Use direct assignment instead of per-row memcpy to reduce elo_sync overhead
                switch (shuffle_size) {
                    case 1: {
                        uint8_t val = constValueBytes[0];
                        auto* dst8 = reinterpret_cast<uint8_t*>(dstPidBase);
                        for (int32_t i = 0; i < count; ++i) {
                            dst8[i] = val;
                        }
                        break;
                    }
                    case 2: {
                        uint16_t val = *reinterpret_cast<uint16_t*>(constValueBytes);
                        auto* dst16 = reinterpret_cast<uint16_t*>(dstPidBase);
                        for (int32_t i = 0; i < count; ++i) {
                            dst16[i] = val;
                        }
                        break;
                    }
                    case 4: {
                        uint32_t val = *reinterpret_cast<uint32_t*>(constValueBytes);
                        auto* dst32 = reinterpret_cast<uint32_t*>(dstPidBase);
                        for (int32_t i = 0; i < count; ++i) {
                            dst32[i] = val;
                        }
                        break;
                    }
                    case 8: {
                        uint64_t val = *reinterpret_cast<uint64_t*>(constValueBytes);
                        auto* dst64 = reinterpret_cast<uint64_t*>(dstPidBase);
                        for (int32_t i = 0; i < count; ++i) {
                            dst64[i] = val;
                        }
                        break;
                    }
                    case 16: {
                        uint64_t lo = *reinterpret_cast<uint64_t*>(constValueBytes);
                        uint64_t hi = *reinterpret_cast<uint64_t*>(constValueBytes + 8);
                        auto* dst128 = reinterpret_cast<uint64_t*>(dstPidBase);
                        for (int32_t i = 0; i < count; ++i) {
                            dst128[i * 2] = lo;
                            dst128[i * 2 + 1] = hi;
                        }
                        break;
                    }
                    default: {
                        for (int32_t i = 0; i < count; ++i) {
                            memcpy(dstPidBase + i * shuffle_size, constValueBytes, shuffle_size);
                        }
                        break;
                    }
                }
                size_delta[pid] += shuffle_size * count;
                partition_buffer_idx_offset_[pid] += count;
            }
        } else if (vb.Get(col_idx_vb)->GetEncoding() == OMNI_DICTIONARY) {
            LogsDebug("Dictionary Columnar process!");

            auto ids_addr = static_cast<int32_t *>(VectorHelper::UnsafeGetValues(vb.Get(col_idx_vb)));
            auto src_addr = reinterpret_cast<int64_t>(VectorHelper::UnsafeGetDictionary(vb.Get(col_idx_vb)));
            auto process = [&]<typename CTYPE>(const ShuffleTypeId shuffleTypeId) {
                const auto shuffle_size = (1 << shuffleTypeId);
                for (auto &pid: partition_used_) {
                    auto dstPidBase = reinterpret_cast<CTYPE *>(dst_addrs[pid]) + partition_buffer_idx_base_[pid];
                    auto pos = partition_row_offset_base_[pid];
                    auto end = partition_row_offset_base_[pid + 1];
                    auto count = end - pos;
                    for (; pos < end; ++pos) {
                        *dstPidBase++ = reinterpret_cast<CTYPE *>(src_addr)[ids_addr[row_offset_row_id_[pos]]];
                    }
                    size_delta[pid] += shuffle_size * count;
                    partition_buffer_idx_offset_[pid] += count;
                }
            };
            switch (column_type_id_[col_idx_schema]) {
                case SHUFFLE_1BYTE:
                    process.operator()<uint8_t>(SHUFFLE_1BYTE);
                    break;
                case SHUFFLE_2BYTE:
                    process.operator()<uint16_t>(SHUFFLE_2BYTE);
                    break;
                case SHUFFLE_4BYTE:
                    process.operator()<uint32_t>(SHUFFLE_4BYTE);
                    break;
                case SHUFFLE_8BYTE:
                    process.operator()<uint64_t>(SHUFFLE_8BYTE);
                    break;
                case SHUFFLE_DECIMAL128:
                    process.operator()<uint128_t>(SHUFFLE_DECIMAL128);
                    break;
                default: {
                    LogsError("SplitFixedWidthValueBuffer not match this type: %d", column_type_id_[col_idx_schema]);
                    throw std::runtime_error("SplitFixedWidthValueBuffer not match this type: " + column_type_id_[col_idx_schema]);
                }
            }
        } else if (vb.Get(col_idx_vb)->GetEncoding() == OMNI_FLAT) {
            auto src_addr = reinterpret_cast<int64_t>(VectorHelper::UnsafeGetValues(vb.Get(col_idx_vb)));
            auto process = [&]<typename CTYPE>(const ShuffleTypeId shuffleTypeId) {
                const auto shuffle_size = (1 << shuffleTypeId);
                for (auto &pid: partition_used_) {
                    auto dst_offset = partition_buffer_idx_base_[pid] + partition_buffer_idx_offset_[pid];
                    auto dstPidBase = reinterpret_cast<CTYPE *>(dst_addrs[pid]) + dst_offset;
                    auto pos = partition_row_offset_base_[pid];
                    auto end = partition_row_offset_base_[pid + 1];
                    auto count = end - pos;
                    // Batch contiguous rows with memcpy to reduce elo_sync from scatter writes
                    for (; pos < end;) {
                        auto rowId = row_offset_row_id_[pos];
                        auto run_start = pos;
                        while (pos + 1 < end && row_offset_row_id_[pos + 1] == row_offset_row_id_[pos] + 1) {
                            ++pos;
                        }
                        auto run_len = pos - run_start + 1;
                        if (run_len >= 4) {
                            memcpy(dstPidBase,
                                   reinterpret_cast<CTYPE*>(src_addr) + rowId,
                                   run_len * sizeof(CTYPE));
                            dstPidBase += run_len;
                        } else {
                            for (auto i = run_start; i <= pos; ++i) {
                                *dstPidBase++ = reinterpret_cast<CTYPE*>(src_addr)[row_offset_row_id_[i]];
                            }
                        }
                        ++pos;
                    }
                    size_delta[pid] += shuffle_size * count;
                    partition_buffer_idx_offset_[pid] += count;
                }
            };
            switch (column_type_id_[col_idx_schema]) {
                case SHUFFLE_1BYTE:
                    process.operator()<uint8_t>(SHUFFLE_1BYTE);
                    break;
                case SHUFFLE_2BYTE:
                    process.operator()<uint16_t>(SHUFFLE_2BYTE);
                    break;
                case SHUFFLE_4BYTE:
                    process.operator()<uint32_t>(SHUFFLE_4BYTE);
                    break;
                case SHUFFLE_8BYTE:
                    process.operator()<uint64_t>(SHUFFLE_8BYTE);
                    break;
                case SHUFFLE_DECIMAL128:
                    process.operator()<uint128_t>(SHUFFLE_DECIMAL128);
                    break;
                default: {
                    LogsError("ERROR: SplitFixedWidthValueBuffer not match this type: %d", column_type_id_[col_idx_schema]);
                    throw std::runtime_error("ERROR: SplitFixedWidthValueBuffer not match this type: " + column_type_id_[col_idx_schema]);
                }
            }
        } else {
         	throw std::runtime_error(
         	    std::string("SplitFixedWidthValueBuffer: unsupported vector encoding ") +
         	    std::to_string(static_cast<int>(vb.Get(col_idx_vb)->GetEncoding())));
        }
        // Write back accumulated size_ deltas once per column to avoid repeated deep indirection
        for (auto &pid : partition_used_) {
            // 定宽 values 已改用 Arrow buffer（[1] 为 nullptr），size_ 跟踪移入 CacheVectorBatch
            if (partition_fixed_width_buffers_[col][pid][1])
                partition_fixed_width_buffers_[col][pid][1]->size_ += size_delta[pid];
        }
    }
    return 0;
}

void HandleNull(VCBatchInfo &vcbInfo, bool isNull) {
    if(isNull) {
        vcbInfo.SetNullFlag(isNull);
    }
}

template<bool hasNull>
void Splitter::SplitBinaryVector(BaseVector *varcharVector, int col_schema) {
    int32_t num_rows = varcharVector->GetSize();
    bool is_null = false;
    if (varcharVector->GetEncoding() == OMNI_ENCODING_CONST) {
        auto constVec = reinterpret_cast<ConstVector<std::string_view> *>(varcharVector);
        bool constIsNull = constVec->HasNull() && constVec->IsNull(0);
        uint8_t *constDst = nullptr;
        uint32_t constStrLen = 0;
        if (!constIsNull) {
            std::string_view constValue = constVec->GetConstValue();
            constDst = reinterpret_cast<uint8_t *>(reinterpret_cast<int64_t>(constValue.data()));
            constStrLen = static_cast<uint32_t>(constValue.length());
        }
        cached_vectorbatch_size_ += num_rows * (sizeof(bool) + sizeof(int32_t));
        cached_vectorbatch_size_ += constStrLen;
        for (auto &pid : partition_used_) {
            auto pos = partition_row_offset_base_[pid];
            auto end = partition_row_offset_base_[pid + 1];
            for (; pos < end; ++pos) {
                if ((vc_partition_array_buffers_[pid][col_schema].size() != 0) &&
                    (vc_partition_array_buffers_[pid][col_schema].back().getVcList().size() <
                        options_.spill_batch_row_num)) {
                    if constexpr (hasNull) {
                        HandleNull(vc_partition_array_buffers_[pid][col_schema].back(), constIsNull);
                    }
                    vc_partition_array_buffers_[pid][col_schema].back().getVcList().emplace_back(
                        (uint64_t)constDst, constStrLen, constIsNull);
                    vc_partition_array_buffers_[pid][col_schema].back().vcb_total_len += constStrLen;
                } else {
                    VCBatchInfo svc(options_.spill_batch_row_num, options_.allocator);
                    svc.getVcList().emplace_back((uint64_t)constDst, constStrLen, constIsNull);
                    svc.vcb_total_len += constStrLen;
                    if constexpr (hasNull) {
                        HandleNull(svc, constIsNull);
                    }
                    vc_partition_array_buffers_[pid][col_schema].emplace_back(std::move(svc));
                }
            }
        }
        return;
    } else if (varcharVector->GetEncoding() == OMNI_DICTIONARY) {
        auto vc = reinterpret_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *>(
                varcharVector);
	    cached_vectorbatch_size_ += num_rows * (sizeof(bool) + sizeof(int32_t));
        for (auto &pid: partition_used_) {
            uint8_t *dst = nullptr;
            uint32_t str_len = 0;
            auto index = 0;
            auto pos = partition_row_offset_base_[pid];
            auto end = partition_row_offset_base_[pid + 1];
            for (; pos < end; ++pos, ++index) {
                auto rowId = row_offset_row_id_[pos];
                if constexpr (hasNull) {
                    if (!vc->IsNull(rowId)) {
                        std::string_view value = vc->GetValue(rowId);
                        dst = reinterpret_cast<uint8_t *>(reinterpret_cast<int64_t>(value.data()));
                        str_len = static_cast<uint32_t>(value.length());
                    }
                } else {
                    std::string_view value = vc->GetValue(rowId);
                    dst = reinterpret_cast<uint8_t *>(reinterpret_cast<int64_t>(value.data()));
                    str_len = static_cast<uint32_t>(value.length());
                }
                if constexpr (hasNull) {
                    is_null = vc->IsNull(rowId);
                }
                cached_vectorbatch_size_ += str_len; // 累计变长部分cache数据
                if ((vc_partition_array_buffers_[pid][col_schema].size() != 0) &&
                    (vc_partition_array_buffers_[pid][col_schema].back().getVcList().size() <
                        options_.spill_batch_row_num)) {
                    if constexpr (hasNull) {
                        HandleNull(vc_partition_array_buffers_[pid][col_schema].back(), is_null);
                    }
                    vc_partition_array_buffers_[pid][col_schema].back().getVcList().emplace_back((uint64_t)dst, str_len, is_null);
                    vc_partition_array_buffers_[pid][col_schema].back().vcb_total_len += str_len;
                } else {
                    VCBatchInfo svc(options_.spill_batch_row_num, options_.allocator);
                    svc.getVcList().emplace_back((uint64_t)dst, str_len, is_null);
                    svc.vcb_total_len += str_len;
                    if constexpr (hasNull) {
                        HandleNull(svc, is_null);
                    }
                    vc_partition_array_buffers_[pid][col_schema].emplace_back(std::move(svc));
                }
            }
        }
    } else if (varcharVector->GetEncoding() == OMNI_FLAT) {
        auto vc = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(varcharVector);
        cached_vectorbatch_size_ += num_rows * (sizeof(bool) + sizeof(int32_t)) + sizeof(int32_t);
        for (auto &pid: partition_used_) {
            auto &vc_partition_array = vc_partition_array_buffers_[pid];
            uint8_t *dst = nullptr;
            uint32_t str_len = 0;
            auto index = 0;
            auto pos = partition_row_offset_base_[pid];
            auto end = partition_row_offset_base_[pid + 1];
            for (; pos < end; ++pos, ++index) {
                auto rowId = row_offset_row_id_[pos];
                if constexpr (hasNull) {
                    if (!vc->IsNull(rowId)) {
                        std::string_view value = vc->GetValue(rowId);
                        dst = reinterpret_cast<uint8_t *>(reinterpret_cast<int64_t>(value.data()));
                        str_len = static_cast<uint32_t>(value.length());
                    }
                } else {
                    std::string_view value = vc->GetValue(rowId);
                    dst = reinterpret_cast<uint8_t *>(reinterpret_cast<int64_t>(value.data()));
                    str_len = static_cast<uint32_t>(value.length());
                }
                if constexpr (hasNull) {
                    is_null = vc->IsNull(rowId);
                }
                cached_vectorbatch_size_ += str_len; // 累计变长部分cache数据
                if ((vc_partition_array[col_schema].size() != 0) &&
                    (vc_partition_array[col_schema].back().getVcList().size() <
                        options_.spill_batch_row_num)) {
                    if constexpr (hasNull) {
                        HandleNull(vc_partition_array[col_schema].back(), is_null);
                    }
                    vc_partition_array[col_schema].back().getVcList().emplace_back((uint64_t)dst, str_len, is_null);
                    vc_partition_array[col_schema].back().vcb_total_len += str_len;
                } else {
                    VCBatchInfo svc(options_.spill_batch_row_num, options_.allocator);
                    svc.getVcList().emplace_back((uint64_t)dst, str_len, is_null);
                    if constexpr (hasNull) {
                        HandleNull(svc, is_null);
                    }
                    svc.vcb_total_len += str_len;
                    vc_partition_array[col_schema].emplace_back(std::move(svc));
                }
            }
        }
     } else {
 	     throw std::runtime_error(
 	         std::string("SplitBinaryVector: unsupported vector encoding ") +
 	         std::to_string(static_cast<int>(varcharVector->GetEncoding())));
    }
}

// ================================================================================================
// Task 11: Arrow 化复杂类型序列化 —— 将 Omni null bytes 转为 Arrow validity bitmap（取反）
// ================================================================================================
std::shared_ptr<arrow::Buffer> Splitter::OmniNullsToArrowBitmap(const uint8_t* nullBytes, int32_t numRows)
{
    // 检查是否有 null
    bool hasNull = false;
    for (int32_t i = 0; i < numRows; ++i) {
        if (nullBytes[i] != 0) { hasNull = true; break; }
    }
    if (!hasNull) return nullptr;  // 全有效 → nullptr 哨兵

    int32_t byteCount = (numRows + 7) / 8;
    auto vr = arrow::AllocateResizableBuffer(byteCount, arrow_pool_.get());
    if (!vr.ok()) {
        throw std::runtime_error("OmniNullsToArrowBitmap alloc failed: " + vr.status().ToString());
    }
    auto buf = std::move(*vr);
    memset(buf->mutable_data(), 0, byteCount);

    // 取反：Omni byte!=0 = null → Arrow bit=1 = valid
    for (int32_t i = 0; i < numRows; ++i) {
        if (nullBytes[i] == 0) {
            buf->mutable_data()[i / 8] |= (1u << (i % 8));
        }
    }
    return buf;
}

// ================================================================================================
// 方案 C: ComplexColumnAccumulator 方法实现
// ================================================================================================

void ComplexColumnAccumulator::Init(const DataTypePtr& dataType, int32_t bufferSize, OmniMemoryPoolAdapter* arrowPool)
{
    pool = arrowPool;
    rowCapacity = bufferSize;

    if (dataType == nullptr) {
        kind = Kind::ROOT;
        return;
    }

    auto typeId = dataType->GetId();
    switch (typeId) {
        case OMNI_BYTE:
        case OMNI_BOOLEAN:   kind = Kind::FIXED; fixedElemSize = 1; break;
        case OMNI_SHORT:     kind = Kind::FIXED; fixedElemSize = 2; break;
        case OMNI_INT:
        case OMNI_DATE32:
        case OMNI_FLOAT:     kind = Kind::FIXED; fixedElemSize = 4; break;
        case OMNI_LONG:
        case OMNI_DOUBLE:
        case OMNI_TIMESTAMP:
        case OMNI_DATE64:
        case OMNI_DECIMAL64: kind = Kind::FIXED; fixedElemSize = 8; break;
        case OMNI_DECIMAL128: kind = Kind::FIXED; fixedElemSize = 16; break;
        case OMNI_VARCHAR:
        case OMNI_CHAR:
        case OMNI_VARBINARY: kind = Kind::VARLEN; break;
        case OMNI_ARRAY:     kind = Kind::LIST; break;
        case OMNI_MAP:       kind = Kind::MAP; break;
        case OMNI_ROW:       kind = Kind::STRUCT; break;
        default:
            throw std::runtime_error("ComplexColumnAccumulator::Init: unsupported typeId " + std::to_string(typeId));
    }

    // Allocate offsets for LIST/MAP/VARLEN (they have offsets)
    if (kind == Kind::LIST || kind == Kind::MAP || kind == Kind::VARLEN) {
        int64_t offsetsSize = static_cast<int64_t>(bufferSize + 1) * sizeof(int32_t);
        auto r = arrow::AllocateResizableBuffer(offsetsSize, pool);
        if (!r.ok()) {
            throw std::runtime_error("ComplexColumnAccumulator::Init offsets alloc failed: " + r.status().ToString());
        }
        offsets = std::move(*r);
        offsets->Resize(offsetsSize);
        // Write initial offsets[0] = 0
        int32_t* p = reinterpret_cast<int32_t*>(offsets->mutable_data());
        p[0] = 0;
    }

    // Allocate values for FIXED (pre-allocate conservative size)
    if (kind == Kind::FIXED) {
        int64_t valuesSize = static_cast<int64_t>(bufferSize) * fixedElemSize;
        auto r = arrow::AllocateResizableBuffer(valuesSize, pool);
        if (!r.ok()) {
            throw std::runtime_error("ComplexColumnAccumulator::Init values alloc failed: " + r.status().ToString());
        }
        values = std::move(*r);
        values->Resize(valuesSize);
    }

    // Allocate values for VARLEN (start small, grow on demand)
    if (kind == Kind::VARLEN) {
        int64_t initialValuesSize = 256;  // small initial, grow as needed
        auto r = arrow::AllocateResizableBuffer(initialValuesSize, pool);
        if (!r.ok()) {
            throw std::runtime_error("ComplexColumnAccumulator::Init varlen values alloc failed: " + r.status().ToString());
        }
        values = std::move(*r);
        values->Resize(initialValuesSize);
    }

    // Recursively init children
    if (kind == Kind::LIST) {
        auto arrayType = std::dynamic_pointer_cast<ArrayType>(dataType);
        children.push_back(std::make_unique<ComplexColumnAccumulator>());
        children[0]->Init(arrayType->ElementType(), bufferSize, arrowPool);
    } else if (kind == Kind::MAP) {
        auto mapType = std::dynamic_pointer_cast<MapType>(dataType);
        children.push_back(std::make_unique<ComplexColumnAccumulator>());
        children[0]->Init(mapType->Key(), bufferSize, arrowPool);
        children.push_back(std::make_unique<ComplexColumnAccumulator>());
        children[1]->Init(mapType->Value(), bufferSize, arrowPool);
    } else if (kind == Kind::STRUCT) {
        auto rowType = std::dynamic_pointer_cast<RowType>(dataType);
        for (uint32_t c = 0; c < rowType->size(); ++c) {
            children.push_back(std::make_unique<ComplexColumnAccumulator>());
            children.back()->Init(rowType->childAt(c), bufferSize, arrowPool);
        }
    }
}

void ComplexColumnAccumulator::EnsureOffsetsCapacity(int64_t needEntries)
{
    if (!offsets) return;
    int64_t needBytes = needEntries * sizeof(int32_t);
    if (offsets->capacity() == 0) {
        int64_t initSize = needBytes > 0 ? needBytes : 4;
        auto r = arrow::AllocateResizableBuffer(initSize, pool);
        if (!r.ok()) {
            throw std::runtime_error("EnsureOffsetsCapacity alloc failed: " + r.status().ToString());
        }
        offsets = std::move(*r);
        offsets->Resize(needBytes);
        int32_t* p = reinterpret_cast<int32_t*>(offsets->mutable_data());
        p[0] = 0;
        return;
    }
    if (offsets->capacity() >= needBytes) {
        if (offsets->size() < needBytes) offsets->Resize(needBytes);
        return;
    }
    int64_t newCap = offsets->capacity();
    if (newCap == 0) newCap = needBytes;
    while (newCap < needBytes) newCap *= 2;
    auto st = offsets->Reserve(newCap);
    if (!st.ok()) {
        throw std::runtime_error("EnsureOffsetsCapacity Reserve failed: " + st.ToString());
    }
    offsets->Resize(needBytes);
}

void ComplexColumnAccumulator::EnsureValidityCapacity(int64_t needBits)
{
    int64_t needBytes = (needBits + 7) / 8;
    if (!validity || validity->capacity() == 0) {
        int64_t cap = (rowCapacity + 7) / 8;
        if (cap < needBytes) cap = needBytes;
        if (cap < 1) cap = 1;
        auto r = arrow::AllocateResizableBuffer(cap, pool);
        if (!r.ok()) {
            throw std::runtime_error("EnsureValidityCapacity alloc failed: " + r.status().ToString());
        }
        validity = std::move(*r);
        validity->Resize(needBytes);
        return;
    }
    if (validity->capacity() >= needBytes) {
        if (validity->size() < needBytes) validity->Resize(needBytes);
        return;
    }
    int64_t newCap = validity->capacity();
    if (newCap == 0) newCap = needBytes;
    while (newCap < needBytes) newCap *= 2;
    auto st = validity->Reserve(newCap);
    if (!st.ok()) {
        throw std::runtime_error("EnsureValidityCapacity Reserve failed: " + st.ToString());
    }
    validity->Resize(needBytes);
}

void ComplexColumnAccumulator::EnsureValuesCapacity(int64_t needBytes)
{
    if (!values || values->capacity() == 0) {
        int64_t initSize = needBytes > 0 ? needBytes : 1;
        if (initSize < 256) initSize = 256;
        auto r = arrow::AllocateResizableBuffer(initSize, pool);
        if (!r.ok()) {
            throw std::runtime_error("EnsureValuesCapacity alloc failed: " + r.status().ToString());
        }
        values = std::move(*r);
        values->Resize(needBytes > 0 ? needBytes : 0);
        return;
    }
    if (values->capacity() >= needBytes) {
        if (values->size() < needBytes) values->Resize(needBytes);
        return;
    }
    int64_t newCap = values->capacity();
    if (newCap == 0) newCap = needBytes;
    while (newCap < needBytes) newCap *= 2;
    auto st = values->Reserve(newCap);
    if (!st.ok()) {
        throw std::runtime_error("EnsureValuesCapacity Reserve failed: " + st.ToString());
    }
    values->Resize(needBytes);
}

void ComplexColumnAccumulator::AppendValidBit(bool isValid)
{
    if (isValid && validity == nullptr) {
        // All-valid so far, keep nullptr sentinel
        return;
    }
    // First null encountered: lazy-allocate validity and backfill all previous bits as 1 (valid)
    if (validity == nullptr) {
        // Backfill must cover ALL previous rows (0..rowCursor-1), not just rowCapacity rows.
        // rowCursor can exceed rowCapacity when partition_buffer_size > buffer_size
        // (DoSplit sets new_size = max(partition_id_cnt_cur, buffer_size)).
        // If we only backfill rowCapacity bytes, rows beyond that have uninitialized garbage bits.
        int64_t needBackfillBytes = (rowCursor + 7) / 8;
        if (needBackfillBytes < 1) needBackfillBytes = 1;
        // Allocate with enough capacity for rowCursor bits + room for growth
        int64_t allocCap = needBackfillBytes;
        if (allocCap < (int64_t)((rowCapacity + 7) / 8)) allocCap = (rowCapacity + 7) / 8;
        if (allocCap < 1) allocCap = 1;
        auto r = arrow::AllocateResizableBuffer(allocCap, pool);
        if (!r.ok()) {
            throw std::runtime_error("AppendValidBit alloc failed: " + r.status().ToString());
        }
        validity = std::move(*r);
        // Backfill ALL previous rows as valid (bit=1) — covers 0..rowCursor-1
        memset(validity->mutable_data(), 0xFF, needBackfillBytes);
        // Set logical size (avoid Resize(0))
        validity->Resize(needBackfillBytes);
    }
    int64_t bit = rowCursor;
    EnsureValidityCapacity(bit + 1);
    int64_t byteIdx = bit / 8;
    int32_t bitIdx = static_cast<int32_t>(bit % 8);
    if (isValid) {
        validity->mutable_data()[byteIdx] |= (1u << bitIdx);
    } else {
        validity->mutable_data()[byteIdx] &= ~(1u << bitIdx);
    }
}

void ComplexColumnAccumulator::CollectBuffers(std::vector<std::shared_ptr<arrow::Buffer>>& out)
{
    if (rowCursor == 0) {
        // No data written for this accumulator node. Delegate to CollectEmptyBuffers
        // which allocates real (non-null) 0-byte / all-zero buffers. The read side
        // (DeserializeArrowBufferToOmniVector) rejects nullptr for offsets/values,
        // so we must NOT push nullptr placeholders here.
        CollectEmptyBuffers(out, 0);
        return;
    }

    // validity
    if (validity) {
        int64_t byteLen = (rowCursor + 7) / 8;
        validity->Resize(byteLen);
        out.push_back(validity);
    } else {
        out.push_back(nullptr);  // all-valid sentinel
    }

    // offsets (LIST/MAP/VARLEN have offsets)
    if (offsets) {
        int64_t byteLen = (rowCursor + 1) * sizeof(int32_t);
        offsets->Resize(byteLen);
        out.push_back(offsets);
    }

    // values (FIXED/VARLEN leaves have values)
    if (values) {
        int64_t byteLen;
        if (kind == Kind::FIXED) {
            byteLen = rowCursor * fixedElemSize;
        } else {
            byteLen = valueBytesCursor;
        }
        values->Resize(byteLen);
        out.push_back(values);
    }

    // Recurse children
    for (auto& child : children) {
        child->CollectBuffers(out);
    }
}

void ComplexColumnAccumulator::CollectEmptyBuffers(std::vector<std::shared_ptr<arrow::Buffer>>& out, int32_t numRows)
{
    // Emit empty-but-valid placeholders for a column with no accumulated data
    out.push_back(nullptr);  // validity = all-valid sentinel

    if (kind == Kind::LIST || kind == Kind::MAP) {
        // offsets: (numRows+1) * 4 bytes, all zeros
        int32_t offsetsLen = (numRows + 1) * static_cast<int32_t>(sizeof(int32_t));
        auto offsetsR = arrow::AllocateBuffer(offsetsLen, pool);
        if (!offsetsR.ok()) {
            throw std::runtime_error("CollectEmptyBuffers offsets alloc failed: " + offsetsR.status().ToString());
        }
        auto emptyOffsetsBuf = std::move(*offsetsR);
        std::memset(emptyOffsetsBuf->mutable_data(), 0, static_cast<size_t>(offsetsLen));
        out.push_back(std::move(emptyOffsetsBuf));

        // Recurse children with 0 child rows
        for (auto& child : children) {
            child->CollectEmptyBuffers(out, 0);
        }
    } else if (kind == Kind::STRUCT) {
        for (auto& child : children) {
            child->CollectEmptyBuffers(out, numRows);
        }
    } else if (kind == Kind::FIXED) {
        // values: empty
        auto valuesR = arrow::AllocateBuffer(0, pool);
        if (!valuesR.ok()) {
            throw std::runtime_error("CollectEmptyBuffers values alloc failed: " + valuesR.status().ToString());
        }
        out.push_back(std::move(*valuesR));
    } else if (kind == Kind::VARLEN) {
        // offsets: (numRows+1) * 4 bytes, all zeros
        int32_t offsetsLen = (numRows + 1) * static_cast<int32_t>(sizeof(int32_t));
        auto offsetsR = arrow::AllocateBuffer(offsetsLen, pool);
        if (!offsetsR.ok()) {
            throw std::runtime_error("CollectEmptyBuffers varlen offsets alloc failed: " + offsetsR.status().ToString());
        }
        auto emptyOffsetsBuf = std::move(*offsetsR);
        std::memset(emptyOffsetsBuf->mutable_data(), 0, static_cast<size_t>(offsetsLen));
        out.push_back(std::move(emptyOffsetsBuf));
        // values: empty
        auto valuesR = arrow::AllocateBuffer(0, pool);
        if (!valuesR.ok()) {
            throw std::runtime_error("CollectEmptyBuffers varlen values alloc failed: " + valuesR.status().ToString());
        }
        out.push_back(std::move(*valuesR));
    }
}

void ComplexColumnAccumulator::Reset()
{
    rowCursor = 0;
    elemCursor = 0;
    valueBytesCursor = 0;
    // Release all buffers. CollectBuffers pushes shared_ptr references into
    // arrow_batch.buffers (zero-copy). If we keep and reuse the same ResizableBuffer,
    // the next batch's AppendColumnToArrow writes would overwrite the previous
    // batch's data in the same physical memory — corrupting data that may not yet
    // have been written to disk by WriteColumnarBatch.
    //
    // Design doc (Solution_Design.md §6.5) intended Reset to only clear cursors
    // and reuse buffers. However, that assumes CollectBuffers makes copies (or
    // that the batch is fully serialized before the next split). In practice,
    // CacheVectorBatch may be called multiple times before WriteSplit, so
    // accumulated batches coexist in memory. Releasing buffers here ensures each
    // cached batch owns its data independently. The next SplitComplexColumns call
    // will lazy-init a fresh accumulator via Init.
    offsets.reset();
    validity.reset();
    values.reset();
    // Recurse: release children too
    for (auto& child : children) {
        child->Reset();
    }
}

void ComplexColumnAccumulator::Release()
{
    offsets.reset();
    validity.reset();
    values.reset();
    children.clear();
}

// ================================================================================================
// 方案 C: AppendColumnToArrow 系列 —— 增量追加到 accumulator（替代 Serialize*ToArrow）
// ================================================================================================

void Splitter::AppendColumnToArrow(BaseVector *vector, std::vector<uint32_t> row_ids,
                                   DataTypePtr dataType, ComplexColumnAccumulator& acc)
{
    if (vector == nullptr) {
        throw std::runtime_error("AppendColumnToArrow: vector is nullptr");
    }
    switch (acc.kind) {
        case ComplexColumnAccumulator::Kind::FIXED:   AppendFlatToArrow(vector, row_ids, acc); return;
        case ComplexColumnAccumulator::Kind::VARLEN:  AppendStringToArrow(vector, row_ids, acc); return;
        case ComplexColumnAccumulator::Kind::LIST:    AppendArrayToArrow(vector, row_ids, dataType, acc); return;
        case ComplexColumnAccumulator::Kind::MAP:     AppendMapToArrow(vector, row_ids, dataType, acc); return;
        case ComplexColumnAccumulator::Kind::STRUCT:  AppendRowToArrow(vector, row_ids, dataType, acc); return;
        default:
            throw std::runtime_error("AppendColumnToArrow: unsupported accumulator kind");
    }
}

void Splitter::AppendFlatToArrow(BaseVector *vector, std::vector<uint32_t> row_ids,
                                 ComplexColumnAccumulator& acc)
{
    int32_t numRows = static_cast<int32_t>(row_ids.size());
    int32_t T_size = acc.fixedElemSize;

    // Ensure values capacity
    int64_t needBytes = (acc.rowCursor + numRows) * T_size;
    acc.EnsureValuesCapacity(needBytes);
    uint8_t* dst = acc.values->mutable_data() + acc.rowCursor * T_size;

    if (vector->GetEncoding() == OMNI_ENCODING_CONST) {
        // ConstVector: fill all rows with the same value
        // Get const value bytes and replicate
        uint8_t constValueBytes[16] = {};
        auto typeId = vector->GetTypeId();
        bool constIsNull = false;
        // Use UnsafeGetValues to get the const value
        const uint8_t* src = static_cast<const uint8_t*>(VectorHelper::UnsafeGetValues(vector));
        if (src != nullptr) {
            memcpy(constValueBytes, src, T_size);
        }
        for (int32_t i = 0; i < numRows; ++i) {
            memcpy(dst + i * T_size, constValueBytes, T_size);
        }
        // Handle nulls for const
        for (int32_t i = 0; i < numRows; ++i) {
            bool isNull = vector->IsNull(row_ids[i]);
            acc.AppendValidBit(!isNull);
        }
    } else if (vector->GetEncoding() == OMNI_FLAT) {
        auto srcAddr = reinterpret_cast<int64_t>(VectorHelper::UnsafeGetValues(vector));
        for (int32_t i = 0; i < numRows; ++i) {
            auto rowId = row_ids[i];
            bool isNull = vector->IsNull(rowId);
            if (!isNull) {
                memcpy(dst + i * T_size, reinterpret_cast<const void*>(srcAddr + rowId * T_size), T_size);
            } else {
                memset(dst + i * T_size, 0, T_size);
            }
            acc.AppendValidBit(!isNull);
        }
    } else if (vector->GetEncoding() == OMNI_DICTIONARY) {
        auto ids_addr = static_cast<const int32_t*>(VectorHelper::UnsafeGetValues(vector));
        const uint8_t* dictData = reinterpret_cast<const uint8_t*>(VectorHelper::UnsafeGetDictionary(vector));
        for (int32_t i = 0; i < numRows; ++i) {
            auto rowId = row_ids[i];
            bool isNull = vector->IsNull(rowId);
            if (!isNull) {
                int32_t dictIdx = ids_addr[rowId];
                memcpy(dst + i * T_size, dictData + dictIdx * T_size, T_size);
            } else {
                memset(dst + i * T_size, 0, T_size);
            }
            acc.AppendValidBit(!isNull);
        }
    } else {
        throw std::runtime_error("AppendFlatToArrow: unsupported encoding");
    }

    acc.rowCursor += numRows;
}

void Splitter::AppendStringToArrow(BaseVector *vector, std::vector<uint32_t> row_ids,
                                   ComplexColumnAccumulator& acc)
{
    int32_t numRows = static_cast<int32_t>(row_ids.size());

    // Ensure offsets capacity
    acc.EnsureOffsetsCapacity(acc.rowCursor + numRows + 1);
    int32_t* offsets = reinterpret_cast<int32_t*>(acc.offsets->mutable_data());
    int32_t base = static_cast<int32_t>(acc.valueBytesCursor);

    // ORC varchar inside struct/array/map is often dictionary-encoded. Reading it as a flat
    // LargeStringContainer treats dictionary ids as string offsets and yields empty strings.
    const auto encoding = vector->GetEncoding();
    ConstVector<std::string_view> *constVec = nullptr;
    Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *dictVec = nullptr;
    Vector<LargeStringContainer<std::string_view>> *stringVec = nullptr;
    bool constIsNull = false;
    std::string_view constValue;
    if (encoding == OMNI_ENCODING_CONST) {
        constVec = reinterpret_cast<ConstVector<std::string_view> *>(vector);
        constIsNull = constVec->HasNull() && constVec->IsNull(0);
        if (!constIsNull) {
            constValue = constVec->GetConstValue();
        }
    } else if (encoding == OMNI_DICTIONARY) {
        dictVec = reinterpret_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *>(vector);
    } else if (encoding == OMNI_FLAT) {
        stringVec = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
    } else {
        throw std::runtime_error(
            std::string("AppendStringToArrow: unsupported vector encoding ") +
            std::to_string(static_cast<int>(encoding)));
    }

    int32_t cur = base;
    for (int32_t i = 0; i < numRows; ++i) {
        auto rowId = row_ids[i];
        bool isNull = false;
        std::string_view sv;
        if (encoding == OMNI_ENCODING_CONST) {
            isNull = constIsNull;
            sv = constValue;
        } else if (encoding == OMNI_DICTIONARY) {
            isNull = dictVec->IsNull(rowId);
            if (!isNull) {
                sv = dictVec->GetValue(rowId);
            }
        } else {
            isNull = stringVec->IsNull(rowId);
            if (!isNull) {
                sv = stringVec->GetValue(rowId);
            }
        }
        if (isNull) {
            acc.AppendValidBit(false);
            offsets[acc.rowCursor + i + 1] = cur;
        } else {
            acc.AppendValidBit(true);
            int32_t len = static_cast<int32_t>(sv.size());
            if (len > 0) {
                int64_t needBytes = acc.valueBytesCursor + len;
                if (acc.values->capacity() < needBytes) {
                    acc.EnsureValuesCapacity(needBytes);
                    // mutable_data may have changed after Reserve
                    offsets = reinterpret_cast<int32_t*>(acc.offsets->mutable_data());
                }
                memcpy(acc.values->mutable_data() + cur, sv.data(), len);
                cur += len;
                acc.valueBytesCursor = cur;
            }
            offsets[acc.rowCursor + i + 1] = cur;
        }
    }

    acc.rowCursor += numRows;
}

void Splitter::AppendArrayToArrow(BaseVector *vector, std::vector<uint32_t> row_ids,
                                  DataTypePtr dataType, ComplexColumnAccumulator& acc)
{
    auto* arrayVec = reinterpret_cast<ArrayVector*>(vector);
    int32_t numRows = static_cast<int32_t>(row_ids.size());

    // Ensure offsets capacity
    acc.EnsureOffsetsCapacity(acc.rowCursor + numRows + 1);
    int32_t* offsets = reinterpret_cast<int32_t*>(acc.offsets->mutable_data());
    int32_t childElemBase = static_cast<int32_t>(acc.elemCursor);

    // Per-row: compute element count, write offsets, collect element positions
    std::vector<uint32_t> elementPositions;
    int32_t cur = childElemBase;
    for (int32_t i = 0; i < numRows; ++i) {
        auto rowId = row_ids[i];
        if (arrayVec->IsNull(rowId)) {
            acc.AppendValidBit(false);
            offsets[acc.rowCursor + i + 1] = cur;
        } else {
            acc.AppendValidBit(true);
            int64_t startPos = arrayVec->GetOffset(rowId);
            int64_t arraySize = arrayVec->GetSize(rowId);
            for (int64_t j = 0; j < arraySize; ++j) {
                elementPositions.push_back(static_cast<uint32_t>(startPos + j));
            }
            cur += static_cast<int32_t>(arraySize);
            offsets[acc.rowCursor + i + 1] = cur;
        }
    }

    // Recurse into child element vector
    auto arrayType = std::dynamic_pointer_cast<ArrayType>(dataType);
    DataTypePtr elementType = arrayType->ElementType();
    auto* elementsVec = arrayVec->GetElementVector().get();
    AppendColumnToArrow(elementsVec, elementPositions, elementType, *acc.children[0]);

    acc.elemCursor = cur;
    acc.rowCursor += numRows;
}

void Splitter::AppendMapToArrow(BaseVector *vector, std::vector<uint32_t> row_ids,
                                DataTypePtr dataType, ComplexColumnAccumulator& acc)
{
    auto* mapVec = reinterpret_cast<MapVector*>(vector);
    int32_t numRows = static_cast<int32_t>(row_ids.size());

    acc.EnsureOffsetsCapacity(acc.rowCursor + numRows + 1);
    int32_t* offsets = reinterpret_cast<int32_t*>(acc.offsets->mutable_data());
    int32_t kvElemBase = static_cast<int32_t>(acc.elemCursor);

    std::vector<uint32_t> kvPositions;
    int32_t cur = kvElemBase;
    for (int32_t i = 0; i < numRows; ++i) {
        auto rowId = row_ids[i];
        if (mapVec->IsNull(rowId)) {
            acc.AppendValidBit(false);
            offsets[acc.rowCursor + i + 1] = cur;
        } else {
            acc.AppendValidBit(true);
            int64_t startPos = mapVec->GetOffset(rowId);
            int64_t mapSize = mapVec->GetSize(rowId);
            for (int64_t j = 0; j < mapSize; ++j) {
                kvPositions.push_back(static_cast<uint32_t>(startPos + j));
            }
            cur += static_cast<int32_t>(mapSize);
            offsets[acc.rowCursor + i + 1] = cur;
        }
    }

    auto mapType = std::dynamic_pointer_cast<MapType>(dataType);
    AppendColumnToArrow(mapVec->GetKeyVector().get(), kvPositions, mapType->Key(), *acc.children[0]);
    AppendColumnToArrow(mapVec->GetValueVector().get(), kvPositions, mapType->Value(), *acc.children[1]);

    acc.elemCursor = cur;
    acc.rowCursor += numRows;
}

void Splitter::AppendRowToArrow(BaseVector *vector, std::vector<uint32_t> row_ids,
                                DataTypePtr dataType, ComplexColumnAccumulator& acc)
{
    auto* rowVec = reinterpret_cast<RowVector*>(vector);
    int32_t numRows = static_cast<int32_t>(row_ids.size());

    // Write validity (no offsets for STRUCT)
    for (int32_t i = 0; i < numRows; ++i) {
        acc.AppendValidBit(!rowVec->IsNull(row_ids[i]));
    }

    // Recurse each child field (same row_ids, same row count)
    auto& children = rowVec->Children();
    auto rowType = std::dynamic_pointer_cast<RowType>(dataType);
    for (size_t c = 0; c < children.size(); ++c) {
        DataTypePtr childType = rowType->childAt(static_cast<uint32_t>(c));
        AppendColumnToArrow(children[c].get(), row_ids, childType, *acc.children[c]);
    }

    acc.rowCursor += numRows;
}

int Splitter::SplitComplexColumns(VectorBatch& vb)
{
    for (auto &pid: partition_used_) {
        auto pos = partition_row_offset_base_[pid];
        auto end = partition_row_offset_base_[pid + 1];
        auto num_rows = end - pos;
        std::vector<uint32_t> row_ids(num_rows);
        for (int32_t i = 0; pos < end; ++pos, ++i) {
            row_ids[i] = row_offset_row_id_[pos];
        }

        for (uint complex_col_idx = 0; complex_col_idx < complex_type_array_idx_.size(); ++complex_col_idx) {
            auto col_idx_vb = complex_type_array_idx_[complex_col_idx];
            auto *vector = vb.Get(col_idx_vb);
            int32_t col_idx_schema = singlePartitionFlag ? col_idx_vb : (col_idx_vb - 1);
            DataTypePtr dataType = inputDataTypes_[col_idx_schema];

            // 方案 C: 增量追加到 accumulator（替代 SerializeColumnToArrow + push_back）
            auto& acc = partition_complex_accumulators_[pid][complex_col_idx];
            // 懒初始化：首次使用时按 dataType 创建 accumulator（inputDataTypes_ 在 Split_Init 后才可用）
            if (!acc) {
                acc = std::make_unique<ComplexColumnAccumulator>();
                acc->Init(dataType, options_.buffer_size, arrow_pool_.get());
            }
            AppendColumnToArrow(vector, row_ids, dataType, *acc);
        }
    }

    return 0;
}

int Splitter::SplitBinaryArray(VectorBatch& vb)
{
    auto vec_cnt_vb = vb.GetVectorCount(); // Total column count, possibly including partition ID
    auto vec_cnt_schema = singlePartitionFlag ? vec_cnt_vb : vec_cnt_vb - 1; // Schema column count: subtract partition ID column unless in single-partition mode
    for (auto col_schema = 0; col_schema < vec_cnt_schema; ++col_schema) {
        switch (column_type_id_[col_schema]) {
            case SHUFFLE_BINARY: {
                auto col_vb = singlePartitionFlag ? col_schema : col_schema + 1;
                auto *varcharVector = vb.Get(col_vb);
                varcharVectorCache.insert(varcharVector);
                if (varcharVector->HasNull()) {
                    this->template SplitBinaryVector<true>(varcharVector, col_schema);
                } else {
                    this->template SplitBinaryVector<false>(varcharVector, col_schema);
                }
                break;
            }
            case SHUFFLE_LARGE_BINARY:
                break;
            default:{
                break;
            }
        }
    }
    return 0;
}

int Splitter::SplitFixedWidthValidityBuffer(VectorBatch& vb){
    for (uint col = 0; col < fixed_width_array_idx_.size(); ++col) {
        auto col_idx = fixed_width_array_idx_[col];
        auto& dst_addrs = partition_fixed_width_validity_addrs_[col];

        if (vb.Get(col_idx)->HasNull()) {
            for (auto pid = 0; pid < num_partitions_; ++pid) {
                if (partition_id_cnt_cur_[pid] > 0 && dst_addrs[pid] == nullptr) {
                    // init bitmap if it's null
                    // Cover the whole value buffer: rows already in it before this batch read as valid.
                    auto new_size = partition_buffer_size_[pid];
                    auto ptr_tmp = static_cast<uint8_t *>(options_.allocator->Alloc(new_size));
                    if (nullptr == ptr_tmp) {
                        throw std::runtime_error("Allocator for ValidityBuffer Failed! ");
                    }
                    std::shared_ptr<Buffer> validity_buffer (
                        new Buffer((uint8_t *)ptr_tmp, partition_id_cnt_cur_[pid], new_size, options_.allocator));
                    dst_addrs[pid] = const_cast<uint8_t*>(validity_buffer->data_);
                    memset(validity_buffer->data_, 0, new_size);
                    partition_fixed_width_buffers_[col][pid][0] = std::move(validity_buffer);
                    fixed_nullBuffer_size_[pid] += new_size;
                }
            }

            Encoding validityEnc = vb.Get(col_idx)->GetEncoding();
            if (validityEnc == OMNI_ENCODING_CONST) {
                uint8_t constNullVal = vb.Get(col_idx)->IsNull(0) ? 1 : 0;
                for (auto &pid : partition_used_) {
                    auto dstPidBase = dst_addrs[pid] + partition_buffer_idx_base_[pid];
                    auto pos = partition_row_offset_base_[pid];
                    auto end = partition_row_offset_base_[pid + 1];
                    for (; pos < end; ++pos) {
                        *dstPidBase++ = constNullVal;
                    }
                }
            } else if (validityEnc == OMNI_DICTIONARY) {
                for (auto &pid: partition_used_) {
                    auto dstPidBase = dst_addrs[pid] + partition_buffer_idx_base_[pid];
                    auto pos = partition_row_offset_base_[pid];
                    auto end = partition_row_offset_base_[pid + 1];
                    for (; pos < end; ++pos) {
                        auto rowId = row_offset_row_id_[pos];
                        *dstPidBase++ = vb.Get(col_idx)->IsNull(rowId);
                    }
                }
            } else if (validityEnc == OMNI_FLAT) {
                auto src_addr = unsafe::UnsafeBaseVector::GetNulls(vb.Get(col_idx));
                for (auto &pid: partition_used_) {
                    auto dstPidBase = dst_addrs[pid] + partition_buffer_idx_base_[pid];
                    auto pos = partition_row_offset_base_[pid];
                    auto end = partition_row_offset_base_[pid + 1];
                    for (; pos < end; ++pos) {
                        auto rowId = row_offset_row_id_[pos];
                        *dstPidBase++ = omniruntime::BitUtil::IsBitSet(src_addr, rowId);
                    }
                }
            } else {
             	throw std::runtime_error(
             	    std::string("SplitFixedWidthValidityBuffer: unsupported vector encoding ") +
             	    std::to_string(static_cast<int>(validityEnc)));
            }
        }
    }
    return 0;
}

int Splitter::CacheVectorBatch(int32_t partition_id, bool reset_buffers, arrow::MemoryPool *bufferPool) {
    arrow::MemoryPool *pool = bufferPool != nullptr ? bufferPool : arrow_pool_.get();
    // 定宽列改用 Arrow buffer 缓存批（Task 9: 阶段A —— 一个缓存批 = 一帧，零拷贝引用）
    // Task 11: 也须处理仅有复杂类型数据的情况（fixed_width_array_idx_ 可能为空）
    bool hasFixedData = (partition_buffer_idx_base_[partition_id] > 0 && fixed_width_array_idx_.size() > 0);
    bool hasComplexData = false;
    if (complex_type_array_idx_.size() > 0) {
        for (uint k = 0; k < complex_type_array_idx_.size(); ++k) {
            if (k < partition_complex_accumulators_[partition_id].size() &&
                partition_complex_accumulators_[partition_id][k] &&
                partition_complex_accumulators_[partition_id][k]->rowCursor > 0) {
                hasComplexData = true;
                break;
            }
        }
    }
    // Task 12: 纯变长列（VARCHAR/CHAR/BINARY）也需要缓存 —— 检查 vc_partition_array_buffers_
    bool hasBinaryData = false;
    for (int i = 0; i < num_fields_; ++i) {
        if ((column_type_id_[i] == SHUFFLE_BINARY || column_type_id_[i] == SHUFFLE_LARGE_BINARY) &&
            !vc_partition_array_buffers_[partition_id][i].empty()) {
            hasBinaryData = true;
            break;
        }
    }

    if (hasFixedData || hasComplexData || hasBinaryData) {
        // 当 hasFixedData=false 时，partition_id_cnt_cur_ 仅含最后一批的行数（每批开头 memset 清零），
        // 不能代表变长列累积的全部行数。此时应以变长列 VCBatchInfo 累计行数为准。
        int32_t num_rows;
        if (hasFixedData) {
            num_rows = partition_buffer_idx_base_[partition_id];
        } else {
            // 纯变长/复杂类型场景：从第一个有数据的变长列取累计行数。
            // 所有变长列的行数应相同（来自同一批输入、同一散列顺序），取第一列即可。
            num_rows = 0;
            for (int i = 0; i < num_fields_; ++i) {
                if (column_type_id_[i] == SHUFFLE_BINARY || column_type_id_[i] == SHUFFLE_LARGE_BINARY) {
                    for (const auto& vcb : vc_partition_array_buffers_[partition_id][i]) {
                        num_rows += static_cast<int32_t>(vcb.getVcList().size());
                    }
                    if (num_rows > 0) break;  // 只取第一个有数据的变长列
                }
            }
            // 纯复杂类型场景：从 accumulator 的 rowCursor 取累积行数
            if (num_rows == 0) {
                for (uint k = 0; k < complex_type_array_idx_.size(); ++k) {
                    if (k < partition_complex_accumulators_[partition_id].size() &&
                        partition_complex_accumulators_[partition_id][k] &&
                        partition_complex_accumulators_[partition_id][k]->rowCursor > 0) {
                        num_rows = static_cast<int32_t>(partition_complex_accumulators_[partition_id][k]->rowCursor);
                        break;
                    }
                }
                if (num_rows == 0) {
                    num_rows = static_cast<int32_t>(partition_id_cnt_cur_[partition_id]);
                }
            }
        }
        auto num_fields = num_fields_;
        auto fixed_width_idx = 0;

        ArrowColumnarCachedBatch arrow_batch;
        arrow_batch.rowCount = num_rows;

        for (int i = 0; i < num_fields; ++i) {
            switch (column_type_id_[i]) {
                case SHUFFLE_BINARY:
                case SHUFFLE_LARGE_BINARY: {
                    // Task 10: 变长列 gather 到 Arrow buffer（离线 gather，C3 保留）
                    // VCBatchInfo 条目与定宽批行数应对齐（同输入批、同分区、相同散列顺序）
                    auto& vcBatches = vc_partition_array_buffers_[partition_id][i];
                    if (!vcBatches.empty()) {
                        int32_t vcRows = 0;
                        int64_t totalValuesSize = 0;
                        bool hasNull = false;
                        for (const auto& vcb : vcBatches) {
                            vcRows += static_cast<int32_t>(vcb.getVcList().size());
                            totalValuesSize += vcb.getVcbTotalLen();
                            if (vcb.hasNull()) hasNull = true;
                        }

                        int32_t gatherRows = static_cast<int32_t>(
                            std::min(static_cast<int64_t>(num_rows), static_cast<int64_t>(vcRows)));

                        // --- validity: Omni VCLocation.is_null → Arrow bitmap（置位=有效）---
                        std::shared_ptr<arrow::Buffer> arrow_validity = nullptr;
                        if (hasNull && gatherRows > 0) {
                            int32_t byteCount = (gatherRows + 7) / 8;
                            auto vr = arrow::AllocateResizableBuffer(byteCount, pool);
                            if (!vr.ok()) {
                                throw std::runtime_error(
                                    "CacheVectorBatch Arrow varchar validity alloc failed: " + vr.status().ToString());
                            }
                            auto bitmapBuf = std::move(*vr);
                            uint8_t* bitmap = bitmapBuf->mutable_data();
                            memset(bitmap, 0, byteCount);
                            arrow_validity = std::move(bitmapBuf);
                        }

                        // --- offsets: int32 数组，(gatherRows+1) × 4 字节 ---
                        int64_t offsetsSize = static_cast<int64_t>(gatherRows + 1) * sizeof(int32_t);
                        auto orStatus = arrow::AllocateResizableBuffer(offsetsSize, pool);
                        if (!orStatus.ok()) {
                            throw std::runtime_error(
                                "CacheVectorBatch Arrow varchar offsets alloc failed: " + orStatus.status().ToString());
                        }
                        auto offsetsBuf = std::move(*orStatus);
                        int32_t* offsets = reinterpret_cast<int32_t*>(offsetsBuf->mutable_data());

                        // --- values: 拼接字符串体 ---
                        auto vr2 = arrow::AllocateResizableBuffer(totalValuesSize, pool);
                        if (!vr2.ok()) {
                            throw std::runtime_error(
                                "CacheVectorBatch Arrow varchar values alloc failed: " + vr2.status().ToString());
                        }
                        auto valuesBuf = std::move(*vr2);
                        char* values = reinterpret_cast<char*>(valuesBuf->mutable_data());

                        // --- Gather: 遍历 VCLocations，取反映射 validity，memcpy 拼接串体 ---
                        offsets[0] = 0;
                        int rowIdx = 0;
                        int64_t actualValuesSize = 0;
                        for (auto& vcb : vcBatches) {
                            auto& lst = vcb.getVcList();
                            for (auto& loc : lst) {
                                if (rowIdx >= gatherRows) break;
                                // validity 取反：Omni is_null → Arrow bit=0; !is_null → Arrow bit=1
                                if (hasNull && !loc.get_is_null()) {
                                    int32_t byteIdx = rowIdx / 8;
                                    int32_t bitIdx = rowIdx % 8;
                                    arrow_validity->mutable_data()[byteIdx] |= (1u << bitIdx);
                                }
                                int32_t len = loc.get_vc_len();
                                if (len > 0) {
                                    memcpy(values + offsets[rowIdx],
                                           reinterpret_cast<const char*>(loc.get_vc_addr()), len);
                                }
                                offsets[rowIdx + 1] = offsets[rowIdx] + len;
                                actualValuesSize += len;
                                rowIdx++;
                            }
                            if (rowIdx >= gatherRows) break;
                        }

                        if (actualValuesSize < totalValuesSize) {
                            valuesBuf->Resize(actualValuesSize);
                        }

                        // Arrow 变长列 buffer 顺序：[validity][offsets][values]
                        arrow_batch.buffers.push_back(std::move(arrow_validity));
                        arrow_batch.buffers.push_back(std::move(offsetsBuf));
                        arrow_batch.buffers.push_back(std::move(valuesBuf));
                    } else {
                        // 该列无变长数据：构造全空串的有效 buffer。
                        // 读侧要求变长列 offsets/values 非空（Arrow 约定），
                        // offsets = (num_rows+1)×4 全 0 表示所有字符串长度为 0（空串），
                        // values = 长度 0 的空 buffer。
                        // validity = nullptr 哨兵表示全有效（空串 = 有效值）。
                        arrow_batch.buffers.push_back(nullptr);  // validity 全有效

                        int32_t offsetsLen = (num_rows + 1) * static_cast<int32_t>(sizeof(int32_t));
                        auto offsetsR = arrow::AllocateBuffer(offsetsLen, pool);
                        if (!offsetsR.ok()) {
                            throw std::runtime_error("CacheVectorBatch empty offsets alloc failed: "
                                                     + offsetsR.status().ToString());
                        }
                        auto emptyOffsetsBuf = std::move(*offsetsR);
                        std::memset(emptyOffsetsBuf->mutable_data(), 0, static_cast<size_t>(offsetsLen));
                        arrow_batch.buffers.push_back(std::move(emptyOffsetsBuf));  // offsets 全 0

                        auto valuesR = arrow::AllocateBuffer(0, pool);
                        if (!valuesR.ok()) {
                            throw std::runtime_error("CacheVectorBatch empty values alloc failed: "
                                                     + valuesR.status().ToString());
                        }
                        arrow_batch.buffers.push_back(std::move(*valuesR));  // values 空
                    }
                    break;
                }
                case SHUFFLE_ARRAY:
                case SHUFFLE_MAP:
                case SHUFFLE_ROW:
                case SHUFFLE_NULL: {
                    // Task 11: 复杂类型——从 Arrow 缓冲累积列表合并后推入缓存批
                    int complexColIdx = -1;
                    for (uint k = 0; k < complex_type_array_idx_.size(); ++k) {
                        int expectedVbIdx = singlePartitionFlag ? i : (i + 1);
                        if (static_cast<int>(complex_type_array_idx_[k]) == expectedVbIdx) {
                            complexColIdx = static_cast<int>(k);
                            break;
                        }
                    }
                    if (complexColIdx >= 0) {
                        auto& acc = partition_complex_accumulators_[partition_id][complexColIdx];
                        LogsInfo("CacheVectorBatch complex: pid=%d col=%d accExist=%d rowCursor=%lld num_rows=%d",
                                 partition_id, complexColIdx, acc ? 1 : 0,
                                 acc ? (long long)acc->rowCursor : -1, num_rows);
                        if (acc && acc->rowCursor > 0) {
                            // 方案 C: 直接从 accumulator 取出 buffer（零拷贝引用）
                            size_t bufCountBefore = arrow_batch.buffers.size();
                            acc->CollectBuffers(arrow_batch.buffers);
                            LogsInfo("CacheVectorBatch complex: CollectBuffers pushed %zu buffers (before=%zu after=%zu)",
                                     arrow_batch.buffers.size() - bufCountBefore, bufCountBefore, arrow_batch.buffers.size());
                        } else if (acc) {
                            // 该列本批无数据（rowCursor==0）：构造空 buffer 占位
                            acc->CollectEmptyBuffers(arrow_batch.buffers, num_rows);
                        } else {
                            // acc 未初始化（该列从未收到数据）。
                            // 按 dataType 临时创建一个 accumulator 来调用 CollectEmptyBuffers，
                            // 确保递归产出正确数量的 buffer（与 NumBuffers 一致），
                            // 而非硬编码 3 个 buffer（对嵌套类型数量不足，会导致读侧 buffer 错位）。
                            int32_t col_idx_vb = complex_type_array_idx_[complexColIdx];
                            int32_t col_idx_schema = singlePartitionFlag ? col_idx_vb : (col_idx_vb - 1);
                            DataTypePtr dt = inputDataTypes_[col_idx_schema];
                            auto tmpAcc = std::make_unique<ComplexColumnAccumulator>();
                            tmpAcc->Init(dt, options_.buffer_size, arrow_pool_.get());
                            tmpAcc->CollectEmptyBuffers(arrow_batch.buffers, num_rows);
                        }
                    }
                    break;
                }
                default: {
                    int32_t type_size = (1 << column_type_id_[i]);

                    // --- validity: Omni 逐字节缓冲 → Arrow bitmap（写侧取反：Omni 置位=null → Arrow 置位=valid）---
                    auto& omni_validity = partition_fixed_width_buffers_[fixed_width_idx][partition_id][0];
                    std::shared_ptr<arrow::Buffer> arrow_validity = nullptr;

                    if (omni_validity != nullptr) {
                        uint8_t* null_bytes = omni_validity->data_;
                        // 检查是否有 null（全有效则 validity 置 nullptr 哨兵）
                        bool has_null = false;
                        for (int32_t r = 0; r < num_rows; ++r) {
                            if (null_bytes[r] != 0) {
                                has_null = true;
                                break;
                            }
                        }

                        if (has_null) {
                            int32_t byte_count = (num_rows + 7) / 8;
                            auto vr = arrow::AllocateResizableBuffer(byte_count, pool);
                            if (!vr.ok()) {
                                throw std::runtime_error(
                                    "CacheVectorBatch Arrow validity alloc failed: " + vr.status().ToString());
                            }
                            auto bitmap_buf = std::move(*vr);
                            uint8_t* bitmap = bitmap_buf->mutable_data();
                            memset(bitmap, 0, byte_count);

                            // 取反映射：Omni null_byte != 0 → Arrow bit = 0; null_byte == 0 → Arrow bit = 1
                            for (int32_t row = 0; row < num_rows; ++row) {
                                if (null_bytes[row] == 0) {  // Omni: not null → Arrow: valid
                                    int32_t byte_idx = row / 8;
                                    int32_t bit_idx = row % 8;
                                    bitmap[byte_idx] |= (1u << bit_idx);
                                }
                            }
                            bitmap_buf->Resize(byte_count);
                            arrow_validity = std::move(bitmap_buf);
                        }
                        // else: 全有效 → arrow_validity 保持 nullptr (哨兵)

                        omni_validity.reset();  // 释放 Omni validity buffer
                    }
                    // else: 该列无 null → arrow_validity 保持 nullptr

                    arrow_batch.buffers.push_back(std::move(arrow_validity));

                    // --- values: 快照 Arrow ResizableBuffer（零拷贝引用，缓存批 = 写出帧）---
                    auto& arrow_values = partition_fixed_width_arrow_buffers_[fixed_width_idx][partition_id];
                    if (arrow_values) {
                        int64_t actual_data_size = static_cast<int64_t>(num_rows) * type_size;
                        // 无条件精确设置逻辑大小为实际数据大小。
                        // AllocatePartitionBuffers 时 Resize(needed_size) 保留了完整容量，
                        // 这里 Resize(actual_data_size) 确保 size() = 实际写入字节数，
                        // 使 WriteColumnarBatch 用 b->size() 写出正确的字节数（不多写垃圾）。
                        // actual_data_size <= capacity（由 CheckCapacityAndAllocate 保证）。
                        // Shrinking to fit reallocates, which takes the smaller buffer before freeing
                        // the old one. A reclaim spill writes and releases this batch right away and
                        // has no headroom in the task pool, so it keeps the capacity.
                        arrow_values->Resize(actual_data_size, /*shrink_to_fit=*/bufferPool == nullptr);
                        arrow_batch.buffers.push_back(arrow_values);
                    } else {
                        arrow_batch.buffers.push_back(nullptr);
                    }

                    fixed_width_idx++;
                    break;
                }
            }
        }

        size_t cachedBufferNum = arrow_batch.buffers.size();
        partition_arrow_batch_[partition_id].push_back(std::move(arrow_batch));

        // 缓存大小统计改用 arrow_pool_ 统一记账（含 values + validity bitmap）
        cached_vectorbatch_size_ = arrow_pool_->bytes_allocated();

        // 清理散列状态，为下一缓存批做准备
        if (reset_buffers) {
            fixed_width_idx = 0;
            for (int i = 0; i < num_fields; ++i) {
                switch (column_type_id_[i]) {
                    case SHUFFLE_BINARY:
                    case SHUFFLE_LARGE_BINARY: {
                        // Arrow 路径已在上方将 VCBatchInfo 中的数据 gather 到 Arrow buffer。
                        // 不再有其他路径需要访问这些条目。
                        // 必须清除，否则下次 CacheVectorBatch 会重复 gather 旧数据，
                        // 导致变长列数据与定宽列数据错位（分区数据量 > buffer_size 时触发）。
                        vc_partition_array_buffers_[partition_id][i].clear();
                        break;
                    }
                    case SHUFFLE_ARRAY:
                    case SHUFFLE_MAP:
                    case SHUFFLE_ROW:
                    case SHUFFLE_NULL: {
                        // 方案 C: Reset accumulator（清游标，复用 buffer）
                        int complexColIdx = -1;
                        for (uint k = 0; k < complex_type_array_idx_.size(); ++k) {
                            int expectedVbIdx = singlePartitionFlag ? i : (i + 1);
                            if (static_cast<int>(complex_type_array_idx_[k]) == expectedVbIdx) {
                                complexColIdx = static_cast<int>(k);
                                break;
                            }
                        }
                        if (complexColIdx >= 0) {
                            auto& acc = partition_complex_accumulators_[partition_id][complexColIdx];
                            if (acc) {
                                acc->Reset();
                                acc.reset();  // release unique_ptr so lazy-init recreates it
                            }
                        }
                        break;
                    }
                    default: {
                        // 释放 Arrow value buffer（缓存批已持有引用，此处释放 partition 引用）
                        partition_fixed_width_arrow_buffers_[fixed_width_idx][partition_id].reset();
                        // 清理 Omni 引用（validity 已在上面 reset）
                        partition_fixed_width_buffers_[fixed_width_idx][partition_id][0].reset();
                        partition_fixed_width_buffers_[fixed_width_idx][partition_id][1].reset();
                        // 清空散列地址
                        partition_fixed_width_validity_addrs_[fixed_width_idx][partition_id] = nullptr;
                        partition_fixed_width_value_addrs_[fixed_width_idx][partition_id] = nullptr;
                        fixed_width_idx++;
                        break;
                    }
                }
            }
        }

        partition_buffer_idx_base_[partition_id] = 0;
    }
    return 0;
}

int Splitter::DoSplit(VectorBatch& vb) {
    // prepare partition buffers and spill if necessary
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        bool hasFixed = fixed_width_array_idx_.size() > 0;
        bool hasComplex = complex_type_array_idx_.size() > 0;
        if ((hasFixed || hasComplex) &&
            partition_id_cnt_cur_[pid] > 0 &&
            partition_buffer_idx_base_[pid] + partition_id_cnt_cur_[pid] > partition_buffer_size_[pid]) {
            auto new_size = NextPartitionBufferRows(pid, partition_id_cnt_cur_[pid]);
            if (GrowsInPlace(pid, partition_id_cnt_cur_[pid])) {
                GrowPartitionBuffers(pid, new_size);
            } else if (partition_buffer_size_[pid] == 0) {
                AllocatePartitionBuffers(pid, new_size);
            } else {
                    CacheVectorBatch(pid, true);
                    AllocatePartitionBuffers(pid, new_size);
            }
        }
    }
    BuildPartition2Row(vb.GetRowCount());

    SplitFixedWidthValueBuffer(vb);
    SplitFixedWidthValidityBuffer(vb);

    // 更新分区缓冲基址（arrow_pool_ 自动记账，不再逐 pid 累加 omni 分配量）
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        partition_buffer_idx_base_[pid] += partition_id_cnt_cur_[pid];
    }

    // Binary split last vector batch...
    SplitBinaryArray(vb);

    // Complex type split
    if (complex_type_array_idx_.size() > 0) {
        SplitComplexColumns(vb);
    }

    num_row_splited_ += vb.GetRowCount();
    // release the fixed width vector and release vectorBatch at the same time
    ReleaseVectorBatch(&vb);
    this->ResetInputVecBatch();

    // spill
    // process level: If the memory usage of the current executor exceeds the threshold, spill is triggered.
    // The legacy process-global accounted-memory query no longer exists, so the
    // executor-level trigger is effectively disabled; the task-level trigger
    // below still fires from `cached_vectorbatch_size_`.
    uint64_t usedMemorySize = 0;
    if (usedMemorySize > options_.executor_spill_mem_threshold) {
        if (rss_mode_) {
            TIME_NANO_OR_RAISE(total_spill_time_, SpillToRss());
        } else {
            TIME_NANO_OR_RAISE(total_spill_time_, SpillToTmpFile());
            isSpill = true;
        }
    }

    // task level: Arrow pool 统一记账（覆盖定宽+变长+复杂+行式全部 Arrow buffer）
    if (arrow_pool_->bytes_allocated() >= options_.task_spill_mem_threshold) {
        if (rss_mode_) {
            TIME_NANO_OR_RAISE(total_spill_time_, SpillToRss());
        } else {
            TIME_NANO_OR_RAISE(total_spill_time_, SpillToTmpFile());
            isSpill = true;
        }
    }
    return 0;
}

void Splitter::ToSplitterTypeId(int num_cols)
{
    for (int i = 0; i < num_cols; ++i) {
        switch (input_col_types.inputVecTypeIds[i]) {
            case OMNI_BYTE: {
                CastOmniToShuffleType(OMNI_BYTE, SHUFFLE_1BYTE);
                break;
            }
            case OMNI_BOOLEAN: {
                CastOmniToShuffleType(OMNI_BOOLEAN, SHUFFLE_1BYTE);
                break;
            }
            case OMNI_SHORT: {
                CastOmniToShuffleType(OMNI_SHORT, SHUFFLE_2BYTE);
                break;
            }
            case OMNI_INT: {
                CastOmniToShuffleType(OMNI_INT, SHUFFLE_4BYTE);
                break;
            }
            case OMNI_LONG: {
                CastOmniToShuffleType(OMNI_LONG, SHUFFLE_8BYTE);
                break;
            }
            case OMNI_TIMESTAMP: {
                CastOmniToShuffleType(OMNI_TIMESTAMP, SHUFFLE_8BYTE);
                break;
            }
            case OMNI_DOUBLE: {
                CastOmniToShuffleType(OMNI_DOUBLE, SHUFFLE_8BYTE);
                break;
            }
            case OMNI_FLOAT: {
                CastOmniToShuffleType(OMNI_FLOAT, SHUFFLE_4BYTE);
                break;
            }
            case OMNI_DATE32: {
                CastOmniToShuffleType(OMNI_DATE32, SHUFFLE_4BYTE);
                break;
            }
            case OMNI_DATE64: {
                CastOmniToShuffleType(OMNI_DATE64, SHUFFLE_8BYTE);
                break;
            }
            case OMNI_DECIMAL64: {
                CastOmniToShuffleType(OMNI_DECIMAL64, SHUFFLE_8BYTE);
                break;
            }
            case OMNI_DECIMAL128: {
                CastOmniToShuffleType(OMNI_DECIMAL128, SHUFFLE_DECIMAL128);
                break;
            }
            case OMNI_VARBINARY: {
                CastOmniToShuffleType(OMNI_VARBINARY, SHUFFLE_BINARY);
                break;
            }
            case OMNI_CHAR: {
                CastOmniToShuffleType(OMNI_CHAR, SHUFFLE_BINARY);
                break;
            }
            case OMNI_VARCHAR: {
                CastOmniToShuffleType(OMNI_VARCHAR, SHUFFLE_BINARY);
                break;
            }
            case OMNI_ARRAY: {
                CastOmniToShuffleType(OMNI_ARRAY, SHUFFLE_ARRAY);
                break;
            }
            case OMNI_MAP: {
                CastOmniToShuffleType(OMNI_MAP, SHUFFLE_MAP);
                break;
            }
            case OMNI_ROW: {
                CastOmniToShuffleType(OMNI_ROW, SHUFFLE_ROW);
                break;
            }
            default: throw std::runtime_error("Unsupported DataTypeId: " + input_col_types.inputVecTypeIds[i]);
        }
    }
}

void Splitter::CastOmniToShuffleType(DataTypeId omniType, ShuffleTypeId shuffleType)
{
    column_type_id_.push_back(shuffleType);
}

int Splitter::Split_Init(){
    num_row_splited_ = 0;
    cached_vectorbatch_size_ = 0;

    partition_id_cnt_cur_ = new int32_t[num_partitions_]();
    partition_id_cnt_cache_ = new uint64_t[num_partitions_]();
    partition_buffer_size_ = new int32_t[num_partitions_]();
    partition_buffer_idx_base_ = new int32_t[num_partitions_]();
    partition_buffer_idx_offset_ = new int32_t[num_partitions_]();
    partition_serialization_size_ = new uint32_t[num_partitions_]();

    fixed_width_array_idx_.clear();
    complex_type_array_idx_.clear();
    partition_lengths_.resize(num_partitions_);

    fixed_valueBuffer_size_ = new uint32_t[num_partitions_]();
    fixed_nullBuffer_size_ = new uint32_t[num_partitions_]();

    // obtain configed dir from Environment Variables
    configured_dirs_ = GetConfiguredLocalDirs();
    sub_dir_selection_.assign(configured_dirs_.size(), 0);

    // Both data_file and shuffle_index_file should be set through jni.
    // For test purpose, Create a temporary subdirectory in the system temporary
    // dir with prefix "columnar-shuffle"
    if (options_.data_file.length() == 0 && !options_.rss_mode) {
        options_.data_file = CreateTempShuffleFile(configured_dirs_[0]);
    }

    fixed_value_width_bytes_ = 0;
    binary_value_column_idx_.clear();
    for (uint i = 0; i < column_type_id_.size(); ++i) {
        switch (column_type_id_[i]) {
            case ShuffleTypeId::SHUFFLE_1BYTE:
            case ShuffleTypeId::SHUFFLE_2BYTE:
            case ShuffleTypeId::SHUFFLE_4BYTE:
            case ShuffleTypeId::SHUFFLE_8BYTE:
            case ShuffleTypeId::SHUFFLE_DECIMAL128:
                fixed_value_width_bytes_ += uint64_t{1} << column_type_id_[i];
                if (singlePartitionFlag) {
                    fixed_width_array_idx_.push_back(i);
                } else {
                    fixed_width_array_idx_.push_back(i + 1);
                }
                break;
            case ShuffleTypeId::SHUFFLE_BINARY:
                binary_value_column_idx_.push_back(static_cast<int32_t>(i));
                break;
            case ShuffleTypeId::SHUFFLE_ARRAY:
            case ShuffleTypeId::SHUFFLE_MAP:
            case ShuffleTypeId::SHUFFLE_ROW:
               if (singlePartitionFlag) {
                   complex_type_array_idx_.push_back(i);
               } else {
                   complex_type_array_idx_.push_back(i + 1);
               }
               break;
            default:
                break;
        }
    }

    // Velox (VeloxHashShuffleWriter::calculatePartitionBufferSize) gives preallocated partition
    // buffers at most a quarter of the task's offheap share. A full buffer_size per partition
    // outgrows that once partitions are many: 600 partitions x 8192 rows x 37 bytes is 182 MB
    // against a 200 MB share. Those buffers are mostly empty when Spark spills the task, so a
    // spill frees capacity rather than rows, the next batch allocates it again, and the writer is
    // spilled every few batches with a handful of rows per partition. Buffers therefore start
    // small and grow with the rows, so they hold rows rather than empty capacity. This is not
    // limited to a small share: with a normal share the same empty capacity is what gets spilled
    // whenever a sibling operator holds most of the task (q3 SF1000 st16: a pinned final
    // aggregation, 4300 writer spills of 93 MB capacity per task for 2 MB of rows each).
    const uint64_t bytesPerRow = fixed_value_width_bytes_ + fixed_width_array_idx_.size(); // values + validity
    growPartitionBuffers_ = options_.task_offheap_bytes > 0 && bytesPerRow > 0;
    if (growPartitionBuffers_) {
        LogsInfo("Splitter partition buffers grow with their rows: partitions=%d buffer_size=%d bytes_per_row=%lu "
                 "task_offheap=%ld", num_partitions_, options_.buffer_size, bytesPerRow,
                 static_cast<long>(options_.task_offheap_bytes));
    }
    auto num_fixed_width = fixed_width_array_idx_.size();
    fixed_column_has_null_scratch_.assign(num_fixed_width, 0);
    partition_fixed_width_validity_addrs_.resize(num_fixed_width);
    partition_fixed_width_value_addrs_.resize(num_fixed_width);
    partition_fixed_width_buffers_.resize(num_fixed_width);
    partition_fixed_width_arrow_buffers_.resize(num_fixed_width);
    for (uint i = 0; i < num_fixed_width; ++i) {
        partition_fixed_width_validity_addrs_[i].resize(num_partitions_);
        partition_fixed_width_value_addrs_[i].resize(num_partitions_);
        partition_fixed_width_buffers_[i].resize(num_partitions_);
        partition_fixed_width_arrow_buffers_[i].resize(num_partitions_);
    }

    /* init varchar partition */
    vc_partition_array_buffers_.resize(num_partitions_);
    for (auto i = 0; i < num_partitions_; ++i) {
        vc_partition_array_buffers_[i].resize(column_type_id_.size());
    }

    /* init complex type accumulator (方案 C) — 懒初始化，在 SplitComplexColumns 首次调用时进行，
       因为 inputDataTypes_ 在 Split_Init 时可能尚未设置（测试通过 SetInputDataTypes 后置设置）*/
    partition_complex_accumulators_.resize(num_partitions_);
    for (auto i = 0; i < num_partitions_; ++i) {
        partition_complex_accumulators_[i].resize(complex_type_array_idx_.size());
    }

    // partition_arena_ is std::vector<SimpleArenaAllocator>, which has no default
    // ctor or late-bound allocator, so each element is built with
    // `reserve + emplace_back(minChunkSize, allocator)` and its pool-bound Allocator*.
    // The production gluten path always supplies a pool; test/legacy callers without
    // a pool fall back to a function-local nullBridge Allocator.
    {
        omniruntime::mem::Allocator *boundAlloc = nullptr;
        if (taskMemoryPool_ != nullptr) {
            boundAlloc = taskMemoryPool_->getBoundAllocator();
        } else {
            // Test/legacy path without a pool: a function-local nullBridge keeps
            // those callers working without per-task accounting.
            static omniruntime::mem::Allocator nullBridge{nullptr};
            boundAlloc = &nullBridge;
        }
        partition_arena_.reserve(num_partitions_);
        for (int32_t pid = 0; pid < num_partitions_; ++pid) {
            partition_arena_.emplace_back(/*minChunkSize=*/4096, boundAlloc);
        }
    }
    partition_row_batch.resize(num_partitions_);
    partition_row_batch_count.resize(num_partitions_);
    std::fill(partition_row_batch_count.begin(), partition_row_batch_count.end(), 0);
    partition_rows.resize(num_partitions_);
    return 0;
}

int Splitter::Split(VectorBatch& vb )
{
    std::lock_guard<std::recursive_mutex> guard(reclaimMutex_);
    columnarMode_ = true;
    LogsTrace(" split vb row number: %d ", vb.GetRowCount());
    EnsureInputDataTypes(vb);
    // Compute routing once into the split scratch. Reservation estimation below
    // consumes these counts; DoSplit reuses the same IDs instead of rescanning
    // the PID vector.
    TIME_NANO_OR_RAISE(total_compute_pid_time_, ComputeAndCountPartitionId(vb, false));
    ChargeMemorySpills();
    ReserveForSplit(vb, "Splitter::Split");
    for (int32_t pid = 0; pid < num_partitions_; ++pid) {
        partition_id_cnt_cache_[pid] += static_cast<uint64_t>(partition_id_cnt_cur_[pid]);
    }
    try {
        NonReclaimableSection nonReclaimable(this);
        DoSplit(vb);
    } catch (...) {
        if (taskMemoryPool_ != nullptr) {
            taskMemoryPool_->releaseUnusedReservation();
        }
        // DoSplit may already have cached this batch's string vectors; the
        // splitter owns those now. Release the rest here so the JNI error path
        // does not free the cached vectors a second time.
        if (GetInputVecBatch() == &vb) {
            ReleaseVectorBatch(&vb);
            ResetInputVecBatch();
        }
        throw;
    }
    return 0;
}

void Splitter::EnsureInputDataTypes(VectorBatch& vb)
{
    if (!inputDataTypes_.empty()) {
        return;
    }

    const int32_t firstDataColumn = singlePartitionFlag ? 0 : 1;
    const int32_t vectorCount = static_cast<int32_t>(vb.GetVectorCount());
    if (vectorCount != num_fields_ + firstDataColumn) {
        throw std::runtime_error(
            "Shuffle input vector count does not match the configured schema");
    }

    inputDataTypes_.reserve(static_cast<size_t>(num_fields_));
    for (int32_t column = firstDataColumn; column < vectorCount; ++column) {
        inputDataTypes_.push_back(VectorHelper::GetDataType(vb.Get(column)));
    }
}

int64_t Splitter::EstimateSplitReservation(VectorBatch& vb)
{
    const auto add = [](uint64_t left, uint64_t right) {
        constexpr uint64_t max = static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
        return left > max - std::min(right, max) ? max : left + right;
    };
    const auto multiply = [](uint64_t left, uint64_t right) {
        constexpr uint64_t max = static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
        return right != 0 && left > max / right ? max : left * right;
    };

    if (!columnarMode_) {
        // Row shuffle never allocates columnar capacity buffers; it needs the
        // serialized bytes each partition will receive.
        std::vector<uint64_t> partitionBytes(num_partitions_, 0);
        if (row_estimate_max_avg_bytes_ > 0) {
            // Like Velox ensureInputFits(), size from an estimate rather than
            // serializing the batch twice: rows per partition times the
            // largest per-batch average row length seen so far. The actual
            // pool-backed arena growth stays authoritative if a batch runs
            // larger than the estimate.
            if (singlePartitionFlag) {
                partitionBytes[0] = multiply(static_cast<uint64_t>(vb.GetRowCount()),
                    row_estimate_max_avg_bytes_);
            } else {
                for (int pid = 0; pid < num_partitions_; ++pid) {
                    partitionBytes[pid] = multiply(static_cast<uint64_t>(partition_id_cnt_cur_[pid]),
                        row_estimate_max_avg_bytes_);
                }
            }
        } else {
            // First batch: no history yet, so measure the serialized rows with
            // the same encoders as SplitByRow (dictionary/constant and
            // variable-width values included).
            const int firstColumn = singlePartitionFlag ? 0 : 1;
            std::vector<DataTypeId> types;
            std::vector<Encoding> encodings;
            for (int col = firstColumn; col < vb.GetVectorCount(); ++col) {
                types.push_back(vb.Get(col)->GetTypeId());
                encodings.push_back(vb.Get(col)->GetEncoding());
            }
            RowBuffer rowBuffer(types, encodings);
            for (int row = 0; row < vb.GetRowCount(); ++row) {
                rowBuffer.TransValue(vb.GetVectors() + firstColumn, row);
                const int32_t length = rowBuffer.CalculateCompactLength(types.size());
                if (length < 0) {
                    throw std::overflow_error("Serialized shuffle row length overflow");
                }
                const int pid = singlePartitionFlag ? 0 : partition_id_[row];
                partitionBytes[pid] = add(partitionBytes[pid], static_cast<uint64_t>(length));
            }
        }
        uint64_t growth = 0;
        for (int pid = 0; pid < num_partitions_; ++pid) {
            const uint64_t bytes = partitionBytes[pid];
            if (bytes == 0) {
                continue;
            }
            if (singlePartitionFlag) {
                growth = add(growth, bytes);
            } else if (bytes > partition_arena_[pid].AvailBytes()) {
                // Cover geometric/linear chunk rounding and tail waste. This
                // bound depends on row bytes and arena state, not buffer_size.
                growth = add(growth, add(multiply(bytes, 2),
                    partition_arena_[pid].GetNextSize(bytes)));
            }
        }
        const int64_t quantized = omniruntime::mem::OmniTaskMemoryPool::quantizedReservationSize(
            static_cast<int64_t>(growth));
        return quantized < 0 ? std::numeric_limits<int64_t>::max() : quantized;
    }

    // VectorBatch::CalculateTotalSize() is not a byte-size API: it sums
    // BaseVector::GetSize(), i.e. element counts. Using it here made a
    // 48-partition, ten-LONG batch look like a few tens of KiB even though
    // AllocatePartitionBuffers() was about to materialize ~15 MiB. Use the
    // already-computed rows per destination and mirror DoSplit's
    // buffer-growth rules so admission happens before persistent state mutates.
    //
    // The schema-derived inputs (`fixed_value_width_bytes_` and
    // `binary_value_column_idx_`) are precomputed by Split_Init(); the
    // per-partition buffer state below is still resolved for every batch.
    const uint64_t fixedValueWidth = fixed_value_width_bytes_;

    // Null presence is a per-batch property, so resolve it once per fixed-width
    // column instead of re-reading it for every partition. The no-null case
    // also skips the per-partition fixed-column walk entirely. The flag vector
    // is splitter-owned scratch (sized in Split_Init) to keep this path free of
    // per-batch heap allocation.
    std::vector<uint8_t>& fixedColumnHasNull = fixed_column_has_null_scratch_;
    bool anyFixedColumnHasNull = false;
    for (size_t fixedColumn = 0; fixedColumn < fixed_width_array_idx_.size(); ++fixedColumn) {
        const bool hasNull = vb.Get(fixed_width_array_idx_[fixedColumn])->HasNull();
        fixedColumnHasNull[fixedColumn] = hasNull ? 1 : 0;
        anyFixedColumnHasNull = anyFixedColumnHasNull || hasNull;
    }

    uint64_t fixedBufferGrowth = 0;
    for (int32_t pid = 0; pid < num_partitions_; ++pid) {
        const uint64_t count = static_cast<uint64_t>(partition_id_cnt_cur_[pid]);
        if (count == 0) {
            continue;
        }
        const bool replacesValueBuffers =
            static_cast<uint64_t>(std::max(partition_buffer_idx_base_[pid], 0)) + count >
            static_cast<uint64_t>(std::max(partition_buffer_size_[pid], 0));
        // A grown buffer holds the old one until its rows are copied, so it peaks at the new
        // capacity too.
        const uint64_t newCapacity = static_cast<uint64_t>(
            NextPartitionBufferRows(pid, static_cast<int32_t>(count)));
        if (replacesValueBuffers) {
            fixedBufferGrowth = add(
                fixedBufferGrowth, multiply(newCapacity, fixedValueWidth));
        }

        // Validity storage is allocated lazily, one byte per row. A value-buffer
        // replacement also resets its validity buffer through CacheVectorBatch().
        if (anyFixedColumnHasNull) {
            for (size_t fixedColumn = 0; fixedColumn < fixed_width_array_idx_.size(); ++fixedColumn) {
                if (fixedColumnHasNull[fixedColumn] != 0 &&
                    (replacesValueBuffers ||
                     partition_fixed_width_validity_addrs_[fixedColumn][pid] == nullptr)) {
                    fixedBufferGrowth = add(fixedBufferGrowth, newCapacity);
                }
            }
        }
    }

    // Binary payload bytes stay in the retained input vector; the new
    // pool-accounted allocation is the VCLocation index. Bound each vector's
    // geometric growth by 2x its final size, including the transient where the
    // old allocation remains live until the new allocation succeeds.
    uint64_t binaryLocationGrowth = 0;
    const uint64_t batchLimit = std::max<uint64_t>(options_.spill_batch_row_num, 1);
    for (int32_t pid = 0; pid < num_partitions_; ++pid) {
        const uint64_t count = static_cast<uint64_t>(partition_id_cnt_cur_[pid]);
        if (count == 0) {
            continue;
        }
        for (const int32_t column : binary_value_column_idx_) {
            uint64_t remaining = count;
            const auto &batches = vc_partition_array_buffers_[pid][column];
            if (!batches.empty() && batches.back().getVcList().size() < batchLimit) {
                const auto &locations = batches.back().getVcList();
                const uint64_t oldSize = locations.size();
                const uint64_t appended = std::min(remaining, batchLimit - oldSize);
                const uint64_t finalSize = add(oldSize, appended);
                if (finalSize > locations.capacity()) {
                    binaryLocationGrowth = add(binaryLocationGrowth,
                        multiply(multiply(finalSize, 2), sizeof(VCLocation)));
                }
                remaining -= appended;
            }
            while (remaining > 0) {
                const uint64_t chunk = std::min(remaining, batchLimit);
                binaryLocationGrowth = add(binaryLocationGrowth,
                    multiply(multiply(chunk, 2), sizeof(VCLocation)));
                remaining -= chunk;
            }
        }
    }

    // Input vectors and BuildPartition2Row's std::vector scratch are not new
    // task-pool allocations. Reserving for them here would manufacture an
    // 8 MiB request even when an existing fixed buffer can absorb the batch.
    const uint64_t incrementalPeak = add(fixedBufferGrowth, binaryLocationGrowth);
    // Match Velox MemoryPoolImpl::maybeReserve(): explicit pre-reservation is
    // rounded once to an 8 MiB growth quantum. The listener then moves whole
    // reservation blocks (8 MiB by default), which an 8 MiB-aligned request
    // already is, so no second quantum is added.
    const int64_t quantized = omniruntime::mem::OmniTaskMemoryPool::quantizedReservationSize(
        static_cast<int64_t>(incrementalPeak));
    return quantized < 0 ? std::numeric_limits<int64_t>::max() : quantized;
}

void Splitter::ReserveForSplit(VectorBatch& vb, const char *site)
{
    if (taskMemoryPool_ == nullptr) {
        return;
    }
    const int64_t estimate = EstimateSplitReservation(vb);
    if (estimate <= 0 || taskMemoryPool_->availableReservation() >= estimate) {
        return;
    }
    omniruntime::op::ReservationRequestSection request(
        nullptr, reclaimableName(), site, estimate);
    if (taskMemoryPool_->maybeReserve(estimate)) {
        return;
    }
    // Keep pre-reservation best effort, just like Velox ensureInputFits. The
    // actual pool-backed buffer growth below remains authoritative and can
    // still arbitrate, reclaim, or report a real allocation failure.
    if (IsOperatorVerboseEnabled()) {
        std::fprintf(stderr,
            "[OMNI-SHUFFLE] event=ensure_input_fits outcome=reservation_denied path=%s writer_pool=%p target=%ld used=%ld reserved=%ld\n",
            columnarMode_ ? "columnar" : "row",
            static_cast<void *>(taskMemoryPool_), static_cast<long>(estimate),
            static_cast<long>(taskMemoryPool_->currentBytes()),
            static_cast<long>(taskMemoryPool_->reservedBytes()));
    }
}

int Splitter::SplitByRow(VectorBatch *vecBatch) {
    std::lock_guard<std::recursive_mutex> guard(reclaimMutex_);
    columnarMode_ = false;
    EnsureInputDataTypes(*vecBatch);
    TIME_NANO_OR_RAISE(total_compute_pid_time_, ComputeAndCountPartitionId(*vecBatch, false));
    ReserveForSplit(*vecBatch, "Splitter::SplitByRow");
    NonReclaimableSection nonReclaimable(this);
    int32_t rowCount = vecBatch->GetRowCount();
    uint64_t batchRowBytes = 0;
    for (int pid = 0; pid < num_partitions_; ++pid) {
        auto needCapacity = partition_rows[pid].size() + rowCount;
        if (partition_rows[pid].capacity() < needCapacity) {
            auto prepareCapacity = partition_rows[pid].capacity() * expansion;
            auto newCapacity = prepareCapacity > needCapacity ? prepareCapacity : needCapacity;
            partition_rows[pid].reserve(newCapacity);
        }
    }

    if (singlePartitionFlag) {
        if (taskMemoryPool_ == nullptr) {
            throw std::invalid_argument("Single-partition row shuffle requires a task memory pool");
        }
        auto *poolAlloc = taskMemoryPool_->getBoundAllocator();
        // Reserve before producing raw owning rows: adoption below cannot grow
        // the vector. RowBatch only owns its pointer array, not its RowInfo rows.
        partition_rows[0].reserve(partition_rows[0].size() + rowCount);
        std::unique_ptr<RowBatch> rowBatch(
            VectorHelper::TransRowBatchFromVectorBatch(vecBatch, poolAlloc));
        for (int i = 0; i < rowCount; ++i) {
            RowInfo *rowInfo = rowBatch->Get(i);
            partition_rows[0].emplace_back(std::unique_ptr<RowInfo>(rowInfo));
            total_input_size += rowInfo->length;
            batchRowBytes += static_cast<uint64_t>(rowInfo->length);
        }
        delete vecBatch;
    } else {
        auto tmpVectorBatch = new VectorBatch(rowCount);
        BuildPartition2Row(rowCount);
        for (int i = 1; i < vecBatch->GetVectorCount(); ++i) {
            tmpVectorBatch->Append(vecBatch->Get(i));
        }
        vecBatch->ResizeVectorCount(1);
        std::vector<DataTypeId> typeIds;
        std::vector<Encoding> encodings;
        int32_t vecCount = tmpVectorBatch->GetVectorCount();
        for (int i = 0; i < vecCount; i++) {
            typeIds.push_back(tmpVectorBatch->Get(i)->GetTypeId());
            encodings.push_back(tmpVectorBatch->Get(i)->GetEncoding());
        }
        auto rowBuffer = std::make_unique<RowBuffer>(typeIds, encodings, typeIds.size() - 1);

        for (auto &pid: partition_used_) {
            auto pos = partition_row_offset_base_[pid];
            auto end = partition_row_offset_base_[pid + 1];
            for (; pos < end; ++pos) {
                rowBuffer->TransValueFromVectorBatch(tmpVectorBatch, static_cast<int32_t>(row_offset_row_id_[pos]));
                auto oneRowLen = rowBuffer->FillBuffer(partition_arena_[pid]);
                partition_rows[pid].emplace_back(std::make_unique<RowInfo>(rowBuffer->TakeRowBuffer(), oneRowLen));
                total_input_size += oneRowLen;
                batchRowBytes += static_cast<uint64_t>(oneRowLen);
            }
        }

        delete vecBatch;
        delete tmpVectorBatch;
    }
    if (rowCount > 0) {
        const uint64_t batchAvg = (batchRowBytes + rowCount - 1) / static_cast<uint64_t>(rowCount);
        row_estimate_max_avg_bytes_ = std::max(row_estimate_max_avg_bytes_, batchAvg);
    }

    // spill
    // process level: If the memory usage of the current executor exceeds the threshold, spill is triggered.
    // The legacy process-global accounted-memory query no longer exists, so the
    // executor-level trigger is effectively disabled; the task-level trigger
    // below still fires from `total_input_size`.
    uint64_t usedMemorySize = 0;
    if (usedMemorySize > options_.executor_spill_mem_threshold) {
        if (rss_mode_) {
            TIME_NANO_OR_RAISE(total_spill_time_, SpillToRssByRow());
        } else {
            TIME_NANO_OR_RAISE(total_spill_time_, SpillToTmpFileByRow());
            isSpill = true;
        }
        total_input_size = 0;
    }

    // Row payloads live in RowInfo allocations or partition arenas, not the
    // Arrow pool. Use the accumulated serialized bytes for the row threshold.
    if (total_input_size > options_.task_spill_mem_threshold) {
        if (rss_mode_) {
            TIME_NANO_OR_RAISE(total_spill_time_, SpillToRssByRow());
        } else {
            TIME_NANO_OR_RAISE(total_spill_time_, SpillToTmpFileByRow());
            isSpill = true;
        }
        total_input_size = 0;
    }
    return 0;
}

std::shared_ptr<Buffer> Splitter::CaculateSpilledTmpFilePartitionOffsets() {
    void *ptr_tmp = static_cast<void *>(options_.allocator->Alloc((num_partitions_ + 1) * sizeof(uint64_t)));
    if (nullptr == ptr_tmp) {
        LogsError("CaculateSpilledTmpFilePartitionOffsets Alloc failed: num_partitions=%d size=%lld",
                  num_partitions_, static_cast<long long>((num_partitions_ + 1) * sizeof(uint64_t)));
        throw std::runtime_error("Allocator for partitionOffsets Failed! ");
    }
    std::shared_ptr<Buffer> ptrPartitionOffsets (new Buffer((uint8_t*)ptr_tmp, 0, (num_partitions_ + 1) * sizeof(uint64_t), options_.allocator));
    // 每批自带 [4B大端size][文件头][batch帧]，无独立文件头，偏移从 0 开始
    uint64_t pidOffset = 0;

    auto pid = 0;
    for (pid = 0; pid < num_partitions_; ++pid) {
        reinterpret_cast<uint64_t *>(ptrPartitionOffsets->data_)[pid] = pidOffset;
        pidOffset += partition_serialization_size_[pid];
        // reset partition_cached_vectorbatch_size_ to 0
        partition_serialization_size_[pid] = 0;
    }
    reinterpret_cast<uint64_t *>(ptrPartitionOffsets->data_)[pid] = pidOffset;
    return ptrPartitionOffsets;
}

int Splitter::WriteDataFileArrow(arrow::MemoryPool *reclaimCachePool) {
    std::unique_ptr<OutputStream> outStream = writeLocalFile(options_.next_spilled_file_dir + ".data");

    // Spill files use the final shuffle codec. Each partition ends on a block boundary
    // (ArrowWriteColumnarPartition flushes), so MergeSpilled appends the compressed segments
    // to the final file as they are, instead of reading raw bytes back and compressing them
    // then. partition_serialization_size_ and total_bytes_spilled_ are on-disk (compressed) bytes.
    ArrowFileHeader header;
    header.version = kArrowShuffleVersion;
    header.layout = ShuffleLayout::COLUMNAR;
    if (inputDataTypes_.size() != static_cast<size_t>(num_fields_)) {
        LogsError("Splitter header build: inputDataTypes_ size mismatch: types=%zu num_fields=%d",
                  inputDataTypes_.size(), num_fields_);
        throw std::runtime_error("Splitter: inputDataTypes_ not set before building Arrow header");
    }
    for (int i = 0; i < num_fields_; ++i) {
        header.schema.push_back(DataTypeToDescriptor(inputDataTypes_[i]));
    }

    auto arrowOut = ArrowOutputStream::Make(
        std::move(outStream),
        options_.compression_type,
        spark::CompressionStrategy_COMPRESSION,
        static_cast<uint64_t>(options_.buffer_size),
        options_.compress_block_size,
        *spark::getDefaultPool());

    // 顺序写入每个partition（每批自带 [4B大端size][文件头][batch帧]，
    // headerAlreadyWritten 参数不再控制文件头写出——serializer 内部每批都写文件头）
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        if (reclaimCachePool != nullptr) {
            CacheVectorBatch(pid, true, reclaimCachePool);
            partition_buffer_size_[pid] = 0; // 溢写之后将其清零，条件溢写需要重新分配内存
        }
        auto written = ArrowWriteColumnarPartition(
            pid, *arrowOut, header, partition_arrow_batch_,
            /*headerAlreadyWritten=*/true);
        total_bytes_spilled_ += written;
        partition_serialization_size_[pid] = written;
        if (reclaimCachePool != nullptr) {
            // Release the partition's buffers before building the next one.
            partition_arrow_batch_[pid].clear();
        }
    }
    auto closeSt = arrowOut->Close();
    if (!closeSt.ok()) {
        LogsError("WriteDataFileArrow Close failed: msg=%s", closeSt.ToString().c_str());
        throw std::runtime_error("WriteDataFileArrow close failed: " + closeSt.ToString());
    }
    memset(partition_id_cnt_cache_, 0, num_partitions_ * sizeof(uint64_t));
    return 0;
}

int Splitter::WriteDataFileArrowByRow() {
    std::unique_ptr<OutputStream> outStream = writeLocalFile(options_.next_spilled_file_dir + ".data");

    // Spill files use the final shuffle codec; see WriteDataFileArrow.
    ArrowFileHeader header;
    header.version = kArrowShuffleVersion;
    header.layout = ShuffleLayout::ROW;
    if (inputDataTypes_.size() != static_cast<size_t>(num_fields_)) {
        LogsError("Splitter header build: inputDataTypes_ size mismatch: types=%zu num_fields=%d",
                  inputDataTypes_.size(), num_fields_);
        throw std::runtime_error("Splitter: inputDataTypes_ not set before building Arrow header");
    }
    for (int i = 0; i < num_fields_; ++i) {
        header.schema.push_back(DataTypeToDescriptor(inputDataTypes_[i]));
    }

    auto arrowOut = ArrowOutputStream::Make(
        std::move(outStream),
        options_.compression_type,
        spark::CompressionStrategy_COMPRESSION,
        static_cast<uint64_t>(options_.buffer_size),
        options_.compress_block_size,
        *spark::getDefaultPool());

    // 每批自带 [4B大端size][文件头][row batch帧]，
    // serializer 内部每批都写文件头，无需单独写。
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        auto written = ArrowWriteRowPartition(
            pid, *arrowOut, header, partition_rows,
            options_.spill_batch_row_num, *arrow_pool_,
            /*headerAlreadyWritten=*/true);
        total_bytes_spilled_ += written;
        partition_serialization_size_[pid] = written;
        // 清理该分区的行数据和 arena
        partition_arena_[pid].Reset();
        partition_rows[pid].clear();
    }
    auto closeSt = arrowOut->Close();
    if (!closeSt.ok()) {
        LogsError("WriteDataFileArrowByRow Close failed: msg=%s", closeSt.ToString().c_str());
        throw std::runtime_error("WriteDataFileArrowByRow close failed: " + closeSt.ToString());
    }
    return 0;
}

// Append one partition's segment of a spill file to the final stream. The segment is a run of
// complete compressed blocks in the final file's own format (the spill writer uses the same codec
// and flushes per partition), so it is written through without decompressing or recompressing.
// Returns the bytes added to the final file: the flush that closed the stream's pending block plus
// the segment itself.
uint64_t Splitter::TransferSpilledSegments(ArrowOutputStream& out,
                                           const std::string& tmpDataFilePath,
                                           uint64_t partitionOffset,
                                           uint64_t partitionSize) {
    if (partitionSize == 0) {
        return 0;
    }
    auto mmapResult = arrow::io::MemoryMappedFile::Open(tmpDataFilePath, arrow::io::FileMode::READ);
    if (!mmapResult.ok()) {
        LogsError("TransferSpilledSegments mmap failed: path=%s offset=%llu size=%llu msg=%s",
                  tmpDataFilePath.c_str(),
                  static_cast<unsigned long long>(partitionOffset),
                  static_cast<unsigned long long>(partitionSize),
                  mmapResult.status().ToString().c_str());
        throw std::runtime_error("TransferSpilledSegments: Failed to mmap " +
                                 tmpDataFilePath + ": " + mmapResult.status().ToString());
    }
    auto mmapFile = std::move(mmapResult).ValueOrDie();
    // 用 ReadAt 读取 mmap 段（Arrow 11 兼容；Arrow 12+ 可改用 data() 直读零拷贝）
    auto readResult = mmapFile->ReadAt(static_cast<int64_t>(partitionOffset),
                                       static_cast<int64_t>(partitionSize));
    if (!readResult.ok()) {
        LogsError("TransferSpilledSegments ReadAt failed: path=%s offset=%llu size=%llu msg=%s",
                  tmpDataFilePath.c_str(),
                  static_cast<unsigned long long>(partitionOffset),
                  static_cast<unsigned long long>(partitionSize),
                  readResult.status().ToString().c_str());
        throw std::runtime_error("TransferSpilledSegments: ReadAt failed: " +
                                 readResult.status().ToString());
    }
    auto buffer = std::move(readResult).ValueOrDie();
    const uint64_t flushed = out.WriteThrough(buffer->data(), static_cast<int64_t>(partitionSize));
    auto closeSt = mmapFile->Close();
    if (!closeSt.ok()) {
        LogsError("TransferSpilledSegments Close mmap failed: path=%s offset=%llu size=%llu msg=%s",
                  tmpDataFilePath.c_str(),
                  static_cast<unsigned long long>(partitionOffset),
                  static_cast<unsigned long long>(partitionSize),
                  closeSt.ToString().c_str());
        throw std::runtime_error("TransferSpilledSegments: Close mmap failed: " + closeSt.ToString());
    }
    return flushed + partitionSize;
}

void Splitter::MergeSpilled() {
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        CacheVectorBatch(pid, true);
        partition_buffer_size_[pid] = 0; // 溢写之后将其清零，条件溢写需要重新分配内存
    }

    std::unique_ptr<OutputStream> outStream = writeLocalFile(options_.data_file);

    // 构建 Arrow 文件头
    ArrowFileHeader header;
    header.version = kArrowShuffleVersion;
    header.layout = ShuffleLayout::COLUMNAR;
    if (inputDataTypes_.size() != static_cast<size_t>(num_fields_)) {
        LogsError("Splitter header build: inputDataTypes_ size mismatch: types=%zu num_fields=%d",
                  inputDataTypes_.size(), num_fields_);
        throw std::runtime_error("Splitter: inputDataTypes_ not set before building Arrow header");
    }
    for (int i = 0; i < num_fields_; ++i) {
        header.schema.push_back(DataTypeToDescriptor(inputDataTypes_[i]));
    }

    auto arrowOut = ArrowOutputStream::Make(
        std::move(outStream),
        options_.compression_type,
        spark::CompressionStrategy_COMPRESSION,
        static_cast<uint64_t>(options_.buffer_size),
        options_.compress_block_size,
        *spark::getDefaultPool());

    for (int pid = 0; pid < num_partitions_; pid++) {
        // 写出内存中该分区的 Arrow 缓存批
        auto written = ArrowWriteColumnarPartition(
            pid, *arrowOut, header, partition_arrow_batch_,
            /*headerAlreadyWritten=*/(pid != 0));
        total_bytes_written_ += written;
        partition_lengths_[pid] += written;
        LogsDebug(" MergeSpilled traversal partition( %d ) written: %d", pid, written);

        // 追加该分区各溢写临时文件的对应段（mmap 零拷贝透传，消除 C8）
        for (auto &pair : spilled_tmp_files_info_) {
            auto tmpDataFilePath = pair.first + ".data";
            auto tmpPartitionOffset = reinterpret_cast<uint64_t *>(pair.second->data_)[pid];
            auto tmpPartitionSize = reinterpret_cast<uint64_t *>(pair.second->data_)[pid + 1]
                                    - reinterpret_cast<uint64_t *>(pair.second->data_)[pid];
            LogsDebug(" TransferSpilledSegments pid=%d offset=%d size=%d path=%s",
                      pid, tmpPartitionOffset, tmpPartitionSize, tmpDataFilePath.c_str());
            const uint64_t added = TransferSpilledSegments(*arrowOut, tmpDataFilePath, tmpPartitionOffset,
                tmpPartitionSize);
            partition_lengths_[pid] += static_cast<int64_t>(added);
            total_bytes_written_ += static_cast<int64_t>(added);
        }
        // Memory spills evicted to disk, then the ones still in memory (always
        // the newest): compressed segments in this stream's format, passed through.
        for (auto &file : evicted_spill_files_) {
            const uint64_t added = TransferSpilledSegments(*arrowOut, file.path + ".data", file.offsets[pid],
                file.offsets[pid + 1] - file.offsets[pid]);
            partition_lengths_[pid] += static_cast<int64_t>(added);
            total_bytes_written_ += static_cast<int64_t>(added);
        }
        for (auto &segment : memory_spills_) {
            const uint64_t size = segment.offsets[pid + 1] - segment.offsets[pid];
            if (size == 0) {
                continue;
            }
            const uint64_t flushed = arrowOut->WriteThrough(segment.data.data() + segment.offsets[pid],
                static_cast<int64_t>(size));
            partition_lengths_[pid] += static_cast<int64_t>(flushed + size);
            total_bytes_written_ += static_cast<int64_t>(flushed + size);
        }
    }

    auto mergeCloseSt = arrowOut->Close();
    if (!mergeCloseSt.ok()) {
        LogsError("MergeSpilled Close failed: msg=%s", mergeCloseSt.ToString().c_str());
        throw std::runtime_error("MergeSpilled close failed: " + mergeCloseSt.ToString());
    }

    memset(partition_id_cnt_cache_, 0, num_partitions_ * sizeof(uint64_t));
    ReleaseVarcharVector();
    num_row_splited_ = 0;
    cached_vectorbatch_size_ = 0;
    memory_spills_.clear();
    UnchargeMemorySpills();
}

void Splitter::MergeSpilledByRow() {
    std::unique_ptr<OutputStream> outStream = writeLocalFile(options_.data_file);

    // 构建 Arrow 文件头
    ArrowFileHeader header;
    header.version = kArrowShuffleVersion;
    header.layout = ShuffleLayout::ROW;
    if (inputDataTypes_.size() != static_cast<size_t>(num_fields_)) {
        LogsError("Splitter header build: inputDataTypes_ size mismatch: types=%zu num_fields=%d",
                  inputDataTypes_.size(), num_fields_);
        throw std::runtime_error("Splitter: inputDataTypes_ not set before building Arrow header");
    }
    for (int i = 0; i < num_fields_; ++i) {
        header.schema.push_back(DataTypeToDescriptor(inputDataTypes_[i]));
    }

    auto arrowOut = ArrowOutputStream::Make(
        std::move(outStream),
        options_.compression_type,
        spark::CompressionStrategy_COMPRESSION,
        static_cast<uint64_t>(options_.buffer_size),
        options_.compress_block_size,
        *spark::getDefaultPool());

    for (int pid = 0; pid < num_partitions_; pid++) {
        // 写出内存中该分区的行式数据
        auto written = ArrowWriteRowPartition(
            pid, *arrowOut, header, partition_rows,
            options_.spill_batch_row_num, *arrow_pool_,
            /*headerAlreadyWritten=*/(pid != 0));
        total_bytes_written_ += written;
        partition_lengths_[pid] += written;
        // 清理该分区的行数据和 arena
        partition_arena_[pid].Reset();
        partition_rows[pid].clear();
        LogsDebug(" MergeSpilled traversal partition( %d ) written: %d", pid, written);

        // 追加该分区各溢写临时文件的对应段（mmap 零拷贝透传，消除 C8）
        for (auto &pair : spilled_tmp_files_info_) {
            auto tmpDataFilePath = pair.first + ".data";
            auto tmpPartitionOffset = reinterpret_cast<uint64_t *>(pair.second->data_)[pid];
            auto tmpPartitionSize = reinterpret_cast<uint64_t *>(pair.second->data_)[pid + 1]
                                    - reinterpret_cast<uint64_t *>(pair.second->data_)[pid];
            LogsDebug(" TransferSpilledSegments pid=%d offset=%d size=%d path=%s",
                      pid, tmpPartitionOffset, tmpPartitionSize, tmpDataFilePath.c_str());
            const uint64_t added = TransferSpilledSegments(*arrowOut, tmpDataFilePath, tmpPartitionOffset,
                tmpPartitionSize);
            partition_lengths_[pid] += static_cast<int64_t>(added);
            total_bytes_written_ += static_cast<int64_t>(added);
        }
    }

    auto mergeRowCloseSt = arrowOut->Close();
    if (!mergeRowCloseSt.ok()) {
        LogsError("MergeSpilledByRow Close failed: msg=%s", mergeRowCloseSt.ToString().c_str());
        throw std::runtime_error("MergeSpilledByRow close failed: " + mergeRowCloseSt.ToString());
    }
}

void Splitter::WriteSplit() {
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        CacheVectorBatch(pid, true);
        partition_buffer_size_[pid] = 0; // 溢写之后将其清零，条件溢写需要重新分配内存
    }

    // 构建 Arrow 文件头：schema 由 inputDataTypes_（递归 DataType 树）通过 DataTypeToDescriptor 逐列构建
    ArrowFileHeader header;
    header.version = kArrowShuffleVersion;
    header.layout = ShuffleLayout::COLUMNAR;
    if (inputDataTypes_.size() != static_cast<size_t>(num_fields_)) {
        LogsError("Splitter header build: inputDataTypes_ size mismatch: types=%zu num_fields=%d",
                  inputDataTypes_.size(), num_fields_);
        throw std::runtime_error("Splitter: inputDataTypes_ not set before building Arrow header");
    }
    for (int i = 0; i < num_fields_; ++i) {
        header.schema.push_back(DataTypeToDescriptor(inputDataTypes_[i]));
    }

    std::unique_ptr<OutputStream> outStream = writeLocalFile(options_.data_file);
    auto arrowOut = ArrowOutputStream::Make(
        std::move(outStream),
        options_.compression_type,
        spark::CompressionStrategy_COMPRESSION,
        static_cast<uint64_t>(options_.buffer_size),
        options_.compress_block_size,
        *spark::getDefaultPool());

    for (auto pid = 0; pid < num_partitions_; ++pid) {
        auto written = ArrowWriteColumnarPartition(
            pid, *arrowOut, header, partition_arrow_batch_,
            /*headerAlreadyWritten=*/(pid != 0));
        total_bytes_written_ += written;
        partition_lengths_[pid] += written;
    }

    auto writeSplitCloseSt = arrowOut->Close();
    if (!writeSplitCloseSt.ok()) {
        LogsError("WriteSplit Close failed: msg=%s", writeSplitCloseSt.ToString().c_str());
        throw std::runtime_error("WriteSplit close failed: " + writeSplitCloseSt.ToString());
    }
    memset(partition_id_cnt_cache_, 0, num_partitions_ * sizeof(uint64_t));
    ReleaseVarcharVector();
    num_row_splited_ = 0;
    cached_vectorbatch_size_ = 0;
}

void Splitter::WriteSplitByRow() {
    std::unique_ptr<OutputStream> outStream = writeLocalFile(options_.data_file);

    // 构建 Arrow 文件头：layout=ROW
    ArrowFileHeader header;
    header.version = kArrowShuffleVersion;
    header.layout = ShuffleLayout::ROW;
    if (inputDataTypes_.size() != static_cast<size_t>(num_fields_)) {
        LogsError("Splitter header build: inputDataTypes_ size mismatch: types=%zu num_fields=%d",
                  inputDataTypes_.size(), num_fields_);
        throw std::runtime_error("Splitter: inputDataTypes_ not set before building Arrow header");
    }
    for (int i = 0; i < num_fields_; ++i) {
        header.schema.push_back(DataTypeToDescriptor(inputDataTypes_[i]));
    }

    auto arrowOut = ArrowOutputStream::Make(
        std::move(outStream),
        options_.compression_type,
        spark::CompressionStrategy_COMPRESSION,
        static_cast<uint64_t>(options_.buffer_size),
        options_.compress_block_size,
        *spark::getDefaultPool());

    for (auto pid = 0; pid < num_partitions_; ++pid) {
        auto written = ArrowWriteRowPartition(
            pid, *arrowOut, header, partition_rows,
            options_.spill_batch_row_num, *arrow_pool_,
            /*headerAlreadyWritten=*/(pid != 0));
        total_bytes_written_ += written;
        partition_lengths_[pid] += written;
        // 清理该分区的行数据和 arena
        partition_arena_[pid].Reset();
        partition_rows[pid].clear();
    }

    auto writeRowCloseSt = arrowOut->Close();
    if (!writeRowCloseSt.ok()) {
        LogsError("WriteSplitByRow Close failed: msg=%s", writeRowCloseSt.ToString().c_str());
        throw std::runtime_error("WriteSplitByRow close failed: " + writeRowCloseSt.ToString());
    }
}

int Splitter::DeleteSpilledTmpFile() {
    for (auto &pair : spilled_tmp_files_info_) {
        auto tmpDataFilePath = pair.first + ".data";
        // 释放存储有各个临时文件的偏移数据内存
        options_.allocator->Free(pair.second->data_, pair.second->capacity_);
        pair.second->SetReleaseFlag();
        if (IsFileExist(tmpDataFilePath)) {
            remove(tmpDataFilePath.c_str());
        }
    }
    // 释放内存空间，Reset spilled_tmp_files_info_, 这个地方是否有内存泄漏的风险？？？
    spilled_tmp_files_info_.clear();
    for (auto &file : evicted_spill_files_) {
        auto dataFilePath = file.path + ".data";
        if (IsFileExist(dataFilePath)) {
            remove(dataFilePath.c_str());
        }
    }
    evicted_spill_files_.clear();
    return 0;
}

int Splitter::SpillToTmpFile(bool inReclaim) {
    if (!inReclaim) {
        for (auto pid = 0; pid < num_partitions_; ++pid) {
            CacheVectorBatch(pid, true);
            partition_buffer_size_[pid] = 0; // 溢写之后将其清零，条件溢写需要重新分配内存
        }
    }

    options_.next_spilled_file_dir = CreateTempShuffleFile(NextSpilledFileDir());
    // Inside reclaim the task pool has no headroom left: build each partition's buffers outside
    // it, right before that partition is written.
    WriteDataFileArrow(inReclaim ? reclaim_spill_pool_.get() : nullptr);
    std::shared_ptr<Buffer> ptrTmp = CaculateSpilledTmpFilePartitionOffsets();
    spilled_tmp_files_info_[options_.next_spilled_file_dir] = ptrTmp;
    ReleaseVarcharVector();
    num_row_splited_ = 0;
    cached_vectorbatch_size_ = 0;
    // 清除已溢写到临时文件的缓存批，防止 MergeSpilled 时重复写出
    for (auto &batches : partition_arrow_batch_) {
        batches.clear();
    }
    return 0;}

// Testing helper: force spill + set isSpill so Stop() → MergeSpilled()
void Splitter::TestForceSpill() {
    SpillToTmpFile();
    isSpill = true;
}

uint64_t Splitter::reclaimableBytes() const
{
    // Spark arbitration can arrive on another thread while Split holds this
    // mutex across a reservation callback. Never wait in that lock order.
    std::unique_lock<std::recursive_mutex> guard(reclaimMutex_, std::try_to_lock);
    if (!guard.owns_lock() || isInNonReclaimableSection()) {
        return 0;
    }

    const auto addReclaimable = [](uint64_t current, uint64_t value) {
        return value > std::numeric_limits<uint64_t>::max() - current
            ? std::numeric_limits<uint64_t>::max()
            : current + value;
    };

    if (!columnarMode_) {
        uint64_t bytes = 0;
        // Single-partition rows own individual allocations rather than arena
        // pages. Spill destroys those RowInfo objects and returns their bytes.
        if (singlePartitionFlag) {
            for (const auto &row : partition_rows[0]) {
                if (row != nullptr && row->length > 0) {
                    bytes = addReclaimable(bytes, static_cast<uint64_t>(row->length));
                }
            }
        }
        for (const auto &arena : partition_arena_) {
            bytes = addReclaimable(bytes, arena.ReclaimableBytesOnReset());
        }
        return bytes;
    }
    if (num_row_splited_ == 0) {
        return static_cast<uint64_t>(std::max<int64_t>(0, memory_spill_bytes_));
    }

    // Count pool-backed allocations which SpillToTmpFile actually destroys.
    // The 2026_930 shuffle path replaced partition_cached_vectorbatch_ with
    // ArrowColumnarCachedBatch plus live Arrow partition/accumulator buffers.
    // OmniMemoryPoolAdapter is the single owner-side ledger for all of those
    // Arrow allocations, so using it here covers both cached and not-yet-cached
    // data without double-counting shared_ptr aliases. cached_vectorbatch_size_
    // and total_input_size remain logical serialized sizes and are not suitable
    // reclaim estimates.
    uint64_t bytes = 0;
    if (arrow_pool_ != nullptr) {
        bytes = static_cast<uint64_t>(std::max<int64_t>(0, arrow_pool_->bytes_allocated()));
    }
    for (const auto &column : partition_fixed_width_buffers_) {
        for (const auto &partition : column) {
            for (const auto &buffer : partition) {
                if (buffer != nullptr) {
                    bytes = addReclaimable(bytes, static_cast<uint64_t>(buffer->capacity_));
                }
            }
        }
    }
    for (const auto &partition : vc_partition_array_buffers_) {
        for (const auto &column : partition) {
            for (const auto &batch : column) {
                const uint64_t capacity = batch.getVcList().capacity();
                const uint64_t vcBytes = capacity > std::numeric_limits<uint64_t>::max() / sizeof(VCLocation)
                    ? std::numeric_limits<uint64_t>::max()
                    : capacity * sizeof(VCLocation);
                bytes = addReclaimable(bytes, vcBytes);
            }
        }
    }
    for (auto *vector : varcharVectorCache) {
        // The cache owns flat, dictionary and constant string vectors. Only a
        // flat vector exposes an unambiguous, exclusively-owned content buffer;
        // dictionary payload can be shared and constants may reference storage
        // outside the vector. Keep those encodings conservative instead of
        // type-punning them as flat vectors (which was both inaccurate and UB).
        if (vector->GetEncoding() == OMNI_FLAT) {
            auto *varcharVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
            bytes = addReclaimable(bytes, UnsafeStringVector::GetCapacityInBytes(varcharVector));
        }
    }
    // Memory spills are written to disk by a reclaim the raw buffers cannot serve.
    bytes = addReclaimable(bytes, static_cast<uint64_t>(std::max<int64_t>(0, memory_spill_bytes_)));
    return bytes;
}

int64_t Splitter::reclaim(int64_t targetBytes)
{
    if (targetBytes <= 0 || taskMemoryPool_ == nullptr || isInNonReclaimableSection()) {
        return 0;
    }
    if (reclaimInProgress_.test_and_set(std::memory_order_acquire)) {
        return 0;
    }
    struct FlagReset {
        std::atomic_flag &flag;
        ~FlagReset() { flag.clear(std::memory_order_release); }
    } reset{reclaimInProgress_};

    std::unique_lock<std::recursive_mutex> guard(reclaimMutex_, std::try_to_lock);
    if (!guard.owns_lock() || reclaimableBytes() == 0) {
        return 0;
    }

    const int64_t before = taskMemoryPool_->currentBytes();
    const uint64_t estimated = reclaimableBytes();
    if (rss_mode_) {
        if (columnarMode_) {
            SpillToRss();
        } else {
            SpillToRssByRow();
            total_input_size = 0;
        }
    } else {
        if (columnarMode_) {
            // Tier 1 (Velox: evict partition buffers): serialize the raw partition
            // buffers into compressed segments kept in memory; this returns the
            // raw bytes, the index and the retained input string vectors. Tier 2
            // (Velox: evict cached payloads) writes those segments to disk only
            // when tier 1 could not cover the request.
            if (num_row_splited_ > 0) {
                SpillToMemory();
            }
            ChargeMemorySpills();
            if (before - taskMemoryPool_->currentBytes() < targetBytes && !memory_spills_.empty()) {
                EvictMemorySpills();
            }
        } else {
            SpillToTmpFileByRow();
            total_input_size = 0;
        }
        isSpill = true;
    }
    const int64_t after = taskMemoryPool_->currentBytes();
    const int64_t freed = std::max<int64_t>(0, before - after);
    if (IsReclaimSuccessTelemetryEnabled()) {
        std::fprintf(stderr,
            "[OMNI-SHUFFLE] event=pool_reclaim target=%ld estimated=%lu writer_pool=%p before=%ld after=%ld freed=%ld scratch_peak=%ld cached=%ld cached_total=%ld evicted_total=%ld spilled_total=%ld\n",
            static_cast<long>(targetBytes), static_cast<unsigned long>(estimated),
            static_cast<void *>(taskMemoryPool_), static_cast<long>(before),
            static_cast<long>(after), static_cast<long>(freed),
            static_cast<long>(ReclaimSpillScratchPeakBytes()), static_cast<long>(memory_spill_bytes_),
            static_cast<long>(total_bytes_cached_), static_cast<long>(total_bytes_evicted_),
            static_cast<long>(total_bytes_spilled_));
    }
    return freed;
}

int Splitter::SpillToMemory()
{
    VectorOutputStream memOut;
    auto header = BuildColumnarHeader();
    auto arrowOut = ArrowOutputStream::Make(
        memOut,
        options_.compression_type,
        spark::CompressionStrategy_COMPRESSION,
        static_cast<uint64_t>(options_.buffer_size),
        options_.compress_block_size,
        *spark::getDefaultPool());

    MemorySpillSegment segment;
    segment.offsets.assign(static_cast<size_t>(num_partitions_) + 1, 0);
    uint64_t offset = 0;
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        // Build each partition's buffers outside the task pool (no headroom
        // inside reclaim), right before it is serialized, and release them after.
        CacheVectorBatch(pid, true, reclaim_spill_pool_.get());
        partition_buffer_size_[pid] = 0;
        const auto written = ArrowWriteColumnarPartition(
            pid, *arrowOut, header, partition_arrow_batch_,
            /*headerAlreadyWritten=*/true);
        segment.offsets[pid] = offset;
        offset += static_cast<uint64_t>(written);
        partition_serialization_size_[pid] = 0;
        partition_arrow_batch_[pid].clear();
    }
    segment.offsets[num_partitions_] = offset;
    auto closeSt = arrowOut->Close();
    if (!closeSt.ok()) {
        LogsError("SpillToMemory Close failed: msg=%s", closeSt.ToString().c_str());
    }
    if (memOut.getLength() != offset) {
        LogsError("SpillToMemory segment size mismatch: stream=%llu partitions=%llu",
                  static_cast<unsigned long long>(memOut.getLength()), static_cast<unsigned long long>(offset));
        throw std::runtime_error("SpillToMemory: partition segments do not cover the stream");
    }
    segment.data = memOut.Release();
    const auto bytes = static_cast<int64_t>(segment.data.size());
    total_bytes_cached_ += bytes;
    memory_spill_uncharged_ += bytes;
    memory_spills_.push_back(std::move(segment));

    memset(partition_id_cnt_cache_, 0, num_partitions_ * sizeof(uint64_t));
    ReleaseVarcharVector();
    num_row_splited_ = 0;
    cached_vectorbatch_size_ = 0;
    for (auto &batches : partition_arrow_batch_) {
        batches.clear();
    }
    return 0;
}

int64_t Splitter::EvictMemorySpills()
{
    int64_t evicted = 0;
    for (auto &segment : memory_spills_) {
        EvictedSpillFile file;
        file.path = CreateTempShuffleFile(NextSpilledFileDir());
        auto out = writeLocalFile(file.path + ".data");
        out->write(segment.data.data(), segment.data.size());
        out->close();
        file.offsets = std::move(segment.offsets);
        const auto bytes = static_cast<int64_t>(segment.data.size());
        total_bytes_spilled_ += bytes;
        total_bytes_evicted_ += bytes;
        evicted += bytes;
        evicted_spill_files_.push_back(std::move(file));
    }
    memory_spills_.clear();
    UnchargeMemorySpills();
    return evicted;
}

void Splitter::ChargeMemorySpills()
{
    if (memory_spill_uncharged_ <= 0) {
        return;
    }
    if (taskMemoryPool_ == nullptr) {
        memory_spill_uncharged_ = 0;
        return;
    }
    // The raw buffers this segment replaced were just freed, so the charge
    // normally fits the pool's existing reservation. When it does not, keep the
    // bytes pending and charge them from the next Split, outside reclaim.
    try {
        taskMemoryPool_->AddMemory(memory_spill_uncharged_);
        memory_spill_bytes_ += memory_spill_uncharged_;
        memory_spill_uncharged_ = 0;
    } catch (const std::exception &e) {
        LogsDebug("ChargeMemorySpills deferred %ld bytes: %s", static_cast<long>(memory_spill_uncharged_), e.what());
    }
}

void Splitter::UnchargeMemorySpills()
{
    if (taskMemoryPool_ != nullptr && memory_spill_bytes_ > 0) {
        taskMemoryPool_->SubMemory(memory_spill_bytes_);
    }
    memory_spill_bytes_ = 0;
    memory_spill_uncharged_ = 0;
}

int Splitter::SpillToTmpFileByRow() {
    options_.next_spilled_file_dir = CreateTempShuffleFile(NextSpilledFileDir());
    WriteDataFileArrowByRow();
    std::shared_ptr<Buffer> ptrTmp = CaculateSpilledTmpFilePartitionOffsets();
    spilled_tmp_files_info_[options_.next_spilled_file_dir] = ptrTmp;
    return 0;
}

Splitter::Splitter(InputDataTypes inputDataTypes, int32_t num_cols, int32_t num_partitions, SplitOptions options, bool flag,
    omniruntime::mem::OmniTaskMemoryPool *taskMemoryPool)
        : taskMemoryPool_(taskMemoryPool),
          rss_mode_(options.rss_mode),
          singlePartitionFlag(flag),
          num_partitions_(num_partitions),
          options_(std::move(options)),
          num_fields_(num_cols),
          input_col_types(inputDataTypes)
{
    // Re-bind options_.allocator to
    // the per-task pool's bound Allocator. SplitOptions is populated at JNI
    // entry (BEFORE the task-bound pool is known) and defaults to a function-
    // local nullBridge in type.h:61. Without this re-bind, all options_.allocator
    // call sites (splitter.cpp:141 / :1080 / :1571 / :1743 / :1747 / :2365)
    // skip per-task accounting — the largest single source of Spark TMM
    // invisibility for shuffle-heavy queries (GB-scale on hash/range shuffle).
    // taskMemoryPool_ == nullptr → keep the nullBridge default (legacy test
    // rigs, accounting skipped but symmetric Free still works).
    if (taskMemoryPool_ != nullptr) {
        options_.allocator = taskMemoryPool_->getBoundAllocator();
    }
    LogsDebug("Input Schema colNum: %d", num_cols);
    ToSplitterTypeId(num_cols);
    arrow_pool_ = std::make_shared<OmniMemoryPoolAdapter>(options_.allocator);
    reclaim_spill_pool_ = std::make_shared<OmniMemoryPoolAdapter>(&unpooledAllocator_);
    partition_arrow_batch_.resize(num_partitions_);
}

Splitter::~Splitter()
{
    memory_spills_.clear();
    UnchargeMemorySpills();
    if (registeredWithTaskPool_ && taskMemoryPool_ != nullptr) {
        taskMemoryPool_->unregisterSpillable(this);
        registeredWithTaskPool_ = false;
    }
    std::lock_guard<std::recursive_mutex> guard(reclaimMutex_);
    NonReclaimableSection nonReclaimable(this);
    ReleaseOwnedState();
    // Explicit split pre-reservation is scoped to this writer. Releasing the
    // owned partition buffers above can leave the task pool with zero used
    // bytes but a retained reservation equal to the writer's former footprint.
    // Return that now instead of pinning Spark execution memory until the
    // task-wide memory manager is destroyed.
    if (taskMemoryPool_ != nullptr) {
        try {
            taskMemoryPool_->releaseUnusedReservation();
        } catch (const std::exception &error) {
            // The pool restores its reservation on failure. Keep that credit
            // for task teardown; a destructor must not terminate the executor.
            LogsError("Splitter reservation release failed during teardown: %s", error.what());
        } catch (...) {
            LogsError("Splitter reservation release failed during teardown");
        }
    }
}

void Splitter::RegisterWithTaskPool()
{
    if (!registeredWithTaskPool_ && taskMemoryPool_ != nullptr) {
        taskMemoryPool_->registerSpillable(this);
        registeredWithTaskPool_ = true;
    }
}

Splitter *Create(InputDataTypes inputDataTypes,
                                 int32_t num_cols,
                                 int32_t num_partitions,
                                 SplitOptions options,
                                 bool flag,
                                 omniruntime::mem::OmniTaskMemoryPool *taskMemoryPool)
{
    auto res = new Splitter(inputDataTypes, num_cols, num_partitions, std::move(options), flag, taskMemoryPool);
    res->Split_Init();
    res->RegisterWithTaskPool();
    return res;
}

Splitter *Splitter::Make(
         const std::string& short_name,
         InputDataTypes inputDataTypes,
         int32_t num_cols,
         int num_partitions,
         SplitOptions options,
         omniruntime::mem::OmniTaskMemoryPool *taskMemoryPool) {
    if (short_name == "hash" || short_name == "rr" || short_name == "range") {
        return Create(inputDataTypes, num_cols, num_partitions, std::move(options), false, taskMemoryPool);
    } else if (short_name == "single") {
        return Create(inputDataTypes, num_cols, num_partitions, std::move(options), true, taskMemoryPool);
    } else {
        throw("ERROR: Unsupported Splitter Type.");
    }
}

std::string Splitter::NextSpilledFileDir() {
    auto spilled_file_dir = GetSpilledShuffleFileDir(configured_dirs_[dir_selection_],
                                                     sub_dir_selection_[dir_selection_]);
    LogsDebug(" spilled_file_dir %s ", spilled_file_dir.c_str());
    sub_dir_selection_[dir_selection_] =
            (sub_dir_selection_[dir_selection_] + 1) % options_.num_sub_dirs;
    dir_selection_ = (dir_selection_ + 1) % configured_dirs_.size();
    return spilled_file_dir;
}

void Splitter::SetRssPushClient(std::shared_ptr<OmniRssPushClient> client)
{
    rss_push_client_ = std::move(client);
}

ArrowFileHeader Splitter::BuildColumnarHeader()
{
    ArrowFileHeader header;
    header.version = kArrowShuffleVersion;
    header.layout = ShuffleLayout::COLUMNAR;
    if (inputDataTypes_.size() != static_cast<size_t>(num_fields_)) {
        LogsError("Splitter header build: inputDataTypes_ size mismatch: types=%zu num_fields=%d",
                  inputDataTypes_.size(), num_fields_);
        throw std::runtime_error("Splitter: inputDataTypes_ not set before building Arrow header");
    }
    for (int i = 0; i < num_fields_; ++i) {
        header.schema.push_back(DataTypeToDescriptor(inputDataTypes_[i]));
    }
    return header;
}

ArrowFileHeader Splitter::BuildRowHeader()
{
    ArrowFileHeader header;
    header.version = kArrowShuffleVersion;
    header.layout = ShuffleLayout::ROW;
    if (inputDataTypes_.size() != static_cast<size_t>(num_fields_)) {
        LogsError("Splitter header build: inputDataTypes_ size mismatch: types=%zu num_fields=%d",
                  inputDataTypes_.size(), num_fields_);
        throw std::runtime_error("Splitter: inputDataTypes_ not set before building Arrow header");
    }
    for (int i = 0; i < num_fields_; ++i) {
        header.schema.push_back(DataTypeToDescriptor(inputDataTypes_[i]));
    }
    return header;
}

int32_t Splitter::PushColumnarPartitionToRss(int32_t pid)
{
    if (partition_arrow_batch_[pid].empty()) {
        return 0;
    }
    if (!rss_push_client_) {
        throw std::runtime_error("RSS push client is not set");
    }

    spark::MemoryOutputStream memOut;
    auto arrowOut = ArrowOutputStream::Make(
        memOut,
        options_.compression_type,
        spark::CompressionStrategy_COMPRESSION,
        static_cast<uint64_t>(options_.buffer_size),
        options_.compress_block_size,
        *spark::getDefaultPool());

    auto header = BuildColumnarHeader();
    auto pushStart = std::chrono::steady_clock::now();
    auto written = ArrowWriteColumnarPartition(
        pid, *arrowOut, header, partition_arrow_batch_, false);
    auto closeSt = arrowOut->Close();
    if (!closeSt.ok()) {
        LogsError("PushColumnarPartitionToRss Close failed: pid=%d msg=%s", pid, closeSt.ToString().c_str());
        throw std::runtime_error("PushColumnarPartitionToRss close failed: " + closeSt.ToString());
    }

    const auto& data = memOut.data();
    if (static_cast<int64_t>(data.size()) < written) {
        throw std::runtime_error("PushColumnarPartitionToRss: memory buffer size mismatch");
    }
    rss_push_client_->pushPartitionData(
        pid, reinterpret_cast<const char*>(data.data()), written);
    auto pushEnd = std::chrono::steady_clock::now();
    total_push_time_ += std::chrono::duration_cast<std::chrono::nanoseconds>(pushEnd - pushStart).count();

    total_bytes_written_ += written;
    partition_lengths_[pid] += written;
    partition_arrow_batch_[pid].clear();
    return written;
}

int32_t Splitter::PushRowPartitionToRss(int32_t pid)
{
    if (partition_rows[pid].empty()) {
        return 0;
    }
    if (!rss_push_client_) {
        throw std::runtime_error("RSS push client is not set");
    }

    spark::MemoryOutputStream memOut;
    auto arrowOut = ArrowOutputStream::Make(
        memOut,
        options_.compression_type,
        spark::CompressionStrategy_COMPRESSION,
        static_cast<uint64_t>(options_.buffer_size),
        options_.compress_block_size,
        *spark::getDefaultPool());

    auto header = BuildRowHeader();
    auto pushStart = std::chrono::steady_clock::now();
    auto written = ArrowWriteRowPartition(
        pid, *arrowOut, header, partition_rows,
        options_.spill_batch_row_num, *arrow_pool_, false);
    auto closeSt = arrowOut->Close();
    if (!closeSt.ok()) {
        LogsError("PushRowPartitionToRss Close failed: pid=%d msg=%s", pid, closeSt.ToString().c_str());
        throw std::runtime_error("PushRowPartitionToRss close failed: " + closeSt.ToString());
    }

    const auto& data = memOut.data();
    if (static_cast<int64_t>(data.size()) < written) {
        throw std::runtime_error("PushRowPartitionToRss: memory buffer size mismatch");
    }
    rss_push_client_->pushPartitionData(
        pid, reinterpret_cast<const char*>(data.data()), written);
    auto pushEnd = std::chrono::steady_clock::now();
    total_push_time_ += std::chrono::duration_cast<std::chrono::nanoseconds>(pushEnd - pushStart).count();

    total_bytes_written_ += written;
    partition_lengths_[pid] += written;
    partition_arena_[pid].Reset();
    partition_rows[pid].clear();
    return written;
}

int Splitter::SpillToRss()
{
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        CacheVectorBatch(pid, true);
        partition_buffer_size_[pid] = 0;
    }
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        PushColumnarPartitionToRss(pid);
    }
    ReleaseVarcharVector();
    num_row_splited_ = 0;
    cached_vectorbatch_size_ = 0;
    return 0;
}

void Splitter::WriteSplitRss()
{
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        CacheVectorBatch(pid, true);
        partition_buffer_size_[pid] = 0;
    }
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        PushColumnarPartitionToRss(pid);
    }
    memset(partition_id_cnt_cache_, 0, num_partitions_ * sizeof(uint64_t));
    ReleaseVarcharVector();
    num_row_splited_ = 0;
    cached_vectorbatch_size_ = 0;
}

int Splitter::SpillToRssByRow()
{
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        PushRowPartitionToRss(pid);
    }
    return 0;
}

void Splitter::WriteSplitRssByRow()
{
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        PushRowPartitionToRss(pid);
    }
}

int Splitter::Stop() {
    std::lock_guard<std::recursive_mutex> guard(reclaimMutex_);
    NonReclaimableSection nonReclaimable(this);
    if (rss_mode_) {
        TIME_NANO_OR_RAISE(total_write_time_, WriteSplitRss());
        if (taskMemoryPool_ != nullptr) {
            taskMemoryPool_->releaseUnusedReservation();
        }
        return 0;
    }
    if (isSpill) {
        TIME_NANO_OR_RAISE(total_write_time_, MergeSpilled());
        TIME_NANO_OR_RAISE(total_write_time_, DeleteSpilledTmpFile());
        LogsDebug(" Spill For Splitter Stopped. total_spill_row_num_: %ld ", total_spill_row_num_);
    } else {
        TIME_NANO_OR_RAISE(total_write_time_, WriteSplit());
    }
    // Batch-to-batch headroom is useful only while the writer is accepting
    // input. Return its unused suffix as soon as native stop completes instead
    // of waiting for the later Scala ShuffleWriter.close callback.
    if (taskMemoryPool_ != nullptr) {
        taskMemoryPool_->releaseUnusedReservation();
    }
    return 0;
}

int Splitter::StopByRow() {
    std::lock_guard<std::recursive_mutex> guard(reclaimMutex_);
    NonReclaimableSection nonReclaimable(this);
    if (rss_mode_) {
        TIME_NANO_OR_RAISE(total_write_time_, WriteSplitRssByRow());
        if (taskMemoryPool_ != nullptr) {
            taskMemoryPool_->releaseUnusedReservation();
        }
        return 0;
    }
    if (isSpill) {
        TIME_NANO_OR_RAISE(total_write_time_, MergeSpilledByRow());
        TIME_NANO_OR_RAISE(total_write_time_, DeleteSpilledTmpFile());
        LogsDebug(" Spill For Splitter Stopped. total_spill_row_num_: %ld ", total_spill_row_num_);
    } else {
        TIME_NANO_OR_RAISE(total_write_time_, WriteSplitByRow());
    }
    if (taskMemoryPool_ != nullptr) {
        taskMemoryPool_->releaseUnusedReservation();
    }
    return 0;
}
