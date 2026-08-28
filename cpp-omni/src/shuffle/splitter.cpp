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
#include <cstring>
#include <string>
#include "shuffle/arrow_columnar_serializer.h"
#include "shuffle/arrow_row_serializer.h"
#include "shuffle/arrow_frame.h"
#include "io/ArrowOutputStream.h"
#include "io/SparkFile.hh"
#include "io/MemoryPool.hh"
#include "shuffle/omni_rss_push_client.h"
#include <arrow/io/file.h>

using namespace omniruntime::vec;
using namespace omniruntime::vec::unsafe;
using namespace omniruntime::type;

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
int Splitter::ComputeAndCountPartitionId(VectorBatch& vb) {
    auto num_rows = vb.GetRowCount();
    memset(partition_id_cnt_cur_, 0, num_partitions_ * sizeof(int32_t));
    partition_id_.resize(num_rows);

    if (singlePartitionFlag) {
        partition_id_cnt_cur_[0] = num_rows;
        partition_id_cnt_cache_[0] += num_rows;
        for (auto i = 0; i < num_rows; ++i) {
            partition_id_[i] = 0;
        }
    } else {
        auto pidVector = vb.Get(0);
        if (pidVector->GetEncoding() == OMNI_ENCODING_CONST) {
            int32_t constPid = reinterpret_cast<ConstVector<int32_t> *>(pidVector)->GetConstValue();
            if (constPid >= num_partitions_) {
                LogsError(" Illegal pid Value: %d >= partition number %d .", constPid, num_partitions_);
                throw std::runtime_error("Shuffle pidVec Illegal pid Value!");
            }
            partition_id_cnt_cur_[constPid] += num_rows;
            partition_id_cnt_cache_[constPid] += num_rows;
            for (auto i = 0; i < num_rows; ++i) {
                partition_id_[i] = constPid;
            }
        } else if (pidVector->GetEncoding() == OMNI_DICTIONARY) {
            auto hash_vct = reinterpret_cast<Vector<DictionaryContainer<int32_t>> *>(pidVector);
            for (auto i = 0; i < num_rows; ++i) {
                int32_t pid = hash_vct->GetValue(i);
                if (pid >= num_partitions_) {
                    LogsError(" Illegal pid Value: %d >= partition number %d .", pid, num_partitions_);
                    throw std::runtime_error("Shuffle pidVec Illegal pid Value!");
                }
                partition_id_[i] = pid;
                partition_id_cnt_cur_[pid]++;
                partition_id_cnt_cache_[pid]++;
            }
        } else if (pidVector->GetEncoding() == OMNI_FLAT) {
            auto hash_vct = reinterpret_cast<Vector<int32_t> *>(pidVector);
            for (auto i = 0; i < num_rows; ++i) {
                // positive mod
                int32_t pid = hash_vct->GetValue(i);
                if (pid >= num_partitions_) {
                    LogsError(" Illegal pid Value: %d >= partition number %d .", pid, num_partitions_);
                    throw std::runtime_error("Shuffle pidVec Illegal pid Value!");
                }
                partition_id_[i] = pid;
                partition_id_cnt_cur_[pid]++;
                partition_id_cnt_cache_[pid]++;
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
                    VCBatchInfo svc(options_.spill_batch_row_num);
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
                    VCBatchInfo svc(options_.spill_batch_row_num);
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
                    VCBatchInfo svc(options_.spill_batch_row_num);
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

    int32_t cur = base;
    for (int32_t i = 0; i < numRows; ++i) {
        auto rowId = row_ids[i];
        bool isNull = vector->IsNull(rowId);
        if (isNull) {
            acc.AppendValidBit(false);
            offsets[acc.rowCursor + i + 1] = cur;
        } else {
            acc.AppendValidBit(true);
            // Get string value
            auto stringVec = reinterpret_cast<Vector<LargeStringContainer<std::string_view>>*>(vector);
            auto sv = stringVec->GetValue(rowId);
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
                    auto new_size = partition_id_cnt_cur_[pid] > options_.buffer_size
                        ? partition_id_cnt_cur_[pid]
                        : options_.buffer_size;
                    auto ptr_tmp = static_cast<uint8_t *>(options_.allocator->Alloc(new_size));
                    if (nullptr == ptr_tmp) {
                        throw std::runtime_error("Allocator for ValidityBuffer Failed! ");
                    }
                    std::shared_ptr<Buffer> validity_buffer (
                        new Buffer((uint8_t *)ptr_tmp, partition_id_cnt_cur_[pid], new_size));
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

int Splitter::CacheVectorBatch(int32_t partition_id, bool reset_buffers) {
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
                            auto vr = arrow::AllocateResizableBuffer(byteCount, arrow_pool_.get());
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
                        auto orStatus = arrow::AllocateResizableBuffer(offsetsSize, arrow_pool_.get());
                        if (!orStatus.ok()) {
                            throw std::runtime_error(
                                "CacheVectorBatch Arrow varchar offsets alloc failed: " + orStatus.status().ToString());
                        }
                        auto offsetsBuf = std::move(*orStatus);
                        int32_t* offsets = reinterpret_cast<int32_t*>(offsetsBuf->mutable_data());

                        // --- values: 拼接字符串体 ---
                        auto vr2 = arrow::AllocateResizableBuffer(totalValuesSize, arrow_pool_.get());
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
                        auto offsetsR = arrow::AllocateBuffer(offsetsLen, arrow_pool_.get());
                        if (!offsetsR.ok()) {
                            throw std::runtime_error("CacheVectorBatch empty offsets alloc failed: "
                                                     + offsetsR.status().ToString());
                        }
                        auto emptyOffsetsBuf = std::move(*offsetsR);
                        std::memset(emptyOffsetsBuf->mutable_data(), 0, static_cast<size_t>(offsetsLen));
                        arrow_batch.buffers.push_back(std::move(emptyOffsetsBuf));  // offsets 全 0

                        auto valuesR = arrow::AllocateBuffer(0, arrow_pool_.get());
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
                            auto vr = arrow::AllocateResizableBuffer(byte_count, arrow_pool_.get());
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
                        // actual_data_size <= capacity（由 CheckCapacityAndAllocate 保证），不会触发 Reallocate。
                        arrow_values->Resize(actual_data_size);
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
            auto new_size = partition_id_cnt_cur_[pid] > options_.buffer_size ? partition_id_cnt_cur_[pid] : options_.buffer_size;
            if (partition_buffer_size_[pid] == 0) {
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
    uint64_t usedMemorySize = omniruntime::mem::MemoryManager::GetGlobalAccountedMemory();
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

    for (uint i = 0; i < column_type_id_.size(); ++i) {
        switch (column_type_id_[i]) {
            case ShuffleTypeId::SHUFFLE_1BYTE:
            case ShuffleTypeId::SHUFFLE_2BYTE:
            case ShuffleTypeId::SHUFFLE_4BYTE:
            case ShuffleTypeId::SHUFFLE_8BYTE:
            case ShuffleTypeId::SHUFFLE_DECIMAL128:
                if (singlePartitionFlag) {
                    fixed_width_array_idx_.push_back(i);
                } else {
                    fixed_width_array_idx_.push_back(i + 1);
                }
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
    auto num_fixed_width = fixed_width_array_idx_.size();
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

    partition_arena_.resize(num_partitions_);
    partition_row_batch.resize(num_partitions_);
    partition_row_batch_count.resize(num_partitions_);
    std::fill(partition_row_batch_count.begin(), partition_row_batch_count.end(), 0);
    partition_rows.resize(num_partitions_);
    return 0;
}

int Splitter::Split(VectorBatch& vb )
{
    LogsTrace(" split vb row number: %d ", vb.GetRowCount());
    TIME_NANO_OR_RAISE(total_compute_pid_time_, ComputeAndCountPartitionId(vb));

    DoSplit(vb);
    return 0;
}

int Splitter::SplitByRow(VectorBatch *vecBatch) {
    int32_t rowCount = vecBatch->GetRowCount();
    for (int pid = 0; pid < num_partitions_; ++pid) {
        auto needCapacity = partition_rows[pid].size() + rowCount;
        if (partition_rows[pid].capacity() < needCapacity) {
            auto prepareCapacity = partition_rows[pid].capacity() * expansion;
            auto newCapacity = prepareCapacity > needCapacity ? prepareCapacity : needCapacity;
            partition_rows[pid].reserve(newCapacity);
        }
    }

    if (singlePartitionFlag) {
        RowBatch *rowBatch = VectorHelper::TransRowBatchFromVectorBatch(vecBatch);
        for (int i = 0; i < rowCount; ++i) {
            RowInfo *rowInfo = rowBatch->Get(i);
            partition_rows[0].emplace_back(rowInfo);
            total_input_size += rowInfo->length;
        }
    } else {
        auto tmpVectorBatch = new VectorBatch(rowCount);
        partition_id_.resize(rowCount);
        memset(partition_id_cnt_cur_, 0, num_partitions_ * sizeof(int32_t));
        BaseVector *pidCol = vecBatch->Get(0);

        if (pidCol->GetEncoding() == OMNI_ENCODING_CONST) {
            int32_t constPid = reinterpret_cast<ConstVector<int32_t> *>(pidCol)->GetConstValue();
            if (constPid >= num_partitions_) {
                LogsError(" Illegal pid Value: %d >= partition number %d .", constPid, num_partitions_);
                throw std::runtime_error("Shuffle pidVec Illegal pid Value!");
            }
            partition_id_cnt_cur_[constPid] += rowCount;
            for (int i = 0; i < rowCount; ++i) {
                partition_id_[i] = constPid;
            }
        } else if (pidCol->GetEncoding() == OMNI_DICTIONARY) {
            auto pidVec = reinterpret_cast<Vector<DictionaryContainer<int32_t>> *>(pidCol);
            for (int i = 0; i < rowCount; ++i) {
                auto pid = pidVec->GetValue(i);
                if (pid >= num_partitions_) {
                    LogsError(" Illegal pid Value: %d >= partition number %d .", pid, num_partitions_);
                    throw std::runtime_error("Shuffle pidVec Illegal pid Value!");
                }
                partition_id_[i] = pid;
                partition_id_cnt_cur_[pid]++;
            }
        } else if (pidCol->GetEncoding() == OMNI_FLAT) {
            auto pidVec = reinterpret_cast<Vector<int32_t> *>(pidCol);
            for (int i = 0; i < rowCount; ++i) {
                auto pid = pidVec->GetValue(i);
                if (pid >= num_partitions_) {
                    LogsError(" Illegal pid Value: %d >= partition number %d .", pid, num_partitions_);
                    throw std::runtime_error("Shuffle pidVec Illegal pid Value!");
                }
                partition_id_[i] = pid;
                partition_id_cnt_cur_[pid]++;
            }
        } else {
         	throw std::runtime_error(
         	    std::string("SplitByRow(pid column): unsupported vector encoding ") +
         	    std::to_string(static_cast<int>(pidCol->GetEncoding())));
        }
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
                partition_rows[pid].emplace_back(new RowInfo(rowBuffer->TakeRowBuffer(), oneRowLen));
                total_input_size += oneRowLen;
            }
        }

        delete vecBatch;
        delete tmpVectorBatch;
    }

    // spill
    // process level: If the memory usage of the current executor exceeds the threshold, spill is triggered.
    uint64_t usedMemorySize = omniruntime::mem::MemoryManager::GetGlobalAccountedMemory();
    if (usedMemorySize > options_.executor_spill_mem_threshold) {
        if (rss_mode_) {
            TIME_NANO_OR_RAISE(total_spill_time_, SpillToRssByRow());
        } else {
            TIME_NANO_OR_RAISE(total_spill_time_, SpillToTmpFileByRow());
            isSpill = true;
        }
        total_input_size = 0;
    }

    // task level: Arrow pool 统一记账（覆盖定宽+变长+复杂+行式全部 Arrow buffer）
    if (arrow_pool_->bytes_allocated() > options_.task_spill_mem_threshold) {
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
    std::shared_ptr<Buffer> ptrPartitionOffsets (new Buffer((uint8_t*)ptr_tmp, 0, (num_partitions_ + 1) * sizeof(uint64_t)));
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

int Splitter::WriteDataFileArrow() {
    std::unique_ptr<OutputStream> outStream = writeLocalFile(options_.next_spilled_file_dir + ".data");

    // Spill 临时文件不压缩（便于后续 mmap），使用 CompressionKind_NONE
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
        outStream.release(),
        CompressionKind_NONE,
        spark::CompressionStrategy_COMPRESSION,
        static_cast<uint64_t>(options_.buffer_size),
        options_.compress_block_size,
        *spark::getDefaultPool());

    // 顺序写入每个partition（每批自带 [4B大端size][文件头][batch帧]，
    // headerAlreadyWritten 参数不再控制文件头写出——serializer 内部每批都写文件头）
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        auto written = ArrowWriteColumnarPartition(
            pid, *arrowOut, header, partition_arrow_batch_,
            /*headerAlreadyWritten=*/true);
        total_bytes_spilled_ += written;
        partition_serialization_size_[pid] = written;
    }
    auto closeSt = arrowOut->Close();
    if (!closeSt.ok()) {
        LogsError("WriteDataFileArrow Close failed: msg=%s", closeSt.ToString().c_str());
    }
    memset(partition_id_cnt_cache_, 0, num_partitions_ * sizeof(uint64_t));
    return 0;
}

int Splitter::WriteDataFileArrowByRow() {
    std::unique_ptr<OutputStream> outStream = writeLocalFile(options_.next_spilled_file_dir + ".data");

    // Spill 临时文件不压缩（便于后续 mmap），使用 CompressionKind_NONE
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
        outStream.release(),
        CompressionKind_NONE,
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
    }
    return 0;
}

// Task 15: mmap 零拷贝透传临时文件段，消除 C8 "磁盘→临时内存"拷贝
// spill 帧与最终写出帧同构，按 [offset, size) 字节段直接写入最终流，不解析帧内容
void Splitter::TransferSpilledSegments(ArrowOutputStream& out,
                                       const std::string& tmpDataFilePath,
                                       uint64_t partitionOffset,
                                       uint64_t partitionSize) {
    if (partitionSize == 0) {
        return;
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
    auto st = out.Write(buffer->data(), static_cast<int64_t>(partitionSize));
    if (!st.ok()) {
        LogsError("TransferSpilledSegments Write failed: path=%s offset=%llu size=%llu msg=%s",
                  tmpDataFilePath.c_str(),
                  static_cast<unsigned long long>(partitionOffset),
                  static_cast<unsigned long long>(partitionSize),
                  st.ToString().c_str());
        throw std::runtime_error("TransferSpilledSegments: Write failed: " + st.ToString());
    }
    auto closeSt = mmapFile->Close();
    if (!closeSt.ok()) {
        LogsError("TransferSpilledSegments Close mmap failed: path=%s offset=%llu size=%llu msg=%s",
                  tmpDataFilePath.c_str(),
                  static_cast<unsigned long long>(partitionOffset),
                  static_cast<unsigned long long>(partitionSize),
                  closeSt.ToString().c_str());
        throw std::runtime_error("TransferSpilledSegments: Close mmap failed: " + closeSt.ToString());
    }
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
        outStream.release(),
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
            TransferSpilledSegments(*arrowOut, tmpDataFilePath, tmpPartitionOffset, tmpPartitionSize);
            // flush 获取压缩后字节数
            uint64_t flushedBytes = arrowOut->FlushAndCount();
            partition_lengths_[pid] += static_cast<int64_t>(flushedBytes);
            total_bytes_written_ += static_cast<int64_t>(flushedBytes);
        }
    }

    auto mergeCloseSt = arrowOut->Close();
    if (!mergeCloseSt.ok()) {
        LogsError("MergeSpilled Close failed: msg=%s", mergeCloseSt.ToString().c_str());
    }

    memset(partition_id_cnt_cache_, 0, num_partitions_ * sizeof(uint64_t));
    ReleaseVarcharVector();
    num_row_splited_ = 0;
    cached_vectorbatch_size_ = 0;
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
        outStream.release(),
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
            TransferSpilledSegments(*arrowOut, tmpDataFilePath, tmpPartitionOffset, tmpPartitionSize);
            // flush 获取压缩后字节数
            uint64_t flushedBytes = arrowOut->FlushAndCount();
            partition_lengths_[pid] += static_cast<int64_t>(flushedBytes);
            total_bytes_written_ += static_cast<int64_t>(flushedBytes);
        }
    }

    auto mergeRowCloseSt = arrowOut->Close();
    if (!mergeRowCloseSt.ok()) {
        LogsError("MergeSpilledByRow Close failed: msg=%s", mergeRowCloseSt.ToString().c_str());
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
        outStream.release(),
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
        outStream.release(),
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
    return 0;
}

int Splitter::SpillToTmpFile() {
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        CacheVectorBatch(pid, true);
        partition_buffer_size_[pid] = 0; // 溢写之后将其清零，条件溢写需要重新分配内存
    }

    options_.next_spilled_file_dir = CreateTempShuffleFile(NextSpilledFileDir());
    WriteDataFileArrow();
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

int Splitter::SpillToTmpFileByRow() {
    options_.next_spilled_file_dir = CreateTempShuffleFile(NextSpilledFileDir());
    WriteDataFileArrowByRow();
    std::shared_ptr<Buffer> ptrTmp = CaculateSpilledTmpFilePartitionOffsets();
    spilled_tmp_files_info_[options_.next_spilled_file_dir] = ptrTmp;
    return 0;
}

Splitter::Splitter(InputDataTypes inputDataTypes, int32_t num_cols, int32_t num_partitions, SplitOptions options, bool flag)
        : rss_mode_(options.rss_mode),
          singlePartitionFlag(flag),
          num_partitions_(num_partitions),
          options_(std::move(options)),
          num_fields_(num_cols),
          input_col_types(inputDataTypes)
{
    LogsDebug("Input Schema colNum: %d", num_cols);
    ToSplitterTypeId(num_cols);
    arrow_pool_ = std::make_shared<OmniMemoryPoolAdapter>(options_.allocator);
    partition_arrow_batch_.resize(num_partitions_);
}

Splitter *Create(InputDataTypes inputDataTypes,
                                 int32_t num_cols,
                                 int32_t num_partitions,
                                 SplitOptions options,
                                 bool flag)
{
    auto res = new Splitter(inputDataTypes, num_cols, num_partitions, std::move(options), flag);
    res->Split_Init();
    return res;
}

Splitter *Splitter::Make(
         const std::string& short_name,
         InputDataTypes inputDataTypes,
         int32_t num_cols,
         int num_partitions,
         SplitOptions options) {
    if (short_name == "hash" || short_name == "rr" || short_name == "range") {
        return Create(inputDataTypes, num_cols, num_partitions, std::move(options), false);
    } else if (short_name == "single") {
        return Create(inputDataTypes, num_cols, num_partitions, std::move(options), true);
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
        &memOut,
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
        &memOut,
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
    if (rss_mode_) {
        TIME_NANO_OR_RAISE(total_write_time_, WriteSplitRss());
        return 0;
    }
    if (isSpill) {
        TIME_NANO_OR_RAISE(total_write_time_, MergeSpilled());
        TIME_NANO_OR_RAISE(total_write_time_, DeleteSpilledTmpFile());
        LogsDebug(" Spill For Splitter Stopped. total_spill_row_num_: %ld ", total_spill_row_num_);
    } else {
        TIME_NANO_OR_RAISE(total_write_time_, WriteSplit());
    }
    return 0;
}

int Splitter::StopByRow() {
    if (rss_mode_) {
        TIME_NANO_OR_RAISE(total_write_time_, WriteSplitRssByRow());
        return 0;
    }
    if (isSpill) {
        TIME_NANO_OR_RAISE(total_write_time_, MergeSpilledByRow());
        TIME_NANO_OR_RAISE(total_write_time_, DeleteSpilledTmpFile());
        LogsDebug(" Spill For Splitter Stopped. total_spill_row_num_: %ld ", total_spill_row_num_);
    } else {
        TIME_NANO_OR_RAISE(total_write_time_, WriteSplitByRow());
    }
    return 0;
}
