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

using namespace omniruntime::vec;
using namespace omniruntime::vec::unsafe;
using namespace omniruntime::type;

void Splitter::InitializeMixedColumnarIndices(MixedVectorBatch& mixedBatch)
{
    mixed_fixed_width_array_idx_.clear();
    mixed_binary_array_idx_.clear();
    mixed_complex_type_array_idx_.clear();
    
    if (mixedBatch.GetMode() == COMPLETE_ROW_ONLY) {
        return;
    }
    
    int32_t start_idx = singlePartitionFlag ? 0 : 1;
    int32_t vector_count = mixedBatch.GetVectorCount();
    
    for (int32_t vec_idx = start_idx; vec_idx < vector_count; ++vec_idx) {
        int32_t schema_idx = mixed_column_count_ + (vec_idx - start_idx);
        
        if (schema_idx >= static_cast<int32_t>(column_type_id_.size())) {
            continue;
        }
        
        switch (column_type_id_[schema_idx]) {
            case ShuffleTypeId::SHUFFLE_1BYTE:
            case ShuffleTypeId::SHUFFLE_2BYTE:
            case ShuffleTypeId::SHUFFLE_4BYTE:
            case ShuffleTypeId::SHUFFLE_8BYTE:
            case ShuffleTypeId::SHUFFLE_DECIMAL128:
                mixed_fixed_width_array_idx_.push_back(vec_idx);
                break;
            case ShuffleTypeId::SHUFFLE_BINARY:
            case ShuffleTypeId::SHUFFLE_LARGE_BINARY:
                mixed_binary_array_idx_.push_back(vec_idx);
                break;
            case ShuffleTypeId::SHUFFLE_ARRAY:
            case ShuffleTypeId::SHUFFLE_MAP:
            case ShuffleTypeId::SHUFFLE_ROW:
                mixed_complex_type_array_idx_.push_back(vec_idx);
                break;
            default:
                break;
        }
    }
    
    auto num_mixed_fixed_width = mixed_fixed_width_array_idx_.size();
    if (partition_mixed_fixed_width_buffers_.size() != num_mixed_fixed_width) {
        partition_mixed_fixed_width_buffers_.resize(num_mixed_fixed_width);
        partition_mixed_fixed_width_value_addrs_.resize(num_mixed_fixed_width);
        partition_mixed_fixed_width_validity_addrs_.resize(num_mixed_fixed_width);
        
        for (uint i = 0; i < num_mixed_fixed_width; ++i) {
            partition_mixed_fixed_width_buffers_[i].resize(num_partitions_);
            partition_mixed_fixed_width_value_addrs_[i].resize(num_partitions_);
            partition_mixed_fixed_width_validity_addrs_[i].resize(num_partitions_);
        }
    }
    
    auto num_mixed_complex = mixed_complex_type_array_idx_.size();
    partition_mixed_complex_type_proto_vecs_.clear();
    partition_mixed_complex_type_proto_vecs_.resize(num_partitions_);
    for (auto i = 0; i < num_partitions_; ++i) {
        partition_mixed_complex_type_proto_vecs_[i].resize(num_mixed_complex);
    }
    
    for (auto i = 0; i < num_partitions_; ++i) {
        if (vc_partition_mixed_array_buffers_[i].size() != column_type_id_.size()) {
            vc_partition_mixed_array_buffers_[i].resize(column_type_id_.size());
        }
    }
}

int Splitter::AllocatePartitionBuffersForMixed(int32_t partition_id, int32_t new_size) {
    for (uint col = 0; col < mixed_fixed_width_array_idx_.size(); ++col) {
        auto vec_idx = mixed_fixed_width_array_idx_[col];
        int32_t schema_idx = mixed_column_count_ + (vec_idx - (singlePartitionFlag ? 0 : 1));
        
        if (schema_idx >= static_cast<int32_t>(column_type_id_.size())) {
            continue;
        }
        
        int32_t type_size = (1 << column_type_id_[schema_idx]);
        int32_t needed_size = new_size * type_size;
        void *ptr_tmp = static_cast<void *>(options_.allocator->Alloc(needed_size));
        fixed_valueBuffer_size_[partition_id] += needed_size;
        if (nullptr == ptr_tmp) {
            throw std::runtime_error("Allocator for AllocatePartitionBuffersForMixed Failed! ");
        }
        std::shared_ptr<Buffer> value_buffer(new Buffer((uint8_t *)ptr_tmp, 0, needed_size));
        partition_mixed_fixed_width_value_addrs_[col][partition_id] =
                const_cast<uint8_t *>(value_buffer->data_);
        partition_mixed_fixed_width_validity_addrs_[col][partition_id] = nullptr;
        partition_mixed_fixed_width_buffers_[col][partition_id] = {
            nullptr, std::move(value_buffer)};
    }

    partition_buffer_size_[partition_id] = new_size;
    return 0;
}

int Splitter::SplitFixedWidthValueBufferForMixed(MixedVectorBatch& mixedBatch) {
    const auto num_rows = mixedBatch.GetRowCount();
    
    if (mixed_fixed_width_array_idx_.size() == 0) {
        return 0;
    }
    
    if (partition_buffer_idx_base_ == nullptr || partition_buffer_idx_offset_ == nullptr) {
        return 0;
    }
    
    if (mixedBatch.GetMode() == COMPLETE_ROW_ONLY) {
        return 0;
    }
    
    std::vector<int64_t> size_delta(num_partitions_, 0);
    
    for (uint col = 0; col < mixed_fixed_width_array_idx_.size(); ++col) {
        if (partition_mixed_fixed_width_value_addrs_.size() <= col) {
            continue;
        }
        
        memset(partition_buffer_idx_offset_, 0, num_partitions_ * sizeof(int32_t));
        std::fill(size_delta.begin(), size_delta.end(), 0);
        
        auto col_idx_vb = mixed_fixed_width_array_idx_[col];
        int32_t col_idx_schema = mixed_column_count_ + (col_idx_vb - (singlePartitionFlag ? 0 : 1));
        
        const auto& dst_addrs = partition_mixed_fixed_width_value_addrs_[col];
        
        if (mixedBatch.Get(col_idx_vb)->GetEncoding() == OMNI_ENCODING_CONST) {
            auto shuffleType = column_type_id_[col_idx_schema];
            const auto shuffle_size = (1 << shuffleType);
            uint8_t constValueBytes[16] = {};
            auto typeId = mixedBatch.Get(col_idx_vb)->GetTypeId();
            switch (typeId) {
                case OMNI_BYTE:
                case OMNI_BOOLEAN: {
                    auto v = reinterpret_cast<ConstVector<int8_t> *>(mixedBatch.Get(col_idx_vb))->GetConstValue();
                    memcpy(constValueBytes, &v, sizeof(v));
                    break;
                }
                case OMNI_SHORT: {
                    auto v = reinterpret_cast<ConstVector<int16_t> *>(mixedBatch.Get(col_idx_vb))->GetConstValue();
                    memcpy(constValueBytes, &v, sizeof(v));
                    break;
                }
                case OMNI_INT:
                case OMNI_DATE32: {
                    auto v = reinterpret_cast<ConstVector<int32_t> *>(mixedBatch.Get(col_idx_vb))->GetConstValue();
                    memcpy(constValueBytes, &v, sizeof(v));
                    break;
                }
                case OMNI_FLOAT: {
                    auto v = reinterpret_cast<ConstVector<float> *>(mixedBatch.Get(col_idx_vb))->GetConstValue();
                    memcpy(constValueBytes, &v, sizeof(v));
                    break;
                }
                case OMNI_LONG:
                case OMNI_TIMESTAMP:
                case OMNI_DATE64:
                case OMNI_DECIMAL64: {
                    auto v = reinterpret_cast<ConstVector<int64_t> *>(mixedBatch.Get(col_idx_vb))->GetConstValue();
                    memcpy(constValueBytes, &v, sizeof(v));
                    break;
                }
                case OMNI_DOUBLE: {
                    auto v = reinterpret_cast<ConstVector<double> *>(mixedBatch.Get(col_idx_vb))->GetConstValue();
                    memcpy(constValueBytes, &v, sizeof(v));
                    break;
                }
                case OMNI_DECIMAL128: {
                    auto v = reinterpret_cast<ConstVector<Decimal128> *>(mixedBatch.Get(col_idx_vb))->GetConstValue();
                    memcpy(constValueBytes, &v, sizeof(v));
                    break;
                }
                default: {
                    throw std::runtime_error("SplitFixedWidthValueBufferForMixed ConstVector unsupported DataTypeId");
                }
            }
            for (auto &pid : partition_used_) {
                if (pid < 0 || pid >= num_partitions_) {
                    continue;
                }
                
                if (dst_addrs.size() <= pid || dst_addrs[pid] == nullptr) {
                    size_delta[pid] = 0;
                    partition_buffer_idx_offset_[pid] = 0;
                    continue;
                }
                
                auto dst_offset = partition_buffer_idx_base_[pid] + partition_buffer_idx_offset_[pid];
                auto dstPidBase = dst_addrs[pid] + dst_offset * shuffle_size;
                auto pos = partition_row_offset_base_[pid];
                auto end = partition_row_offset_base_[pid + 1];
                auto count = end - pos;
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
        } else if (mixedBatch.Get(col_idx_vb)->GetEncoding() == OMNI_DICTIONARY) {
            auto ids_addr = static_cast<int32_t *>(VectorHelper::UnsafeGetValues(mixedBatch.Get(col_idx_vb)));
            auto src_addr = reinterpret_cast<int64_t>(VectorHelper::UnsafeGetDictionary(mixedBatch.Get(col_idx_vb)));
            auto process = [&]<typename CTYPE>(const ShuffleTypeId shuffleTypeId) {
                const auto shuffle_size = (1 << shuffleTypeId);
                for (auto &pid: partition_used_) {
                    if (pid < 0 || pid >= num_partitions_) {
                        continue;
                    }
                    
                    if (dst_addrs.size() <= pid || dst_addrs[pid] == nullptr) {
                        size_delta[pid] = 0;
                        partition_buffer_idx_offset_[pid] = 0;
                        continue;
                    }
                    
                    auto dstPidBase = reinterpret_cast<CTYPE *>(dst_addrs[pid]) + partition_buffer_idx_base_[pid];
                    auto pos = partition_row_offset_base_[pid];
                    auto end = partition_row_offset_base_[pid + 1];
                    auto count = end - pos;
                    for (; pos < end; ++pos) {
                        auto rowId = row_offset_row_id_[pos];
                        *dstPidBase++ = reinterpret_cast<CTYPE *>(src_addr)[ids_addr[rowId]];
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
                    throw std::runtime_error("SplitFixedWidthValueBufferForMixed not match this type");
                }
            }
        } else if (mixedBatch.Get(col_idx_vb)->GetEncoding() == OMNI_FLAT) {
            auto src_addr = reinterpret_cast<int64_t>(VectorHelper::UnsafeGetValues(mixedBatch.Get(col_idx_vb)));
            auto process = [&]<typename CTYPE>(const ShuffleTypeId shuffleTypeId) {
                const auto shuffle_size = (1 << shuffleTypeId);
                for (auto &pid: partition_used_) {
                    if (pid < 0 || pid >= num_partitions_) {
                        continue;
                    }
                    
                    if (dst_addrs.size() <= pid || dst_addrs[pid] == nullptr) {
                        size_delta[pid] = 0;
                        partition_buffer_idx_offset_[pid] = 0;
                        continue;
                    }
                    
                    auto dst_offset = partition_buffer_idx_base_[pid] + partition_buffer_idx_offset_[pid];
                    auto dstPidBase = reinterpret_cast<CTYPE *>(dst_addrs[pid]) + dst_offset;
                    auto pos = partition_row_offset_base_[pid];
                    auto end = partition_row_offset_base_[pid + 1];
                    auto count = end - pos;
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
                    throw std::runtime_error("SplitFixedWidthValueBufferForMixed not match this type");
                }
            }
        } else {
            throw std::runtime_error(
                std::string("SplitFixedWidthValueBufferForMixed: unsupported vector encoding ") +
                std::to_string(static_cast<int>(mixedBatch.Get(col_idx_vb)->GetEncoding())));
        }
        for (auto &pid : partition_used_) {
            if (pid >= 0 && pid < num_partitions_ && 
                partition_mixed_fixed_width_buffers_.size() > col &&
                partition_mixed_fixed_width_buffers_[col].size() > pid &&
                partition_mixed_fixed_width_buffers_[col][pid].size() > 1 &&
                partition_mixed_fixed_width_buffers_[col][pid][1] != nullptr) {
                partition_mixed_fixed_width_buffers_[col][pid][1]->size_ += size_delta[pid];
            }
        }
    }
    return 0;
}

int Splitter::SplitFixedWidthValidityBufferForMixed(MixedVectorBatch& mixedBatch){
    if (mixed_fixed_width_array_idx_.size() == 0) {
        return 0;
    }
    
    if (mixedBatch.GetMode() == COMPLETE_ROW_ONLY) {
        return 0;
    }
    
    for (uint col = 0; col < mixed_fixed_width_array_idx_.size(); ++col) {
        auto col_idx = mixed_fixed_width_array_idx_[col];
        auto& dst_addrs = partition_mixed_fixed_width_validity_addrs_[col];

        if (mixedBatch.Get(col_idx)->HasNull()) {
            for (auto pid = 0; pid < num_partitions_; ++pid) {
                if (partition_id_cnt_cur_[pid] > 0 && dst_addrs[pid] == nullptr) {
                    auto new_size = partition_id_cnt_cur_[pid] > options_.buffer_size
                        ? partition_id_cnt_cur_[pid]
                        : options_.buffer_size;
                    auto ptr_tmp = static_cast<uint8_t *>(options_.allocator->Alloc(new_size));
                    if (nullptr == ptr_tmp) {
                        throw std::runtime_error("Allocator for ValidityBuffer Failed! ");
                    }
                    std::shared_ptr<Buffer> validity_buffer(
                        new Buffer((uint8_t *)ptr_tmp, partition_id_cnt_cur_[pid], new_size));
                    dst_addrs[pid] = const_cast<uint8_t*>(validity_buffer->data_);
                    memset(validity_buffer->data_, 0, new_size);
                    partition_mixed_fixed_width_buffers_[col][pid][0] = std::move(validity_buffer);
                    fixed_nullBuffer_size_[pid] += new_size;
                }
            }

            Encoding validityEnc = mixedBatch.Get(col_idx)->GetEncoding();
            if (validityEnc == OMNI_ENCODING_CONST) {
                uint8_t constNullVal = mixedBatch.Get(col_idx)->IsNull(0) ? 1 : 0;
                for (auto &pid : partition_used_) {
                    if (dst_addrs[pid] == nullptr) {
                        continue;
                    }
                    
                    auto dstPidBase = dst_addrs[pid] + partition_buffer_idx_base_[pid];
                    auto pos = partition_row_offset_base_[pid];
                    auto end = partition_row_offset_base_[pid + 1];
                    for (; pos < end; ++pos) {
                        *dstPidBase++ = constNullVal;
                    }
                }
            } else if (validityEnc == OMNI_DICTIONARY) {
                for (auto &pid: partition_used_) {
                    if (dst_addrs[pid] == nullptr) {
                        continue;
                    }
                    
                    auto dstPidBase = dst_addrs[pid] + partition_buffer_idx_base_[pid];
                    auto pos = partition_row_offset_base_[pid];
                    auto end = partition_row_offset_base_[pid + 1];
                    for (; pos < end; ++pos) {
                        auto rowId = row_offset_row_id_[pos];
                        *dstPidBase++ = mixedBatch.Get(col_idx)->IsNull(rowId);
                    }
                }
            } else if (validityEnc == OMNI_FLAT) {
                auto src_addr = unsafe::UnsafeBaseVector::GetNulls(mixedBatch.Get(col_idx));
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
                    std::string("SplitFixedWidthValidityBufferForMixed: unsupported vector encoding ") +
                    std::to_string(static_cast<int>(validityEnc)));
            }
        }
    }
    return 0;
}

int Splitter::SplitBinaryArrayForMixed(MixedVectorBatch& mixedBatch)
{
    if (mixedBatch.GetMode() == COMPLETE_ROW_ONLY) {
        return 0;
    }
    
    auto vec_cnt_vb = mixedBatch.GetVectorCount();
    int32_t start_idx = singlePartitionFlag ? 0 : 1;
    
    for (int32_t vec_idx = start_idx; vec_idx < vec_cnt_vb; ++vec_idx) {
        int32_t col_schema = mixed_column_count_ + (vec_idx - start_idx);
        
        if (col_schema >= static_cast<int32_t>(column_type_id_.size())) {
            continue;
        }
        
        switch (column_type_id_[col_schema]) {
            case SHUFFLE_BINARY: {
                auto *varcharVector = mixedBatch.Get(vec_idx);
                varcharVectorCache.insert(varcharVector);
                if (varcharVector->HasNull()) {
                    this->template SplitBinaryVectorForMixed<true>(varcharVector, col_schema);
                } else {
                    this->template SplitBinaryVectorForMixed<false>(varcharVector, col_schema);
                }
                break;
            }
            case SHUFFLE_LARGE_BINARY:
                break;
            default:
                break;
        }
    }
    
    return 0;
}

void HandleNullMix(VCBatchInfo &vcbInfo, bool isNull) {
    if(isNull) {
        vcbInfo.SetNullFlag(isNull);
    }
}

template<bool hasNull>
void Splitter::SplitBinaryVectorForMixed(BaseVector *varcharVector, int col_schema) {
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
        for (auto &pid : partition_used_) {
            auto pos = partition_row_offset_base_[pid];
            auto end = partition_row_offset_base_[pid + 1];
            for (; pos < end; ++pos) {
                cached_vectorbatch_size_ += constStrLen;
                if ((vc_partition_mixed_array_buffers_[pid][col_schema].size() != 0) &&
                    (vc_partition_mixed_array_buffers_[pid][col_schema].back().getVcList().size() <
                        options_.spill_batch_row_num)) {
                    if constexpr (hasNull) {
                        HandleNullMix(vc_partition_mixed_array_buffers_[pid][col_schema].back(), constIsNull);
                    }
                    vc_partition_mixed_array_buffers_[pid][col_schema].back().getVcList().emplace_back(
                        (uint64_t)constDst, constStrLen, constIsNull);
                    vc_partition_mixed_array_buffers_[pid][col_schema].back().vcb_total_len += constStrLen;
                } else {
                    VCBatchInfo svc(options_.spill_batch_row_num);
                    svc.getVcList().emplace_back((uint64_t)constDst, constStrLen, constIsNull);
                    svc.vcb_total_len += constStrLen;
                    if constexpr (hasNull) {
                        HandleNullMix(svc, constIsNull);
                    }
                    vc_partition_mixed_array_buffers_[pid][col_schema].emplace_back(std::move(svc));
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
                cached_vectorbatch_size_ += str_len;
                if ((vc_partition_mixed_array_buffers_[pid][col_schema].size() != 0) &&
                    (vc_partition_mixed_array_buffers_[pid][col_schema].back().getVcList().size() <
                        options_.spill_batch_row_num)) {
                    if constexpr (hasNull) {
                        HandleNullMix(vc_partition_mixed_array_buffers_[pid][col_schema].back(), is_null);
                    }
                    vc_partition_mixed_array_buffers_[pid][col_schema].back().getVcList().emplace_back((uint64_t)dst, str_len, is_null);
                    vc_partition_mixed_array_buffers_[pid][col_schema].back().vcb_total_len += str_len;
                } else {
                    VCBatchInfo svc(options_.spill_batch_row_num);
                    svc.getVcList().emplace_back((uint64_t)dst, str_len, is_null);
                    svc.vcb_total_len += str_len;
                    if constexpr (hasNull) {
                        HandleNullMix(svc, is_null);
                    }
                    vc_partition_mixed_array_buffers_[pid][col_schema].emplace_back(std::move(svc));
                }
            }
        }
    } else if (varcharVector->GetEncoding() == OMNI_FLAT) {
        auto vc = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(varcharVector);
        cached_vectorbatch_size_ += num_rows * (sizeof(bool) + sizeof(int32_t)) + sizeof(int32_t);
        for (auto &pid: partition_used_) {
            auto &vc_partition_array = vc_partition_mixed_array_buffers_[pid];
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
                cached_vectorbatch_size_ += str_len;
                if ((vc_partition_array[col_schema].size() != 0) &&
                    (vc_partition_array[col_schema].back().getVcList().size() <
                        options_.spill_batch_row_num)) {
                    if constexpr (hasNull) {
                        HandleNullMix(vc_partition_array[col_schema].back(), is_null);
                    }
                    vc_partition_array[col_schema].back().getVcList().emplace_back((uint64_t)dst, str_len, is_null);
                    vc_partition_array[col_schema].back().vcb_total_len += str_len;
                } else {
                    VCBatchInfo svc(options_.spill_batch_row_num);
                    svc.getVcList().emplace_back((uint64_t)dst, str_len, is_null);
                    if constexpr (hasNull) {
                        HandleNullMix(svc, is_null);
                    }
                    svc.vcb_total_len += str_len;
                    vc_partition_array[col_schema].emplace_back(std::move(svc));
                }
            }
        }
    } else {
        throw std::runtime_error(
            std::string("SplitBinaryVectorForMixed: unsupported vector encoding ") +
            std::to_string(static_cast<int>(varcharVector->GetEncoding())));
    }
}

int Splitter::SplitComplexColumnsForMixed(MixedVectorBatch& mixedBatch)
{
    if (mixedBatch.GetMode() == COMPLETE_ROW_ONLY) {
        return 0;
    }
    
    for (auto &pid: partition_used_) {
        auto pos = partition_row_offset_base_[pid];
        auto end = partition_row_offset_base_[pid + 1];
        auto num_rows = end - pos;
        std::vector<uint32_t> row_ids(num_rows);
        for (int32_t i = 0; pos < end; ++pos, ++i) {
            row_ids[i] = row_offset_row_id_[pos];
        }

        for (uint complex_col_idx = 0; complex_col_idx < mixed_complex_type_array_idx_.size(); ++complex_col_idx) {
            auto col_idx_vb = mixed_complex_type_array_idx_[complex_col_idx];
            auto *vector = mixedBatch.Get(col_idx_vb);
            
            int32_t col_idx_schema = mixed_column_count_ + (col_idx_vb - (singlePartitionFlag ? 0 : 1));
            
            DataTypePtr dataType = inputDataTypes_[col_idx_schema];

            if (partition_mixed_complex_type_proto_vecs_[pid][complex_col_idx] == nullptr) {
                spark::Vec* proto_vec = new spark::Vec();
                partition_mixed_complex_type_proto_vecs_[pid][complex_col_idx] = proto_vec;
                SerializeColumn(vector, row_ids, *proto_vec, dataType);
            } else {
                spark::Vec tmpVec;
                SerializeColumn(vector, row_ids, tmpVec, dataType);
                MergeProtoVec(*partition_mixed_complex_type_proto_vecs_[pid][complex_col_idx], tmpVec);
            }
        }
    }

    return 0;
}

int Splitter::CacheVectorBatchForMixed(int32_t partition_id, bool reset_buffers) {
    if (partition_buffer_idx_base_[partition_id] > 0 && mixed_fixed_width_array_idx_.size() > 0) {
        int64_t batch_partition_size = 0;
        std::vector<std::vector<std::shared_ptr<Buffer>>> bufferArrayTotal(mixed_fixed_width_array_idx_.size());

        for (uint col = 0; col < mixed_fixed_width_array_idx_.size(); ++col) {
            auto& buffers = partition_mixed_fixed_width_buffers_[col][partition_id];
            if (buffers[0] != nullptr) {
                batch_partition_size += buffers[0]->capacity_;
            }
            batch_partition_size += buffers[1]->capacity_;
            if (reset_buffers) {
                bufferArrayTotal[col] = std::move(buffers);
                buffers = {nullptr};
                partition_mixed_fixed_width_validity_addrs_[col][partition_id] = nullptr;
                partition_mixed_fixed_width_value_addrs_[col][partition_id] = nullptr;
            } else {
                bufferArrayTotal[col] = buffers;
            }
        }
        cached_vectorbatch_size_ += batch_partition_size;
        partition_mixed_cached_vectorbatch_[partition_id].push_back(std::move(bufferArrayTotal));
        fixed_valueBuffer_size_[partition_id] = 0;
        fixed_nullBuffer_size_[partition_id] = 0;
        partition_buffer_idx_base_[partition_id] = 0;
    }
    return 0;
}

void Splitter::SerializingFixedColumnsForMixed(int32_t partitionId,
                                              spark::Vec& vec,
                                              int fixColIndexTmp,
                                              SplitRowInfo* splitRowInfoTmp)
{
    if (fixColIndexTmp < 0 || fixColIndexTmp >= static_cast<int>(mixed_fixed_width_array_idx_.size())) {
        vec.mutable_values()->resize(0);
        vec.mutable_nulls()->resize(0);
        return;
    }
    
    auto &cachedBatches = partition_mixed_cached_vectorbatch_[partitionId];
    if (cachedBatches.empty() || splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp] >= cachedBatches.size()) {
        vec.mutable_values()->resize(0);
        vec.mutable_nulls()->resize(0);
        return;
    }
    
    if (splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp] < cachedBatches.size()) {
        auto colIndexTmpSchema = mixed_column_count_ + 
            (mixed_fixed_width_array_idx_[fixColIndexTmp] - (singlePartitionFlag ? 0 : 1));
        int32_t typeSize = (1 << column_type_id_[colIndexTmpSchema]);
        auto onceCopyLen = splitRowInfoTmp->onceCopyRow * typeSize;
        uint32_t onceCopyRow = splitRowInfoTmp->onceCopyRow;

        auto *protoValue = vec.mutable_values();
        protoValue->resize(onceCopyLen);
        uint8_t* valuePtr = reinterpret_cast<uint8_t*>(&(*protoValue)[0]);

        uint8_t* nullPtr = nullptr;
        auto *protoNulls = vec.mutable_nulls();

        uint destCopyedLength = 0;
        uint memCopyLen = 0;
        uint cacheBatchSize = 0;
        bool nullAllocated = false;
        while (destCopyedLength < onceCopyLen) {
            auto &batchIdx = splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp];
            auto &batchCopiedLen = splitRowInfoTmp->cacheBatchCopyedLen[fixColIndexTmp];
            if (batchIdx >= cachedBatches.size()) {
                throw std::runtime_error("Mixed shuffle CacheBatchIndex out of bound.");
            }
            auto &colBuffers = cachedBatches[batchIdx][fixColIndexTmp];
            
            if (colBuffers.size() < 2 || colBuffers[1] == nullptr) {
                throw std::runtime_error("SerializingFixedColumnsForMixed: buffer not initialized for fixColIndexTmp " + std::to_string(fixColIndexTmp));
            }
            
            cacheBatchSize = colBuffers[1]->size_;
            if (not nullAllocated && colBuffers[0] != nullptr) {
                protoNulls->resize(onceCopyRow);
                nullPtr = reinterpret_cast<uint8_t*>(&(*protoNulls)[0]);
                nullAllocated = true;
            }
            if ((onceCopyLen - destCopyedLength) >= (cacheBatchSize - batchCopiedLen)) {
                memCopyLen = cacheBatchSize - batchCopiedLen;
                memcpy(valuePtr + destCopyedLength,
                       colBuffers[1]->data_ + batchCopiedLen,
                       memCopyLen);

                if (colBuffers[0] != nullptr) {
                    memcpy(nullPtr + (destCopyedLength / typeSize),
                           colBuffers[0]->data_ + (batchCopiedLen / typeSize),
                           memCopyLen / typeSize);

                    options_.allocator->Free(colBuffers[0]->data_, colBuffers[0]->capacity_);
                    colBuffers[0]->SetReleaseFlag();
                }
                options_.allocator->Free(colBuffers[1]->data_, colBuffers[1]->capacity_);
                colBuffers[1]->SetReleaseFlag();
                destCopyedLength += memCopyLen;
                batchIdx += 1;
                batchCopiedLen = 0;
            } else {
                memCopyLen = onceCopyLen - destCopyedLength;
                memcpy(valuePtr + destCopyedLength,
                    colBuffers[1]->data_ + batchCopiedLen,
                    memCopyLen);

                if(colBuffers[0] != nullptr) {
                    memcpy(nullPtr + (destCopyedLength / typeSize),
                           colBuffers[0]->data_ + (batchCopiedLen / typeSize),
                           memCopyLen / typeSize);
                }
                destCopyedLength = onceCopyLen;
                batchCopiedLen += memCopyLen;
            }
        }
    }
}

void Splitter::SerializingBinaryColumnsForMixed(int32_t partitionId, spark::Vec& vec, int colIndex, int curBatch)
{
    if (partitionId < 0 || partitionId >= num_partitions_) {
        throw std::runtime_error("SerializingBinaryColumnsForMixed: partitionId out of bounds: " + std::to_string(partitionId));
    }
    
    if (colIndex < 0 || colIndex >= static_cast<int>(column_type_id_.size())) {
        throw std::runtime_error("SerializingBinaryColumnsForMixed: colIndex out of bounds: " + std::to_string(colIndex));
    }
    
    if (vc_partition_mixed_array_buffers_[partitionId].size() <= colIndex) {
        throw std::runtime_error("SerializingBinaryColumnsForMixed: vc_partition_mixed_array_buffers_ second dimension not initialized for colIndex " + std::to_string(colIndex));
    }
    
    if (curBatch < 0 || curBatch >= static_cast<int>(vc_partition_mixed_array_buffers_[partitionId][colIndex].size())) {
        vec.mutable_offsets()->resize(sizeof(int32_t));
        vec.mutable_nulls()->resize(0);
        vec.mutable_values()->resize(0);
        return;
    }
    
    VCBatchInfo &vcb = vc_partition_mixed_array_buffers_[partitionId][colIndex][curBatch];
    int valuesTotalLen = vcb.getVcbTotalLen();
    std::vector<VCLocation> &lst = vcb.getVcList();
    int itemsTotalLen = lst.size();

    auto *protoOffsets = vec.mutable_offsets();
    protoOffsets->resize(sizeof(int32_t) * (itemsTotalLen + 1));

    auto *protoNulls = vec.mutable_nulls();

    auto *protoValues = vec.mutable_values();
    protoValues->resize(valuesTotalLen);

    if(vcb.hasNull()) {
        BytesGen<true>(reinterpret_cast<uint64_t>(protoOffsets->data()),
                 *protoNulls,
                 reinterpret_cast<uint64_t>(protoValues->data()), vcb);
    } else {
        BytesGen<false>(reinterpret_cast<uint64_t>(protoOffsets->data()),
                         *protoNulls,
                         reinterpret_cast<uint64_t>(protoValues->data()), vcb);
    }
}

int Splitter::SplitByMixed(MixedVectorBatch* mixedBatch)
{
    LogsTrace(" split mixedBatch row number: %d ", mixedBatch->GetRowCount());

    mixed_vector_count_ = mixedBatch->GetVectorCount();
    mixed_column_count_ = mixedBatch->GetColumnCount();

    DoSplitByMixed(mixedBatch);
    return 0;
}

int Splitter::DoSplitByMixed(MixedVectorBatch* mixedBatch)
{
    mixed_vector_count_ = mixedBatch->GetVectorCount();
    mixed_column_count_ = mixedBatch->GetColumnCount();
    mixedBatchMode_ = mixedBatch->GetMode();

    TIME_NANO_OR_RAISE(total_compute_pid_time_, ComputeAndCountPartitionId(*mixedBatch));

    int32_t rowCount = mixedBatch->GetRowCount();

    // 确保混存结构已初始化（惰性：仅混存路径需要，纯列存 shuffle task 不白付
    // num_partitions_ 大小的三次分配；三个结构仅在混存路径使用且同时初始化）
    if (static_cast<int32_t>(partition_row_data_.size()) < num_partitions_) {
        partition_row_data_.resize(num_partitions_);
        partition_mixed_cached_vectorbatch_.resize(num_partitions_);
        vc_partition_mixed_array_buffers_.resize(num_partitions_);
    }

    for (auto pid = 0; pid < num_partitions_; ++pid) {
        auto needCapacity = partition_row_data_[pid].offsets.size() + rowCount;
        if (partition_row_data_[pid].offsets.capacity() < needCapacity) {
            auto prepareCapacity = partition_row_data_[pid].offsets.capacity() * expansion;
            auto newCapacity = prepareCapacity > needCapacity ? prepareCapacity : needCapacity;
            partition_row_data_[pid].offsets.reserve(newCapacity);
            partition_row_data_[pid].keyLengths.reserve(newCapacity);
            partition_row_data_[pid].stateOffsets.reserve(newCapacity);
        }
    }

    BuildPartition2Row(rowCount);

    mixed_vector_count_ = mixedBatch->GetVectorCount();
    mixed_column_count_ = mixedBatch->GetColumnCount();

    InitializeMixedColumnarIndices(*mixedBatch);

    for (auto pid = 0; pid < num_partitions_; ++pid) {
        if (mixed_fixed_width_array_idx_.size() > 0 &&
            partition_id_cnt_cur_[pid] > 0 &&
            partition_buffer_idx_base_[pid] + partition_id_cnt_cur_[pid] > partition_buffer_size_[pid]) {
            auto new_size = partition_id_cnt_cur_[pid] > options_.buffer_size ? partition_id_cnt_cur_[pid] : options_.buffer_size;
            if (partition_buffer_size_[pid] == 0) {
                AllocatePartitionBuffersForMixed(pid, new_size);
            } else {
                CacheVectorBatchForMixed(pid, true);
                AllocatePartitionBuffersForMixed(pid, new_size);
            }
        }
    }

    SplitFixedWidthValueBufferForMixed(*mixedBatch);
    SplitFixedWidthValidityBufferForMixed(*mixedBatch);

    current_fixed_alloc_buffer_size_ = 0;
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        partition_buffer_idx_base_[pid] += partition_id_cnt_cur_[pid];
        current_fixed_alloc_buffer_size_ += fixed_valueBuffer_size_[pid];
        current_fixed_alloc_buffer_size_ += fixed_nullBuffer_size_[pid];
    }

    SplitBinaryArrayForMixed(*mixedBatch);

    if (mixed_complex_type_array_idx_.size() > 0) {
        SplitComplexColumnsForMixed(*mixedBatch);
    }

    // 立即拷贝 row segment 数据到 partition_row_data_，不再持有 batch 引用
    if (mixed_column_count_ > 0 || mixedBatch->GetMode() == MixedBatchMode::COMPLETE_ROW_ONLY) {
        for (auto &pid : partition_used_) {
            auto pos = partition_row_offset_base_[pid];
            auto end = partition_row_offset_base_[pid + 1];
            auto& prd = partition_row_data_[pid];
            for (; pos < end; ++pos) {
                auto rowId = row_offset_row_id_[pos];
                auto rowInfo = mixedBatch->GetRow(rowId);

                prd.offsets.push_back(static_cast<int32_t>(prd.rowBytes.size()));
                prd.rowBytes.append(reinterpret_cast<const char*>(rowInfo->data), rowInfo->length);
                prd.keyLengths.push_back(rowInfo->keyLength);
                prd.stateOffsets.push_back(rowInfo->stateOffset);

                total_input_size += rowInfo->length;
            }
        }
    }

    num_row_splited_ += mixedBatch->GetRowCount();

    uint64_t usedMemorySize = omniruntime::mem::MemoryManager::GetGlobalAccountedMemory();
    if (usedMemorySize > options_.executor_spill_mem_threshold) {
        TIME_NANO_OR_RAISE(total_spill_time_, SpillToTmpFileByMixed());
        isSpill = true;
    }

    if (cached_vectorbatch_size_ + current_fixed_alloc_buffer_size_ + total_input_size >= options_.task_spill_mem_threshold) {
        TIME_NANO_OR_RAISE(total_spill_time_, SpillToTmpFileByMixed());
        total_input_size = 0;
        isSpill = true;
    }

    // 释放 batch（和列存 ReleaseVectorBatch 一致）
    mixedBatch->ClearVectors();
    delete mixedBatch;
    this->ResetInputVecBatch();
    return 0;
}

int Splitter::protoSpillPartitionByMixed(int32_t partition_id, std::unique_ptr<BufferedOutputStream> &bufferStream)
{
    bool isCompleteRowMode = (mixedBatchMode_ == COMPLETE_ROW_ONLY);

    SplitRowInfo splitRowInfoTmp;
    splitRowInfoTmp.copyedRow = 0;
    splitRowInfoTmp.remainCopyRow = partition_id_cnt_cache_[partition_id];
    splitRowInfoTmp.cacheBatchIndex.resize(mixed_fixed_width_array_idx_.size());
    splitRowInfoTmp.cacheBatchCopyedLen.resize(mixed_fixed_width_array_idx_.size());

    int curBatch = 0;
    total_spill_row_num_ += splitRowInfoTmp.remainCopyRow;

    auto& prd = partition_row_data_[partition_id];
    // 补 trailing offset，使 offsets[i+1] - offsets[i] 可安全计算最后一行长度
    if (prd.offsets.size() == prd.keyLengths.size() && !prd.offsets.empty()) {
        prd.offsets.push_back(static_cast<int32_t>(prd.rowBytes.size()));
    }

    while (0 < splitRowInfoTmp.remainCopyRow) {
        if (options_.spill_batch_row_num < splitRowInfoTmp.remainCopyRow) {
            splitRowInfoTmp.onceCopyRow = options_.spill_batch_row_num;
        } else {
            splitRowInfoTmp.onceCopyRow = splitRowInfoTmp.remainCopyRow;
        }

        protoMixedBatch->set_rowcnt(splitRowInfoTmp.onceCopyRow);
        protoMixedBatch->set_veccnt(isCompleteRowMode ? 0 : column_type_id_.size());
        protoMixedBatch->set_mixtype(1);

        auto* offsetsStr = protoMixedBatch->mutable_offsets();
        offsetsStr->resize((splitRowInfoTmp.onceCopyRow + 1) * sizeof(int32_t));
        auto* offsetsPtr = reinterpret_cast<int32_t*>(&(*offsetsStr)[0]);
        offsetsPtr[0] = 0;

        auto* keyLenStr = protoMixedBatch->mutable_key_lengths();
        keyLenStr->resize(splitRowInfoTmp.onceCopyRow * sizeof(int32_t));
        auto* keyLenPtr = reinterpret_cast<int32_t*>(&(*keyLenStr)[0]);

        auto* stateOffStr = protoMixedBatch->mutable_state_offsets();
        stateOffStr->resize(splitRowInfoTmp.onceCopyRow * sizeof(int32_t));
        auto* stateOffPtr = reinterpret_cast<int32_t*>(&(*stateOffStr)[0]);

        int32_t totalLen = 0;
        for (int32_t i = 0; i < splitRowInfoTmp.onceCopyRow; ++i) {
            int32_t rowIdx = splitRowInfoTmp.copyedRow + i;
            int32_t rowLen = prd.offsets[rowIdx + 1] - prd.offsets[rowIdx];
            offsetsPtr[i + 1] = offsetsPtr[i] + rowLen;
            keyLenPtr[i] = prd.keyLengths[rowIdx];
            stateOffPtr[i] = prd.stateOffsets[rowIdx];
            totalLen += rowLen;
        }

        // rowBytes 已连续，一次构造替代逐行 append
        int32_t startOff = prd.offsets[splitRowInfoTmp.copyedRow];
        std::string rows(prd.rowBytes, startOff, totalLen);
        protoMixedBatch->set_rows(std::move(rows));

        for (int32_t i = 0; i < mixed_column_count_; ++i) {
            spark::VecType *vt = protoMixedBatch->add_vectypes();
            InitVecType(vt, inputDataTypes_[i]);
        }

        int fixColIndexTmp = 0;
        int complexColIndexTmp = 0;
        
        if (!isCompleteRowMode) {
            for (size_t indexSchema = 0; indexSchema < column_type_id_.size(); indexSchema++) {
                spark::Vec *vec = protoMixedBatch->vecs_size() > static_cast<int>(indexSchema)
                    ? protoMixedBatch->mutable_vecs(indexSchema) : protoMixedBatch->add_vecs();
                
                bool isColumnarShuffle = (indexSchema >= mixed_column_count_);
                
                switch (column_type_id_[indexSchema]) {
                    case ShuffleTypeId::SHUFFLE_1BYTE:
                    case ShuffleTypeId::SHUFFLE_2BYTE:
                    case ShuffleTypeId::SHUFFLE_4BYTE:
                    case ShuffleTypeId::SHUFFLE_8BYTE:
                    case ShuffleTypeId::SHUFFLE_DECIMAL128: {
                        if (isColumnarShuffle && mixed_fixed_width_array_idx_.size() > 0) {
                            SerializingFixedColumnsForMixed(partition_id, *vec, fixColIndexTmp, &splitRowInfoTmp);
                            fixColIndexTmp++;
                        } else {
                            vec->mutable_values()->resize(0);
                            vec->mutable_nulls()->resize(0);
                        }
                        break;
                    }
                    case ShuffleTypeId::SHUFFLE_BINARY: {
                        if (isColumnarShuffle && mixed_binary_array_idx_.size() > 0) {
                            SerializingBinaryColumnsForMixed(partition_id, *vec, indexSchema, curBatch);
                        } else {
                            vec->mutable_offsets()->resize(sizeof(int32_t));
                            vec->mutable_nulls()->resize(0);
                            vec->mutable_values()->resize(0);
                        }
                        break;
                    }
                    case ShuffleTypeId::SHUFFLE_ARRAY:
                    case ShuffleTypeId::SHUFFLE_MAP:
                    case ShuffleTypeId::SHUFFLE_ROW: {
                        if (isColumnarShuffle && mixed_complex_type_array_idx_.size() > 0 &&
                            partition_mixed_complex_type_proto_vecs_[partition_id][complexColIndexTmp] != nullptr) {
                            *vec = *partition_mixed_complex_type_proto_vecs_[partition_id][complexColIndexTmp];
                        } else {
                            vec->mutable_values()->resize(0);
                            vec->mutable_nulls()->resize(0);
                        }
                        complexColIndexTmp++;
                        break;
                    }
                    default: {
                        throw std::runtime_error("protoSpillPartitionByMixed # Unsupported ShuffleType: " + std::to_string(column_type_id_[indexSchema]));
                    }
                }
                spark::VecType *vt = vec->mutable_vectype();
                vt->set_typeid_(proto_col_types_[indexSchema]);
                if(vt->typeid_() == spark::VecType::VEC_TYPE_DECIMAL128 || vt->typeid_() == spark::VecType::VEC_TYPE_DECIMAL64){
                    vt->set_precision(input_col_types.inputDataPrecisions[indexSchema]);
                    vt->set_scale(input_col_types.inputDataScales[indexSchema]);
                }
            }
        }
        curBatch++;

        auto byteSize = protoMixedBatch->ByteSizeLong();
        if (byteSize > UINT32_MAX) {
            throw std::runtime_error("Unsafe static_cast long to uint_32t.");
        }
        uint32_t protoMixedBatchSize = reversebytes_uint32t(static_cast<uint32_t>(byteSize));
        void *buffer = nullptr;
        if (!bufferStream->NextNBytes(&buffer, sizeof(protoMixedBatchSize))) {
            throw std::runtime_error("Allocate Memory Failed: Flush Spilled Data, Next failed.");
        }
        memcpy(buffer, &protoMixedBatchSize, sizeof(protoMixedBatchSize));

        protoMixedBatch->SerializeToZeroCopyStream(bufferStream.get());

        splitRowInfoTmp.remainCopyRow -= splitRowInfoTmp.onceCopyRow;
        splitRowInfoTmp.copyedRow += splitRowInfoTmp.onceCopyRow;
        protoMixedBatch->Clear();
    }

    uint64_t partitionBatchSize = bufferStream->flush();
    total_bytes_spilled_ += partitionBatchSize;
    partition_serialization_size_[partition_id] = partitionBatchSize;

    partition_mixed_cached_vectorbatch_[partition_id].clear();
    if (!isCompleteRowMode) {
        for (size_t col = 0; col < column_type_id_.size(); col++) {
            vc_partition_mixed_array_buffers_[partition_id][col].clear();
        }
    }
    for (size_t complexIdx = 0; complexIdx < mixed_complex_type_array_idx_.size(); ++complexIdx) {
        if (partition_mixed_complex_type_proto_vecs_[partition_id][complexIdx] != nullptr) {
            partition_mixed_complex_type_proto_vecs_[partition_id][complexIdx]->Clear();
        }
    }
    ClearPartitionMixedRefs(partition_id);

    return 0;
}

int32_t Splitter::ProtoWritePartitionByMixed(int32_t partition_id, std::unique_ptr<BufferedOutputStream> &bufferStream, void *bufferOut, int32_t &sizeOut)
{
    bool isCompleteRowMode = (mixedBatchMode_ == COMPLETE_ROW_ONLY);

    SplitRowInfo splitRowInfoTmp;
    splitRowInfoTmp.copyedRow = 0;
    splitRowInfoTmp.remainCopyRow = partition_id_cnt_cache_[partition_id];
    splitRowInfoTmp.cacheBatchIndex.resize(mixed_fixed_width_array_idx_.size());
    splitRowInfoTmp.cacheBatchCopyedLen.resize(mixed_fixed_width_array_idx_.size());

    int curBatch = 0;
    auto& prd = partition_row_data_[partition_id];
    // 补 trailing offset，使 offsets[i+1] - offsets[i] 可安全计算最后一行长度
    if (prd.offsets.size() == prd.keyLengths.size() && !prd.offsets.empty()) {
        prd.offsets.push_back(static_cast<int32_t>(prd.rowBytes.size()));
    }

    while (0 < splitRowInfoTmp.remainCopyRow) {
        if (options_.spill_batch_row_num < splitRowInfoTmp.remainCopyRow) {
            splitRowInfoTmp.onceCopyRow = options_.spill_batch_row_num;
        } else {
            splitRowInfoTmp.onceCopyRow = splitRowInfoTmp.remainCopyRow;
        }

        protoMixedBatch->set_rowcnt(splitRowInfoTmp.onceCopyRow);
        protoMixedBatch->set_veccnt(isCompleteRowMode ? 0 : column_type_id_.size());
        protoMixedBatch->set_mixtype(1);

        auto* offsetsStr = protoMixedBatch->mutable_offsets();
        offsetsStr->resize((splitRowInfoTmp.onceCopyRow + 1) * sizeof(int32_t));
        auto* offsetsPtr = reinterpret_cast<int32_t*>(&(*offsetsStr)[0]);
        offsetsPtr[0] = 0;

        auto* keyLenStr = protoMixedBatch->mutable_key_lengths();
        keyLenStr->resize(splitRowInfoTmp.onceCopyRow * sizeof(int32_t));
        auto* keyLenPtr = reinterpret_cast<int32_t*>(&(*keyLenStr)[0]);

        auto* stateOffStr = protoMixedBatch->mutable_state_offsets();
        stateOffStr->resize(splitRowInfoTmp.onceCopyRow * sizeof(int32_t));
        auto* stateOffPtr = reinterpret_cast<int32_t*>(&(*stateOffStr)[0]);

        int32_t totalLen = 0;
        for (int32_t i = 0; i < splitRowInfoTmp.onceCopyRow; ++i) {
            int32_t rowIdx = splitRowInfoTmp.copyedRow + i;
            int32_t rowLen = prd.offsets[rowIdx + 1] - prd.offsets[rowIdx];
            offsetsPtr[i + 1] = offsetsPtr[i] + rowLen;
            keyLenPtr[i] = prd.keyLengths[rowIdx];
            stateOffPtr[i] = prd.stateOffsets[rowIdx];
            totalLen += rowLen;
        }

        // rowBytes 已连续，一次构造替代逐行 append
        int32_t startOff = prd.offsets[splitRowInfoTmp.copyedRow];
        std::string rows(prd.rowBytes, startOff, totalLen);
        protoMixedBatch->set_rows(std::move(rows));

        for (int32_t i = 0; i < mixed_column_count_; ++i) {
            spark::VecType *vt = protoMixedBatch->add_vectypes();
            InitVecType(vt, inputDataTypes_[i]);
        }

        int fixColIndexTmp = 0;
        int complexColIndexTmp = 0;

        if (!isCompleteRowMode) {
            for (size_t indexSchema = 0; indexSchema < column_type_id_.size(); indexSchema++) {
            spark::Vec *vec = protoMixedBatch->vecs_size() > static_cast<int>(indexSchema)
                ? protoMixedBatch->mutable_vecs(indexSchema) : protoMixedBatch->add_vecs();
            
            bool isColumnarShuffle = (indexSchema >= mixed_column_count_);
            
            switch (column_type_id_[indexSchema]) {
                case ShuffleTypeId::SHUFFLE_1BYTE:
                case ShuffleTypeId::SHUFFLE_2BYTE:
                case ShuffleTypeId::SHUFFLE_4BYTE:
                case ShuffleTypeId::SHUFFLE_8BYTE:
                case ShuffleTypeId::SHUFFLE_DECIMAL128: {
                    if (isColumnarShuffle && mixed_fixed_width_array_idx_.size() > 0) {
                        SerializingFixedColumnsForMixed(partition_id, *vec, fixColIndexTmp, &splitRowInfoTmp);
                        fixColIndexTmp++;
                    } else {
                        vec->mutable_values()->resize(0);
                        vec->mutable_nulls()->resize(0);
                    }
                    break;
                }
                case ShuffleTypeId::SHUFFLE_BINARY: {
                    if (isColumnarShuffle && mixed_binary_array_idx_.size() > 0) {
                        SerializingBinaryColumnsForMixed(partition_id, *vec, indexSchema, curBatch);
                    } else {
                        vec->mutable_offsets()->resize(sizeof(int32_t));
                        vec->mutable_nulls()->resize(0);
                        vec->mutable_values()->resize(0);
                    }
                    break;
                }
                case ShuffleTypeId::SHUFFLE_ARRAY:
                case ShuffleTypeId::SHUFFLE_MAP:
                case ShuffleTypeId::SHUFFLE_ROW: {
                    if (isColumnarShuffle && mixed_complex_type_array_idx_.size() > 0 &&
                        partition_mixed_complex_type_proto_vecs_[partition_id][complexColIndexTmp] != nullptr) {
                        *vec = *partition_mixed_complex_type_proto_vecs_[partition_id][complexColIndexTmp];
                    } else {
                        vec->mutable_values()->resize(0);
                        vec->mutable_nulls()->resize(0);
                    }
                    complexColIndexTmp++;
                    break;
                }
                default: {
                    throw std::runtime_error("ProtoWritePartitionByMixed # Unsupported ShuffleType: " + std::to_string(column_type_id_[indexSchema]));
                }
            }
            spark::VecType *vt = vec->mutable_vectype();
            vt->set_typeid_(proto_col_types_[indexSchema]);
            if(vt->typeid_() == spark::VecType::VEC_TYPE_DECIMAL128 || vt->typeid_() == spark::VecType::VEC_TYPE_DECIMAL64){
                vt->set_precision(input_col_types.inputDataPrecisions[indexSchema]);
                vt->set_scale(input_col_types.inputDataScales[indexSchema]);
            }
            }
        }
        curBatch++;

        auto byteSize = protoMixedBatch->ByteSizeLong();
        if (byteSize > UINT32_MAX) {
            throw std::runtime_error("Unsafe static_cast long to uint_32t.");
        }
        uint32_t protoMixedBatchSize = reversebytes_uint32t(static_cast<uint32_t>(byteSize));
        if (bufferStream->Next(&bufferOut, &sizeOut)) {
            memcpy(bufferOut, &protoMixedBatchSize, sizeof(protoMixedBatchSize));
            if (sizeof(protoMixedBatchSize) < static_cast<uint32_t>(sizeOut)) {
                bufferStream->BackUp(sizeOut - sizeof(protoMixedBatchSize));
            }
        }

        protoMixedBatch->SerializeToZeroCopyStream(bufferStream.get());
        splitRowInfoTmp.remainCopyRow -= splitRowInfoTmp.onceCopyRow;
        splitRowInfoTmp.copyedRow += splitRowInfoTmp.onceCopyRow;
        protoMixedBatch->Clear();
    }

    uint64_t partitionBatchSize = bufferStream->flush();
    total_bytes_written_ += partitionBatchSize;
    partition_lengths_[partition_id] += partitionBatchSize;

    partition_mixed_cached_vectorbatch_[partition_id].clear();
    if (!isCompleteRowMode) {
        for (size_t col = 0; col < column_type_id_.size(); col++) {
            vc_partition_mixed_array_buffers_[partition_id][col].clear();
        }
    }
    for (size_t complexIdx = 0; complexIdx < mixed_complex_type_array_idx_.size(); ++complexIdx) {
        if (partition_mixed_complex_type_proto_vecs_[partition_id][complexIdx] != nullptr) {
            partition_mixed_complex_type_proto_vecs_[partition_id][complexIdx]->Clear();
        }
    }
    ClearPartitionMixedRefs(partition_id);

    return 0;
}

int Splitter::WriteDataFileProtoByMixed()
{
    std::unique_ptr<OutputStream> outStream = writeLocalFile(options_.next_spilled_file_dir + ".data");
    WriterOptions options;
    options.setCompression(CompressionKind_NONE);
    std::unique_ptr<StreamsFactory> streamsFactory = createStreamsFactory(options, outStream.get());
    std::unique_ptr<BufferedOutputStream> bufferStream = streamsFactory->createStream();
    
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        protoSpillPartitionByMixed(pid, bufferStream);
    }
    memset(partition_id_cnt_cache_, 0, num_partitions_ * sizeof(uint64_t));
    outStream->close();
    return 0;
}

void Splitter::MergeSpilledByMixed()
{
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        CacheVectorBatchForMixed(pid, true);
        partition_buffer_size_[pid] = 0;
    }

    std::unique_ptr<OutputStream> outStream = writeLocalFile(options_.data_file);
    WriterOptions options;
    options.setCompression(options_.compression_type);
    options.setCompressionBlockSize(options_.compress_block_size);
    options.setCompressionStrategy(CompressionStrategy_COMPRESSION);
    std::unique_ptr<StreamsFactory> streamsFactory = createStreamsFactory(options, outStream.get());
    std::unique_ptr<BufferedOutputStream> bufferOutPutStream = streamsFactory->createStream();

    void* bufferOut = nullptr;
    int32_t sizeOut = 0;
    for (int pid = 0; pid < num_partitions_; pid++) {
        ProtoWritePartitionByMixed(pid, bufferOutPutStream, bufferOut, sizeOut);
        for (auto &pair : spilled_tmp_files_info_) {
            auto tmpDataFilePath = pair.first + ".data";
            auto tmpPartitionOffset = reinterpret_cast<uint64_t *>(pair.second->data_)[pid];
            auto tmpPartitionSize = reinterpret_cast<uint64_t *>(pair.second->data_)[pid + 1] - reinterpret_cast<uint64_t *>(pair.second->data_)[pid];
            std::unique_ptr<InputStream> inputStream = readLocalFile(tmpDataFilePath);
            uint64_t targetLen = tmpPartitionSize;
            uint64_t seekPosit = tmpPartitionOffset;
            uint64_t onceReadLen = 0;
            while ((targetLen > 0) && bufferOutPutStream->Next(&bufferOut, &sizeOut)) {
                onceReadLen = targetLen > static_cast<uint32_t>(sizeOut) ? sizeOut : targetLen;
                inputStream->read(bufferOut, onceReadLen, seekPosit);
                targetLen -= onceReadLen;
                seekPosit += onceReadLen;
                if (onceReadLen < static_cast<uint32_t>(sizeOut)) {
                    bufferOutPutStream->BackUp(sizeOut - onceReadLen);
                    break;
                }
            }

            uint64_t flushSize = bufferOutPutStream->flush();
            total_bytes_written_ += flushSize;
            partition_lengths_[pid] += flushSize;
}
    }
    
    memset(partition_id_cnt_cache_, 0, num_partitions_ * sizeof(uint64_t));
    ReleaseVarcharVector();
    num_row_splited_ = 0;
    cached_vectorbatch_size_ = 0;
    outStream->close();
}

void Splitter::WriteSplitByMixed()
{
    bool isCompleteRowOnly = (mixedBatchMode_ == COMPLETE_ROW_ONLY);

    // === 列shuffle前置处理：flush列缓冲（行shuffle跳过，无列数据）===
    if (!isCompleteRowOnly) {
        for (auto pid = 0; pid < num_partitions_; ++pid) {
            CacheVectorBatchForMixed(pid, true);
            partition_buffer_size_[pid] = 0;
        }
    }

    // === 公共：创建输出流 + 序列化 ===
    std::unique_ptr<OutputStream> outStream = writeLocalFile(options_.data_file);
    WriterOptions options;
    options.setCompression(options_.compression_type);
    options.setCompressionBlockSize(options_.compress_block_size);
    options.setCompressionStrategy(CompressionStrategy_COMPRESSION);
    std::unique_ptr<StreamsFactory> streamsFactory = createStreamsFactory(options, outStream.get());
    std::unique_ptr<BufferedOutputStream> bufferOutPutStream = streamsFactory->createStream();

    void* bufferOut = nullptr;
    int32_t sizeOut = 0;
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        ProtoWritePartitionByMixed(pid, bufferOutPutStream, bufferOut, sizeOut);
    }

    // === 公共：状态清理（DoSplitByMixed累积的状态，两种模式都需要）===
    memset(partition_id_cnt_cache_, 0, num_partitions_ * sizeof(uint64_t));
    num_row_splited_ = 0;

    // === 列shuffle后置处理：释放列缓存（行shuffle跳过，无列缓存）===
    if (!isCompleteRowOnly) {
        ReleaseVarcharVector();
        cached_vectorbatch_size_ = 0;
    }

    outStream->close();
}

int Splitter::StopByMixed()
{
    if (isSpill) {
        TIME_NANO_OR_RAISE(total_write_time_, MergeSpilledByMixed());
        TIME_NANO_OR_RAISE(total_write_time_, DeleteSpilledTmpFile());
    } else {
        TIME_NANO_OR_RAISE(total_write_time_, WriteSplitByMixed());
    }
    if (nullptr == protoMixedBatch) {
        throw std::runtime_error("delete nullptr error for free protobuf mixedBatch memory");
    }
    return 0;
}