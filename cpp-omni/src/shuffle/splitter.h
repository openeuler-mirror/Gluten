/**
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef CPP_SPLITTER_H
#define CPP_SPLITTER_H

#include <vector/vector_common.h>
#include <cstring>
#include <vector>
#include <chrono>
#include <memory>
#include <list>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>

#include "type.h"
#include "shuffle/arrow_frame.h"
#include "../io/ColumnWriter.hh"
#include "../common/common.h"
#include "vector/omni_row.h"
#include "shuffle/omni_arrow_memory_pool.h"
#include <arrow/buffer.h>

// Forward declaration for TransferSpilledSegments
class ArrowOutputStream;
class OmniRssPushClient;

using namespace std;
using namespace spark;
using namespace omniruntime::vec;
using namespace omniruntime::type;

// Arrow 化后的列式缓存批：一个缓存批 = 一帧，持有各列 arrow::Buffer 列表
// 定宽列 buffers 顺序 = [validity?][values]（validity 仅在该列含 null 时存在，全有效置 nullptr）
struct ArrowColumnarCachedBatch {
    int32_t rowCount = 0;
    std::vector<std::shared_ptr<arrow::Buffer>> buffers;   // 按列 schema 顺序、复杂类型递归展开
};

// =====================================================================================
// ComplexColumnAccumulator: 复杂类型列在一个分区中的 Arrow buffer 增量累积器（方案 C）
// 递归持有 offsets / validity / child values 及写入游标，多批数据首尾接续追加，
// 消除 MergeArrowBufferLists 合并步骤。
// =====================================================================================
struct ComplexColumnAccumulator {
    enum class Kind { FIXED, VARLEN, LIST, MAP, STRUCT, ROOT };

    // --- 物理缓冲（nullptr = 该层级无此 buffer）---
    std::shared_ptr<arrow::ResizableBuffer> offsets;    // int32 offsets（LIST/MAP/变长 child 有；STRUCT/定宽 child 无）
    std::shared_ptr<arrow::ResizableBuffer> validity;   // Arrow validity bitmap（bit=1=valid，全有效时 nullptr）
    std::shared_ptr<arrow::ResizableBuffer> values;     // 定宽 child 值缓冲 或 变长 child 串体缓冲（叶子节点才有）

    // --- 写入游标 ---
    int64_t rowCursor = 0;          // 已写入行数
    int64_t elemCursor = 0;         // 已写入子元素总数（LIST/MAP offsets 基址）
    int64_t valueBytesCursor = 0;   // 变长 child 串体已写字节数

    // --- 递归子节点 ---
    std::vector<std::unique_ptr<ComplexColumnAccumulator>> children;

    // --- 列元数据 ---
    Kind kind = Kind::ROOT;
    int32_t fixedElemSize = 0;      // kind==FIXED 时为元素字节数

    // --- 内存池（所有节点共享 splitter 的 arrow_pool_）---
    OmniMemoryPoolAdapter* pool = nullptr;

    // --- 预分配行容量上限（通常 = options_.buffer_size）---
    int32_t rowCapacity = 0;

    // --- 方法 ---
    void Init(const DataTypePtr& dataType, int32_t bufferSize, OmniMemoryPoolAdapter* arrowPool);
    void EnsureOffsetsCapacity(int64_t needEntries);
    void EnsureValidityCapacity(int64_t needBits);
    void EnsureValuesCapacity(int64_t needBytes);
    void AppendValidBit(bool isValid);
    void CollectBuffers(std::vector<std::shared_ptr<arrow::Buffer>>& out);
    void CollectEmptyBuffers(std::vector<std::shared_ptr<arrow::Buffer>>& out, int32_t numRows);
    void Reset();
    void Release();
};

class Splitter {
    virtual int DoSplit(VectorBatch& vb);

    int WriteDataFileArrow();

    int WriteDataFileArrowByRow();

    std::shared_ptr<Buffer> CaculateSpilledTmpFilePartitionOffsets();

    int SplitComplexColumns(VectorBatch& vb);

    // --- 方案 C: 复杂类型增量直接写入 accumulator (替代 Serialize*ToArrow + MergeArrowBufferLists) ---
    void AppendColumnToArrow(BaseVector *vector, std::vector<uint32_t> row_ids,
                             DataTypePtr dataType, ComplexColumnAccumulator& acc);
    void AppendFlatToArrow(BaseVector *vector, std::vector<uint32_t> row_ids,
                           ComplexColumnAccumulator& acc);
    void AppendStringToArrow(BaseVector *vector, std::vector<uint32_t> row_ids,
                             ComplexColumnAccumulator& acc);
    void AppendArrayToArrow(BaseVector *vector, std::vector<uint32_t> row_ids,
                            DataTypePtr dataType, ComplexColumnAccumulator& acc);
    void AppendMapToArrow(BaseVector *vector, std::vector<uint32_t> row_ids,
                          DataTypePtr dataType, ComplexColumnAccumulator& acc);
    void AppendRowToArrow(BaseVector *vector, std::vector<uint32_t> row_ids,
                          DataTypePtr dataType, ComplexColumnAccumulator& acc);

    // 将 Omni null bytes (byte!=0=null) 转为 Arrow validity bitmap (bit=1=valid, 取反)
    std::shared_ptr<arrow::Buffer> OmniNullsToArrowBitmap(
        const uint8_t* nullBytes, int32_t numRows);

    int ComputeAndCountPartitionId(VectorBatch& vb);

    int AllocatePartitionBuffers(int32_t partition_id, int32_t new_size);

    int SplitFixedWidthValueBuffer(VectorBatch& vb);

    int SplitFixedWidthValidityBuffer(VectorBatch& vb);

    int SplitBinaryArray(VectorBatch& vb);

    template<bool HasNull>
    void SplitBinaryVector(BaseVector *varcharVector, int col_schema);

    int CacheVectorBatch(int32_t partition_id, bool reset_buffers);

    void ToSplitterTypeId(int num_cols);

    void CastOmniToShuffleType(DataTypeId omniType, ShuffleTypeId shuffleType);

    void MergeSpilled();

    void MergeSpilledByRow();

    // Task 15: mmap 零拷贝透传临时文件段（行/列共用），消除 C8 拷贝
    void TransferSpilledSegments(ArrowOutputStream& out,
                                 const std::string& tmpDataFilePath,
                                 uint64_t partitionOffset,
                                 uint64_t partitionSize);

    void WriteSplit();

    void WriteSplitByRow();

    void WriteSplitRss();

    int SpillToRss();

    int32_t PushColumnarPartitionToRss(int32_t pid);

    void WriteSplitRssByRow();

    int SpillToRssByRow();

    int32_t PushRowPartitionToRss(int32_t pid);

    ArrowFileHeader BuildColumnarHeader();

    ArrowFileHeader BuildRowHeader();

    // Common structures for row formats and col formats
    bool isSpill = false;
    bool rss_mode_ = false;
    std::shared_ptr<OmniRssPushClient> rss_push_client_;
    int64_t total_push_time_ = 0;
    int64_t total_bytes_written_ = 0;
    int64_t total_bytes_spilled_ = 0;
    int64_t total_write_time_ = 0;
    int64_t total_spill_time_ = 0;
    int64_t total_spill_row_num_ = 0;

    // configured local dirs for spilled file
    int32_t dir_selection_ = 0;
    std::vector<int32_t> sub_dir_selection_;
    std::vector<std::string> configured_dirs_;

    // Task 15: 临时文件头部大小（用于修正 CaculateSpilledTmpFilePartitionOffsets 的起始偏移）
    int64_t spill_file_header_size_ = 0;

    // Data structures required to handle col formats
    int64_t total_compute_pid_time_ = 0;
    std::vector<int64_t> partition_lengths_;
    std::vector<int32_t> partition_id_; // 记录当前vb每一行的pid
    int32_t *partition_id_cnt_cur_; // 统计不同partition记录的行数(当前处理中的vb)
    uint64_t *partition_id_cnt_cache_; // 统计不同partition记录的行数，cache住的
    std::vector<uint32_t> row_offset_row_id_;
    std::vector<uint32_t> partition_used_;
    std::vector<uint32_t> partition_row_offset_base_;
    std::vector<SimpleArenaAllocator> partition_arena_;
    // column number
    uint32_t num_row_splited_; // cached row number
    uint64_t cached_vectorbatch_size_; // cache total vectorbatch size in bytes
    uint64_t current_fixed_alloc_buffer_size_ = 0;
    uint32_t *fixed_valueBuffer_size_; // 当前定长omniAlloc已经分配value内存大小byte
    uint32_t *fixed_nullBuffer_size_; // 当前定长omniAlloc已分配null内存大小byte
    // int32_t num_cache_vector_;
    std::vector<ShuffleTypeId> column_type_id_; // 各列映射SHUFFLE类型，schema列id序列
    std::vector<std::vector<uint8_t*>> partition_fixed_width_validity_addrs_;
    std::vector<std::vector<uint8_t*>> partition_fixed_width_value_addrs_; //
    std::vector<std::vector<std::vector<std::shared_ptr<Buffer>>>> partition_fixed_width_buffers_;
    std::vector<std::vector<std::shared_ptr<Buffer>>> partition_binary_builders_;
    std::vector<int32_t> fixed_width_array_idx_; // 记录各定长类型列的序号，VB 列id序列
    std::vector<int32_t> binary_array_idx_; //记录各变长类型列序号
    std::vector<int32_t> complex_type_array_idx_; // 记录各复杂类型列序号 (array, map, struct)
    int32_t *partition_buffer_size_; // 各分区的buffer大小
    int32_t *partition_buffer_idx_base_; //当前已缓存的各partition行数据记录，用于定位缓冲buffer当前可用位置
    int32_t *partition_buffer_idx_offset_; //split定长列时用于统计offset的临时变量
    uint32_t *partition_serialization_size_; // 记录序列化后的各partition大小，用于stop返回partition偏移 in bytes
    /*
     * varchar buffers:
     *  partition_array_buffers_[partition_id][col_id][varcharBatch_id]
     * 
     */
    std::vector<std::vector<std::vector<VCBatchInfo>>> vc_partition_array_buffers_;

    // --- Arrow 化新增成员 (Task 9: 定宽列散列改 Arrow ResizableBuffer) ---
    std::shared_ptr<OmniMemoryPoolAdapter> arrow_pool_;                            // Arrow 内存统一记账适配器
    std::vector<std::vector<std::shared_ptr<arrow::ResizableBuffer>>> partition_fixed_width_arrow_buffers_; // [col][pid] 定宽列 Arrow 值缓冲
    std::vector<std::vector<ArrowColumnarCachedBatch>> partition_arrow_batch_;     // [pid][batchIdx] Arrow 化缓存批

    // --- 方案 C: 复杂类型列在每个分区的增量累积器 ---
    // [partition_id][complex_col_idx] → unique_ptr<ComplexColumnAccumulator>
    std::vector<std::vector<std::unique_ptr<ComplexColumnAccumulator>>> partition_complex_accumulators_;

    // Data structures required to handle row formats
    std::vector<std::vector<RowInfo *>> partition_rows; // pid : std::vector<row>
    RowBatch *array_partition_rows;
    std::vector<std::vector<RowBatch *>> partition_row_batch;
    std::vector<uint32_t> partition_row_batch_count;
    uint64_t total_input_size = 0; // total row size in bytes
    uint32_t expansion = 2; // expansion coefficient

    std::vector<DataTypePtr> inputDataTypes_;

private:
    void BuildPartition2Row(int32_t row_count);

    void ReleaseVarcharVector()
    {
        std::set<BaseVector *>::iterator it;
        for (it = varcharVectorCache.begin(); it != varcharVectorCache.end(); it++) {
            delete *it;
        }
        varcharVectorCache.clear();
    }

    void ReleaseVectorBatch(VectorBatch *vb)
    {
        int vectorCnt = vb->GetVectorCount();
        std::set<BaseVector *> vectorAddress; // vector deduplication
        for (int vecIndex = 0; vecIndex < vectorCnt; vecIndex++) {
            BaseVector *vector = vb->Get(vecIndex);
            // not varchar vector can be released;
            if (varcharVectorCache.find(vector) == varcharVectorCache.end() &&
                vectorAddress.find(vector) == vectorAddress.end()) {
                vectorAddress.insert(vector);
                delete vector;
            }
        }
        vectorAddress.clear();
        vb->ClearVectors();
        delete vb;
    }



    // Data structures required to handle col formats
    std::set<BaseVector *> varcharVectorCache;
public:
    // Common structures for row formats and col formats
    bool singlePartitionFlag = false;
    int32_t num_partitions_;
    SplitOptions options_;
    // 分区数
    int32_t num_fields_;
    InputDataTypes input_col_types;
    omniruntime::vec::VectorBatch *inputVecBatch = nullptr;
    std::map<std::string, std::shared_ptr<Buffer>> spilled_tmp_files_info_;

    virtual int Split_Init();

    virtual int Split(VectorBatch& vb);

    virtual int SplitByRow(VectorBatch* vb);

    int Stop();

    int StopByRow();

    int SpillToTmpFile();

    int SpillToTmpFileByRow();

    Splitter(InputDataTypes inputDataTypes,
             int32_t num_cols,
             int32_t num_partitions,
             SplitOptions options,
             bool flag);

    static Splitter *Make(
            const std::string &short_name,
            InputDataTypes inputDataTypes,
            int32_t num_cols,
            int num_partitions,
            SplitOptions options); 
    
    std::string NextSpilledFileDir();

    int DeleteSpilledTmpFile();

    int64_t TotalBytesWritten() const { return total_bytes_written_; }

    int64_t TotalBytesSpilled() const { return total_bytes_spilled_; }

    // Testing helper: force spill and set isSpill flag so Stop() calls MergeSpilled()
    void TestForceSpill();

    int64_t TotalWriteTime() const { return total_write_time_; }

    int64_t TotalPushTime() const { return total_push_time_; }

    bool IsRssMode() const { return rss_mode_; }

    void SetRssPushClient(std::shared_ptr<OmniRssPushClient> client);

    int64_t TotalSpillTime() const { return total_spill_time_; }

    int64_t TotalComputePidTime() const { return total_compute_pid_time_; }

    // Arrow 化缓存批访问器 (Task 9)
    int64_t TotalCachedArrowRows() const {
        int64_t sum = 0;
        for (const auto& batches : partition_arrow_batch_)
            for (const auto& b : batches) sum += b.rowCount;
        return sum;
    }
    const std::vector<std::vector<ArrowColumnarCachedBatch>>& ArrowCachedBatches() const {
        return partition_arrow_batch_;
    }

    const std::vector<int64_t>& PartitionLengths() const { return partition_lengths_; }

    virtual ~Splitter()
    {
	delete[] partition_id_cnt_cur_;
	delete[] partition_id_cnt_cache_;
	delete[] partition_buffer_size_;
	delete[] partition_buffer_idx_base_;
	delete[] partition_buffer_idx_offset_;
	delete[] partition_serialization_size_;
	delete[] fixed_valueBuffer_size_;
	delete[] fixed_nullBuffer_size_;
	partition_fixed_width_buffers_.clear();
	partition_binary_builders_.clear();
	spilled_tmp_files_info_.clear();
    }

    omniruntime::vec::VectorBatch *GetInputVecBatch()
    {
        return inputVecBatch;
    }

    void SetInputVecBatch(omniruntime::vec::VectorBatch *inVecBatch)
    {
        inputVecBatch = inVecBatch;
    }

    void SetInputDataTypes(std::vector<DataTypePtr>& inputDataTypes)
    {
        inputDataTypes_ = inputDataTypes;
    }

    // no need to clear memory when exception, so we have to reset
    void ResetInputVecBatch()
    {
        inputVecBatch = nullptr;
    }
};


#endif // CPP_SPLITTER_H
