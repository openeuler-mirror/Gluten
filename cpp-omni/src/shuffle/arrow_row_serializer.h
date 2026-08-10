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

#ifndef CPP_ARROW_ROW_SERIALIZER_H
#define CPP_ARROW_ROW_SERIALIZER_H

#include <cstdint>
#include <vector>
#include "io/ArrowOutputStream.h"
#include "shuffle/arrow_frame.h"
#include "shuffle/omni_arrow_memory_pool.h"
#include "shuffle/splitter.h"

// Arrow 行式序列化写出：遍历 partitionRows[partition_id]，按 spillBatchRowNum 分批，
// 每批用 BufferBuilder 拼接连续行段 + Copy offsets → WriteRowBatch 序列化帧，
// 通过 arrowOut->Write 顺序写出。首次写文件头（headerAlreadyWritten=false）。
//
// @param partition_id         目标分区编号
// @param out                  Arrow 文件输出流（已选压缩/非压缩模式）
// @param header               文件头（含版本、layout=ROW、schema）
// @param partitionRows        [pid] 的 RowInfo* 列表（const 引用，调用方负责清理）
// @param spillBatchRowNum     每批最大行数（分批阈值）
// @param pool                 Arrow 内存池适配器（统一记账）
// @param headerAlreadyWritten 若为 true，跳过文件头写出（同一文件多分区连续写时复用）
// @return                     本次写出字节数（不含 headerAlreadyWritten=true 时跳过的文件头）
int32_t ArrowWriteRowPartition(int32_t partition_id,
                               ArrowOutputStream& out,
                               const ArrowFileHeader& header,
                               const std::vector<std::vector<RowInfo*>>& partitionRows,
                               uint64_t spillBatchRowNum,
                               OmniMemoryPoolAdapter& pool,
                               bool headerAlreadyWritten);

#endif // CPP_ARROW_ROW_SERIALIZER_H
