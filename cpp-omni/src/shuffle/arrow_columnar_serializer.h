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

#ifndef CPP_ARROW_COLUMNAR_SERIALIZER_H
#define CPP_ARROW_COLUMNAR_SERIALIZER_H

#include <cstdint>
#include <vector>
#include "io/ArrowOutputStream.h"
#include "shuffle/arrow_frame.h"
#include "shuffle/splitter.h"

// Arrow 列式序列化写出：遍历 partitionArrowBatch[partition_id] 每帧写文件头（首次）+ 逐帧 WriteColumnarBatch，
// 逐 buffer 顺序 arrowOut->Write，返回写出字节数。
//
// @param partition_id         目标分区编号
// @param out                  Arrow 文件输出流（已选压缩/非压缩模式）
// @param header               文件头（含版本、layout、schema）
// @param partitionArrowBatch  [pid][batchIdx] 的 ArrowColumnarCachedBatch 列表
// @param headerAlreadyWritten 若为 true，跳过文件头写出（同一文件多分区连续写时复用）
// @return                     本次写出字节数（不含 headerAlreadyWritten=true 时跳过的文件头）
int32_t ArrowWriteColumnarPartition(int32_t partition_id,
                                    ArrowOutputStream& out,
                                    const ArrowFileHeader& header,
                                    const std::vector<std::vector<ArrowColumnarCachedBatch>>& partitionArrowBatch,
                                    bool headerAlreadyWritten);

#endif // CPP_ARROW_COLUMNAR_SERIALIZER_H
