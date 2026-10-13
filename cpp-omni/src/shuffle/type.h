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

#ifndef CPP_TYPE_H
#define CPP_TYPE_H
#include <vector/vector_common.h>
#include "../io/SparkFile.hh"
#include "../io/ColumnWriter.hh"

using namespace spark;
using namespace omniruntime::mem;

static constexpr int32_t kDefaultSplitterBufferSize = 4096;
static constexpr int32_t kDefaultNumSubDirs = 64;
static constexpr double spillThreshold = 0.9;
// The legacy process-global memory limit query no longer exists; shuffle relies
// on Spark-side memory accounting. A per-task limit could be read from
// gluten::OmniMemoryManager if an arbitrator-aware shuffle path needs it.
inline constexpr int64_t offHeapSize = 0;

struct SplitOptions {
    int32_t buffer_size = kDefaultSplitterBufferSize;
    int32_t num_sub_dirs = kDefaultNumSubDirs;
    CompressionKind compression_type = CompressionKind_NONE;
    std::string next_spilled_file_dir = "";

    std::string data_file;

    int64_t thread_id = -1;
    int64_t task_attempt_id = -1;

    // SplitOptions is a POD-ish settings struct populated at JNI entry before the
    // splitter or its task-bound pool exists, so this IIFE-static nullBridge
    // supplies a no-pool Allocator default. Splitter construction re-binds
    // .allocator to the task pool when one is available.
    Allocator *allocator = []() -> Allocator * {
        static Allocator nullBridge{nullptr};
        return &nullBridge;
    }();

    uint64_t spill_batch_row_num = 10000; // default value
    uint64_t task_spill_mem_threshold = 2L * 1024 * 1024 * 1024; // default value
    uint64_t executor_spill_mem_threshold = spillThreshold * offHeapSize; // default value
    uint64_t compress_block_size = 64 * 1024; // default value

    bool rss_mode = false;

    static SplitOptions Defaults();
};

enum ShuffleTypeId : int {
    SHUFFLE_1BYTE = 0,
    SHUFFLE_2BYTE = 1,
    SHUFFLE_4BYTE = 2,
    SHUFFLE_8BYTE = 3,
    SHUFFLE_DECIMAL128 = 4,
    SHUFFLE_BIT = 5,
    SHUFFLE_BINARY = 6,
    SHUFFLE_LARGE_BINARY = 7,
    SHUFFLE_NULL = 8,
    NUM_TYPES = 9,
    SHUFFLE_NOT_IMPLEMENTED = 10,
    SHUFFLE_ARRAY = 11,
    SHUFFLE_MAP = 12,
    SHUFFLE_ROW = 13
};

struct InputDataTypes {
    int32_t *inputVecTypeIds = nullptr;
    uint32_t *inputDataPrecisions = nullptr;
    uint32_t *inputDataScales = nullptr;
};

#endif //CPP_TYPE_H