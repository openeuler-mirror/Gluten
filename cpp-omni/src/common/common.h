/*
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef CPP_COMMON_H
#define CPP_COMMON_H

#include <vector/vector_common.h>
#include <cstring>
#include <cstdint>
#include <stdexcept>
#include <chrono>
#include <memory>
#include <list>
#include <set>
#include <fstream>
#include <iostream>
#include <sys/stat.h>
#include <unistd.h>

#include "../io/Common.hh"
#include "../utils/macros.h"
#include "BinaryLocation.h"
#include "debug.h"
#include "Buffer.h"

using namespace omniruntime::vec;
using namespace omniruntime::type;

template<bool hasNull>
int32_t BytesGen(uint64_t offsetsAddr, std::string &nullStr, uint64_t valuesAddr, VCBatchInfo& vcb)
{
    int32_t* offsets = reinterpret_cast<int32_t *>(offsetsAddr);  // offsets of the string
    char *nulls = nullptr;
    char* values = reinterpret_cast<char *>(valuesAddr); // raw pointer of string data
    auto &lst = vcb.getVcList();
    int itemsTotalLen = lst.size(); // number of strings

    int64_t valueTotalLen = 0; // total length of the string (use 64-bit to avoid overflow)
    if constexpr (hasNull) {
        nullStr.resize(itemsTotalLen, 0);  // initialize nullStr only when there are null values
        nulls = nullStr.data();
    }

    offsets[0] = 0;
    for (int i = 0; i < itemsTotalLen; i++) {
        char* addr = reinterpret_cast<char *>(lst[i].get_vc_addr()); // address of the string
        int len = lst[i].get_vc_len();
        if constexpr(hasNull) {
            if (lst[i].get_is_null()) {
                nulls[i] = 1;
            }
        }

        if (len != 0) {
            memcpy((char *) (values + offsets[i]), addr, len);
            valueTotalLen += len;
        }
        // Detect int32 overflow on cumulative offset before writing offsets[i+1].
        int64_t next = static_cast<int64_t>(offsets[i]) + static_cast<int64_t>(len);
        if (next > static_cast<int64_t>(INT32_MAX)) {
            throw std::overflow_error("BytesGen: cumulative string offset exceeds INT32_MAX");
        }
        offsets[i + 1] = static_cast<int32_t>(next);
    }
    return static_cast<int32_t>(valueTotalLen);
}

uint32_t reversebytes_uint32t(uint32_t value);

spark::CompressionKind GetCompressionType(const std::string& name);

int IsFileExist(const std::string path);

#endif //CPP_COMMON_H