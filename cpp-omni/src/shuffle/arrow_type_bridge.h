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

#ifndef CPP_ARROW_TYPE_BRIDGE_H
#define CPP_ARROW_TYPE_BRIDGE_H

#include <cstdint>
#include <memory>
#include <vector>
#include "shuffle/type.h"
#include "type/data_type.h"

using omniruntime::type::DataTypePtr;

// 递归类型描述：即文件头 schema 的内容。构造期由列类型一次性构建并全程复用。
struct OmniTypeDescriptor {
    int32_t typeId = 0;          // OMNI_* DataTypeId
    int32_t precision = 0;       // DECIMAL 用
    int32_t scale = 0;           // DECIMAL 用
    int32_t numChildren = 0;
    std::vector<OmniTypeDescriptor> children;
};

// Arrow 物理布局类型。DECIMAL64 为独立类型（8B 直存），不退化为 INT64。
enum class OmniPhysicalType {
    INT8, INT16, INT32, INT64,
    DECIMAL64, DECIMAL128,
    BOOL,
    BINARY,        // VARCHAR/CHAR/VARBINARY: offsets(int32) + values
    LARGE_BINARY,  // 预留，当前不用
    LIST,          // ARRAY
    MAP,
    STRUCT,        // ROW
    NULL_
};

// 从完整的递归 DataType 树构建 schema 描述符（与 ComplexColumnAccumulator 的数据树同源）。
// 标量列产出 {typeId, precision/scale} 无 children；ARRAY/MAP/ROW 递归展开 children。
OmniTypeDescriptor DataTypeToDescriptor(const DataTypePtr& dt);
std::shared_ptr<omniruntime::type::DataType> DescriptorToOmniType(const OmniTypeDescriptor& d);

OmniPhysicalType PhysicalTypeOf(const OmniTypeDescriptor& d);
int32_t PhysicalTypeByteWidth(OmniPhysicalType t);   // 定宽字节数；变长/复杂返回 0
int NumBuffers(const OmniTypeDescriptor& d);          // Arrow 布局下该列 buffer 数（含 validity）

#endif // CPP_ARROW_TYPE_BRIDGE_H
