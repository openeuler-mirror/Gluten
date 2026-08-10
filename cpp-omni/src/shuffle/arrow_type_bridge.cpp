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

#include "shuffle/arrow_type_bridge.h"
#include <stdexcept>
#include <string>

using namespace omniruntime::type;

OmniPhysicalType PhysicalTypeOf(const OmniTypeDescriptor& d)
{
    switch (d.typeId) {
        case OMNI_BYTE:       return OmniPhysicalType::INT8;
        case OMNI_BOOLEAN:    return OmniPhysicalType::BOOL;
        case OMNI_SHORT:      return OmniPhysicalType::INT16;
        case OMNI_INT:        return OmniPhysicalType::INT32;
        case OMNI_LONG:       return OmniPhysicalType::INT64;
        case OMNI_TIMESTAMP:  return OmniPhysicalType::INT64;
        case OMNI_DOUBLE:     return OmniPhysicalType::INT64;  // 8B
        case OMNI_FLOAT:      return OmniPhysicalType::INT32;  // 4B
        case OMNI_DATE32:     return OmniPhysicalType::INT32;
        case OMNI_DECIMAL64:  return OmniPhysicalType::DECIMAL64;   // 独立，不退化为 INT64
        case OMNI_DECIMAL128: return OmniPhysicalType::DECIMAL128;
        case OMNI_CHAR:
        case OMNI_VARCHAR:
        case OMNI_VARBINARY:  return OmniPhysicalType::BINARY;
        case OMNI_ARRAY:      return OmniPhysicalType::LIST;
        case OMNI_MAP:        return OmniPhysicalType::MAP;
        case OMNI_ROW:        return OmniPhysicalType::STRUCT;
        case OMNI_NONE:       return OmniPhysicalType::NULL_;
        default:
            throw std::runtime_error("arrow_type_bridge: unsupported omni typeId in descriptor: " + std::to_string(d.typeId));
    }
}

int32_t PhysicalTypeByteWidth(OmniPhysicalType t)
{
    switch (t) {
        case OmniPhysicalType::INT8:      return 1;
        case OmniPhysicalType::INT16:     return 2;
        case OmniPhysicalType::INT32:     return 4;
        case OmniPhysicalType::INT64:     return 8;
        case OmniPhysicalType::DECIMAL64: return 8;     // 8B 直存
        case OmniPhysicalType::DECIMAL128:return 16;
        case OmniPhysicalType::BOOL:      return 1;     // 物理上按 bitmap，定宽按 1B 占位计
        case OmniPhysicalType::BINARY:
        case OmniPhysicalType::LARGE_BINARY:
        case OmniPhysicalType::LIST:
        case OmniPhysicalType::MAP:
        case OmniPhysicalType::STRUCT:
        case OmniPhysicalType::NULL_:     return 0;     // 变长/复杂
    }
    return 0;
}

OmniTypeDescriptor DataTypeToDescriptor(const DataTypePtr& dt)
{
    OmniTypeDescriptor d;
    d.typeId = dt->GetId();
    if (d.typeId == OMNI_DECIMAL64 || d.typeId == OMNI_DECIMAL128) {
        auto dec = std::dynamic_pointer_cast<DecimalDataType>(dt);
        d.precision = static_cast<int32_t>(dec->GetPrecision());
        d.scale = static_cast<int32_t>(dec->GetScale());
    }
    switch (d.typeId) {
        case OMNI_ARRAY: {
            auto arr = std::dynamic_pointer_cast<ArrayType>(dt);
            d.children.push_back(DataTypeToDescriptor(arr->ElementType()));
            break;
        }
        case OMNI_MAP: {
            auto mp = std::dynamic_pointer_cast<MapType>(dt);
            d.children.push_back(DataTypeToDescriptor(mp->Key()));
            d.children.push_back(DataTypeToDescriptor(mp->Value()));
            break;
        }
        case OMNI_ROW: {
            auto row = std::dynamic_pointer_cast<RowType>(dt);
            for (uint32_t c = 0; c < row->size(); ++c) {
                d.children.push_back(DataTypeToDescriptor(row->childAt(c)));
            }
            break;
        }
        default:
            break;  // 标量：无 children
    }
    d.numChildren = static_cast<int32_t>(d.children.size());
    return d;
}

std::shared_ptr<omniruntime::type::DataType> DescriptorToOmniType(const OmniTypeDescriptor& d)
{
    // 按 typeId 还原 omniruntime::type::DataType（唯一实现，供读侧三处入口共用）。
    switch (d.typeId) {
        case OMNI_BYTE:       return ByteDataType::Instance();
        case OMNI_SHORT:      return ShortDataType::Instance();
        case OMNI_INT:        return IntDataType::Instance();
        case OMNI_LONG:       return LongDataType::Instance();
        case OMNI_FLOAT:      return FloatDataType::Instance();
        case OMNI_DOUBLE:     return DoubleDataType::Instance();
        case OMNI_BOOLEAN:    return BooleanDataType::Instance();
        case OMNI_DATE32:     return Date32DataType::Instance();
        case OMNI_DATE64:     return Date64DataType::Instance();
        case OMNI_TIMESTAMP:  return TimestampDataType::Instance();
        case OMNI_VARCHAR:    return VarcharDataType::Instance();
        case OMNI_CHAR:       return CharDataType::Instance();
        case OMNI_VARBINARY:  return VarBinaryDataType::Instance();
        case OMNI_DECIMAL64:  return std::make_shared<Decimal64DataType>(d.precision, d.scale);
        case OMNI_DECIMAL128: return std::make_shared<Decimal128DataType>(d.precision, d.scale);
        case OMNI_ARRAY: {
            if (d.children.empty()) {
                throw std::runtime_error("DescriptorToOmniType: ARRAY has no children");
            }
            auto elementType = DescriptorToOmniType(d.children.at(0));
            return std::make_shared<ArrayType>(elementType);
        }
        case OMNI_MAP: {
            if (d.children.size() < 2) {
                throw std::runtime_error("DescriptorToOmniType: MAP needs key+value children");
            }
            auto keyType = DescriptorToOmniType(d.children.at(0));
            auto valueType = DescriptorToOmniType(d.children.at(1));
            return std::make_shared<MapType>(keyType, valueType);
        }
        case OMNI_ROW: {
            std::vector<std::shared_ptr<omniruntime::type::DataType>> childTypes;
            for (const auto& c : d.children) childTypes.push_back(DescriptorToOmniType(c));
            return std::make_shared<RowType>(childTypes);
        }
        case OMNI_NONE:
            return NoneDataType::Instance();
        default:
            throw std::runtime_error("DescriptorToOmniType: unsupported typeId " + std::to_string(d.typeId));
    }
}

int NumBuffers(const OmniTypeDescriptor& d)
{
    auto pt = PhysicalTypeOf(d);
    switch (pt) {
        case OmniPhysicalType::INT8: case OmniPhysicalType::INT16:
        case OmniPhysicalType::INT32: case OmniPhysicalType::INT64:
        case OmniPhysicalType::DECIMAL64: case OmniPhysicalType::DECIMAL128:
        case OmniPhysicalType::BOOL:
            return 2;   // validity + values
        case OmniPhysicalType::BINARY:
        case OmniPhysicalType::LARGE_BINARY:
            return 3;   // validity + offsets + values
        case OmniPhysicalType::LIST: {
            // validity + offsets + child 的 buffers
            int n = 2;
            for (const auto& c : d.children) n += NumBuffers(c);
            return n;
        }
        case OmniPhysicalType::MAP: {
            int n = 2;  // validity + offsets
            for (const auto& c : d.children) n += NumBuffers(c);
            return n;
        }
        case OmniPhysicalType::STRUCT: {
            int n = 1;  // 仅 validity
            for (const auto& c : d.children) n += NumBuffers(c);
            return n;
        }
        case OmniPhysicalType::NULL_:
            return 0;
    }
    return 0;
}
