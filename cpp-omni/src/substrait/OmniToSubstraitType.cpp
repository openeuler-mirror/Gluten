/*
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

#include "OmniToSubstraitType.h"

namespace omniruntime {
const ::substrait::Type &OmniToSubstraitTypeConvertor::toSubstraitType(google::protobuf::Arena &arena,
    const DataTypePtr &type)
{
    auto *substraitType = google::protobuf::Arena::CreateMessage<::substrait::Type>(&arena);
    if (type->GetId() == OMNI_DATE32 || type->GetId() == OMNI_DATE64) {
        auto substraitDate = google::protobuf::Arena::CreateMessage<::substrait::Type_Date>(&arena);
        substraitDate->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
        substraitType->set_allocated_date(substraitDate);
        return *substraitType;
    }

    switch (type->GetId()) {
        case OMNI_BOOLEAN: {
            auto substraitBool = google::protobuf::Arena::CreateMessage<::substrait::Type_Boolean>(&arena);
            substraitBool->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
            substraitType->set_allocated_bool_(substraitBool);
            break;
        }
        case OMNI_BYTE: {
            auto substraitI8 = google::protobuf::Arena::CreateMessage<::substrait::Type_I8>(&arena);
            substraitI8->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
            substraitType->set_allocated_i8(substraitI8);
            break;
        }
        case OMNI_SHORT: {
            auto substraitI16 = google::protobuf::Arena::CreateMessage<::substrait::Type_I16>(&arena);
            substraitI16->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
            substraitType->set_allocated_i16(substraitI16);
            break;
        }
        case OMNI_INT: {
            auto substraitI32 = google::protobuf::Arena::CreateMessage<::substrait::Type_I32>(&arena);
            substraitI32->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
            substraitType->set_allocated_i32(substraitI32);
            break;
        }
        case OMNI_LONG: {
            auto substraitI64 = google::protobuf::Arena::CreateMessage<::substrait::Type_I64>(&arena);
            substraitI64->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
            substraitType->set_allocated_i64(substraitI64);
            break;
        }
        case OMNI_FLOAT: {
            auto substraitFp32 = google::protobuf::Arena::CreateMessage<::substrait::Type_FP32>(&arena);
            substraitFp32->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
            substraitType->set_allocated_fp32(substraitFp32);
            break;
        }
        case OMNI_DOUBLE: {
            auto substraitFp64 = google::protobuf::Arena::CreateMessage<::substrait::Type_FP64>(&arena);
            substraitFp64->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
            substraitType->set_allocated_fp64(substraitFp64);
            break;
        }
        case OMNI_VARCHAR: {
            auto substraitString = google::protobuf::Arena::CreateMessage<::substrait::Type_String>(&arena);
            substraitString->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
            substraitType->set_allocated_string(substraitString);
            break;
        }
        case OMNI_VARBINARY: {
            auto substraitVarBinary = google::protobuf::Arena::CreateMessage<::substrait::Type_Binary>(&arena);
            substraitVarBinary->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
            substraitType->set_allocated_binary(substraitVarBinary);
            break;
        }
        case OMNI_TIMESTAMP: {
            auto substraitTimestamp = google::protobuf::Arena::CreateMessage<::substrait::Type_Timestamp>(&arena);
            substraitTimestamp->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
            substraitType->set_allocated_timestamp(substraitTimestamp);
            break;
        }
        case OMNI_ARRAY: {
            auto *substraitList = google::protobuf::Arena::CreateMessage<::substrait::Type_List>(&arena);
            auto arrayType = std::dynamic_pointer_cast<ArrayType>(type);
            substraitList->mutable_type()->MergeFrom(toSubstraitType(arena, arrayType->ElementType()));
            substraitList->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
            substraitType->set_allocated_list(substraitList);
            break;
        }
        case OMNI_MAP: {
            auto *substraitMap = google::protobuf::Arena::CreateMessage<::substrait::Type_Map>(&arena);
            auto mapType = std::dynamic_pointer_cast<MapType>(type);
            substraitMap->mutable_key()->MergeFrom(toSubstraitType(arena, mapType->Key()));
            substraitMap->mutable_value()->MergeFrom(toSubstraitType(arena, mapType->Value()));
            substraitMap->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
            substraitType->set_allocated_map(substraitMap);
            break;
        }
        case OMNI_ROW: {
            auto *substraitStruct = google::protobuf::Arena::CreateMessage<::substrait::Type_Struct>(&arena);
            auto rowType = std::dynamic_pointer_cast<RowType>(type);
            for (const auto &child : rowType->Children()) {
                substraitStruct->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
                substraitStruct->add_types()->MergeFrom(toSubstraitType(arena, child));
            }
            substraitStruct->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
            substraitType->set_allocated_struct_(substraitStruct);
            break;
        }
        case OMNI_UNKNOWN: {
            auto substraitUserDefined = google::protobuf::Arena::CreateMessage<::substrait::Type_UserDefined>(&arena);
            substraitUserDefined->set_type_reference(0);
            substraitUserDefined->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
            substraitType->set_allocated_user_defined(substraitUserDefined);
            break;
        }
        case OMNI_OPAQUE:
        case OMNI_INVALID:
        default:
            OMNI_THROW("Runtime error:", "Unsupported omni type id {}", static_cast<int>(type->GetId()));
    }
    return *substraitType;
}
} // namespace gluten
