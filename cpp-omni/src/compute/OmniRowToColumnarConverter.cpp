/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

#include "OmniRowToColumnarConverter.h"

#include <nlohmann/json.hpp>

#include "util/omni_exception.h"
#include "vector/string_view.h"
#include "vector/vector_helper.h"

namespace gluten {
using omniruntime::type::DataTypeId;
using omniruntime::type::OMNI_BOOLEAN;
using omniruntime::type::OMNI_BYTE;
using omniruntime::type::OMNI_DOUBLE;
using omniruntime::type::OMNI_FLOAT;
using omniruntime::type::OMNI_INT;
using omniruntime::type::OMNI_LONG;
using omniruntime::type::OMNI_SHORT;
using omniruntime::type::OMNI_STRING_VIEW;
using omniruntime::type::OMNI_VARCHAR;
using omniruntime::vec::BaseVector;
using omniruntime::vec::LargeStringContainer;
using omniruntime::vec::StringView;
using omniruntime::vec::Vector;

namespace {
DataTypeId ParseStringOmniType(const nlohmann::json &field)
{
    if (!field.contains("omniType")) {
        return OMNI_STRING_VIEW;
    }

    const auto omniType = field["omniType"].get<std::string>();
    if (omniType == "string_view" || omniType == "OMNI_STRING_VIEW") {
        return OMNI_STRING_VIEW;
    }
    if (omniType == "varchar" || omniType == "OMNI_VARCHAR") {
        return OMNI_VARCHAR;
    }

    throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR",
        "Unsupported Omni string type for native row-to-columnar: " + omniType);
}

DataTypeId SparkTypeToOmniType(const nlohmann::json &typeNode)
{
    if (!typeNode.is_string()) {
        throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR",
            "Omni native row-to-columnar currently supports only flat primitive Spark types");
    }

    const auto typeName = typeNode.get<std::string>();
    if (typeName == "boolean") {
        return OMNI_BOOLEAN;
    }
    if (typeName == "byte") {
        return OMNI_BYTE;
    }
    if (typeName == "short") {
        return OMNI_SHORT;
    }
    if (typeName == "integer" || typeName == "date") {
        return OMNI_INT;
    }
    if (typeName == "long" || typeName == "timestamp") {
        return OMNI_LONG;
    }
    if (typeName == "float") {
        return OMNI_FLOAT;
    }
    if (typeName == "double") {
        return OMNI_DOUBLE;
    }
    if (typeName == "string") {
        return OMNI_STRING_VIEW;
    }

    throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR",
        "Unsupported Spark type for Omni native row-to-columnar: " + typeName);
}

template <typename T>
void FillPrimitiveColumn(BaseVector *vector, int32_t columnIdx, int64_t fieldOffset,
    const std::vector<int64_t> &rowOffsets, uint8_t *memoryAddress)
{
    auto *typedVector = reinterpret_cast<Vector<T> *>(vector);
    for (int32_t pos = 0; pos < static_cast<int32_t>(rowOffsets.size()); ++pos) {
        auto *rowAddress = memoryAddress + rowOffsets[pos];
        if (OmniRowToColumnarConverter::IsNull(rowAddress, columnIdx)) {
            typedVector->SetNull(pos);
            continue;
        }
        typedVector->SetValue(pos, *reinterpret_cast<T *>(rowAddress + fieldOffset));
    }
}

std::string_view ReadUnsafeRowString(uint8_t *rowAddress, int64_t fieldOffset)
{
    int64_t offsetAndSize = *reinterpret_cast<int64_t *>(rowAddress + fieldOffset);
    int32_t length = static_cast<int32_t>(offsetAndSize);
    int32_t wordOffset = static_cast<int32_t>(offsetAndSize >> 32);
    return {reinterpret_cast<char *>(rowAddress + wordOffset), static_cast<size_t>(length)};
}
} // namespace

OmniRowToColumnarConverter::OmniRowToColumnarConverter(const std::string &schemaJson)
    : fieldTypes_(ParseSchema(schemaJson))
{
}

omniruntime::vec::VectorBatch *OmniRowToColumnarConverter::Convert(
    int64_t numRows, int64_t *rowLength, uint8_t *memoryAddress) const
{
    const auto numFields = static_cast<int32_t>(fieldTypes_.size());
    const int64_t nullBitsetWidthInBytes = CalculateBitSetWidthInBytes(numFields);

    std::vector<int64_t> rowOffsets(numRows);
    for (int64_t i = 1; i < numRows; ++i) {
        rowOffsets[i] = rowOffsets[i - 1] + rowLength[i - 1];
    }

    auto *batch = new omniruntime::vec::VectorBatch(static_cast<size_t>(numRows));
    try {
        for (int32_t columnIdx = 0; columnIdx < numFields; ++columnIdx) {
            const auto typeId = fieldTypes_[columnIdx];
            auto *vector = omniruntime::vec::VectorHelper::CreateFlatVector(typeId, static_cast<int32_t>(numRows));
            FillColumn(vector, typeId, columnIdx, GetFieldOffset(nullBitsetWidthInBytes, columnIdx),
                rowOffsets, memoryAddress);
            batch->Append(vector);
        }
    } catch (...) {
        delete batch;
        throw;
    }
    return batch;
}

std::vector<DataTypeId> OmniRowToColumnarConverter::ParseSchema(const std::string &schemaJson)
{
    const auto schema = nlohmann::json::parse(schemaJson);
    if (!schema.contains("fields") || !schema["fields"].is_array()) {
        throw omniruntime::exception::OmniException("INVALID_ARGUMENT",
            "Spark schema JSON does not contain a fields array");
    }

    std::vector<DataTypeId> fieldTypes;
    for (const auto &field : schema["fields"]) {
        if (!field.contains("type")) {
            throw omniruntime::exception::OmniException("INVALID_ARGUMENT",
                "Spark schema field does not contain a type");
        }
        auto typeId = SparkTypeToOmniType(field["type"]);
        if (typeId == OMNI_STRING_VIEW) {
            typeId = ParseStringOmniType(field);
        }
        fieldTypes.emplace_back(typeId);
    }
    return fieldTypes;
}

int64_t OmniRowToColumnarConverter::CalculateBitSetWidthInBytes(int32_t numFields)
{
    return ((numFields + 63) / 64) * 8;
}

int64_t OmniRowToColumnarConverter::GetFieldOffset(int64_t nullBitsetWidthInBytes, int32_t index)
{
    return nullBitsetWidthInBytes + 8L * index;
}

bool OmniRowToColumnarConverter::IsNull(uint8_t *rowAddress, int32_t index)
{
    int64_t mask = 1L << (index & 0x3f);
    int64_t wordOffset = (index >> 6) * 8;
    int64_t value = *reinterpret_cast<int64_t *>(rowAddress + wordOffset);
    return (value & mask) != 0;
}

void OmniRowToColumnarConverter::FillColumn(BaseVector *vector,
    DataTypeId typeId,
    int32_t columnIdx,
    int64_t fieldOffset,
    const std::vector<int64_t> &rowOffsets,
    uint8_t *memoryAddress)
{
    switch (typeId) {
        case OMNI_BOOLEAN:
            FillPrimitiveColumn<bool>(vector, columnIdx, fieldOffset, rowOffsets, memoryAddress);
            return;
        case OMNI_BYTE:
            FillPrimitiveColumn<int8_t>(vector, columnIdx, fieldOffset, rowOffsets, memoryAddress);
            return;
        case OMNI_SHORT:
            FillPrimitiveColumn<int16_t>(vector, columnIdx, fieldOffset, rowOffsets, memoryAddress);
            return;
        case OMNI_INT:
            FillPrimitiveColumn<int32_t>(vector, columnIdx, fieldOffset, rowOffsets, memoryAddress);
            return;
        case OMNI_LONG:
            FillPrimitiveColumn<int64_t>(vector, columnIdx, fieldOffset, rowOffsets, memoryAddress);
            return;
        case OMNI_FLOAT:
            FillPrimitiveColumn<float>(vector, columnIdx, fieldOffset, rowOffsets, memoryAddress);
            return;
        case OMNI_DOUBLE:
            FillPrimitiveColumn<double>(vector, columnIdx, fieldOffset, rowOffsets, memoryAddress);
            return;
        case OMNI_VARCHAR: {
            auto *typedVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
            for (int32_t pos = 0; pos < static_cast<int32_t>(rowOffsets.size()); ++pos) {
                auto *rowAddress = memoryAddress + rowOffsets[pos];
                if (IsNull(rowAddress, columnIdx)) {
                    typedVector->SetNull(pos);
                    continue;
                }
                typedVector->SetValue(pos, ReadUnsafeRowString(rowAddress, fieldOffset));
            }
            return;
        }
        case OMNI_STRING_VIEW: {
            auto *typedVector = reinterpret_cast<Vector<StringView> *>(vector);
            for (int32_t pos = 0; pos < static_cast<int32_t>(rowOffsets.size()); ++pos) {
                auto *rowAddress = memoryAddress + rowOffsets[pos];
                if (IsNull(rowAddress, columnIdx)) {
                    typedVector->SetNull(pos);
                    continue;
                }
                const auto value = ReadUnsafeRowString(rowAddress, fieldOffset);
                typedVector->SetValue(pos, StringView(value));
            }
            return;
        }
        default:
            throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR",
                "Unsupported Omni type for native row-to-columnar converter");
    }
}

} // namespace gluten
