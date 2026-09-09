/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "type/data_type.h"
#include "vector/vector_batch.h"

namespace gluten {

class OmniRowToColumnarConverter {
public:
    explicit OmniRowToColumnarConverter(const std::string &schemaJson);

    omniruntime::vec::VectorBatch *Convert(int64_t numRows, int64_t *rowLength, uint8_t *memoryAddress) const;

    static bool IsNull(uint8_t *rowAddress, int32_t index);

private:
    std::vector<omniruntime::type::DataTypeId> fieldTypes_;

    static std::vector<omniruntime::type::DataTypeId> ParseSchema(const std::string &schemaJson);

    static int64_t CalculateBitSetWidthInBytes(int32_t numFields);

    static int64_t GetFieldOffset(int64_t nullBitsetWidthInBytes, int32_t index);

    static void FillColumn(omniruntime::vec::BaseVector *vector,
        omniruntime::type::DataTypeId typeId,
        int32_t columnIdx,
        int64_t fieldOffset,
        const std::vector<int64_t> &rowOffsets,
        uint8_t *memoryAddress);
};

} // namespace gluten
