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

#include "arrow_test_vectors.h"

#include <arrow/buffer.h>
#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "shuffle/arrow_type_bridge.h"
#include <type/data_type.h>
#include <vector/vector_common.h>

using namespace omniruntime::vec;
using namespace omniruntime::type;

// ============================================================================
//  Internal helpers
// ============================================================================

namespace {

/// Return the fixed-width byte size for a DataTypeId, or 0 for variable-width.
int32_t TypeIdByteWidth(int32_t typeId) {
  switch (typeId) {
    case OMNI_BYTE:        return 1;
    case OMNI_BOOLEAN:     return 1;
    case OMNI_SHORT:       return 2;
    case OMNI_INT:         return 4;
    case OMNI_LONG:        return 8;
    case OMNI_DOUBLE:      return 8;
    case OMNI_FLOAT:       return 4;
    case OMNI_DATE32:      return 4;
    case OMNI_TIMESTAMP:   return 8;
    case OMNI_DECIMAL64:   return 8;
    case OMNI_DECIMAL128:  return 16;
    default:               return 0;  // variable-width or complex
  }
}

/// Check whether a DataTypeId is a string type (VARCHAR / CHAR / VARBINARY).
bool IsStringType(int32_t typeId) {
  return typeId == OMNI_VARCHAR || typeId == OMNI_CHAR || typeId == OMNI_VARBINARY;
}

/// Invert a single validity byte (Omni → Arrow): Omni set-bit=null → Arrow set-bit=valid.
inline uint8_t InvertByte(uint8_t b) { return ~b; }

/// Write an Arrow validity buffer (set-bit=valid) from Omni null bitmap
/// (set-bit=null). If all rows are valid, the output buffer is left as nullptr
/// (the all-valid sentinel).
std::shared_ptr<arrow::Buffer> BuildArrowValidity(BaseVector* vec,
                                                  int32_t rowCount) {
  // Check whether any row is null
  bool hasNull = false;
  for (int32_t i = 0; i < rowCount; ++i) {
    if (vec->IsNull(i)) {
      hasNull = true;
      break;
    }
  }
  if (!hasNull) {
    return nullptr;  // all-valid sentinel
  }

  int32_t byteCount = (rowCount + 7) / 8;
  auto* omniNulls = unsafe::UnsafeBaseVector::GetNulls(vec);
  auto buf = arrow::AllocateBuffer(byteCount);
  if (!buf.ok()) {
    throw std::runtime_error("BuildArrowValidity: Arrow buffer allocation failed");
  }
  uint8_t* dst = (*buf)->mutable_data();
  for (int32_t i = 0; i < byteCount; ++i) {
    dst[i] = InvertByte(omniNulls[i]);
  }
  // Mask trailing bits in the last byte
  int32_t rem = rowCount % 8;
  if (rem > 0) {
    uint8_t mask = static_cast<uint8_t>((1u << rem) - 1u);
    dst[byteCount - 1] &= mask;
  }
  return std::move(*buf);
}

/// Expand a dictionary-encoded vector to flat Arrow buffers for fixed-width types.
void ExpandFixedDictToBuffers(BaseVector* vec, int32_t rowCount,
                              int32_t width,
                              std::vector<std::shared_ptr<arrow::Buffer>>& out) {
  // 1. validity
  out.push_back(BuildArrowValidity(vec, rowCount));

  // 2. values: expand each row's dictionary entry
  auto buf = arrow::AllocateBuffer(static_cast<size_t>(width) * rowCount);
  if (!buf.ok()) {
    throw std::runtime_error("ExpandFixedDictToBuffers: Arrow buffer allocation failed");
  }
  uint8_t* dst = (*buf)->mutable_data();

  int32_t* ids = nullptr;
  const uint8_t* dictValues = nullptr;
  switch (width) {
    case 4: {
      auto* dictVec = reinterpret_cast<Vector<DictionaryContainer<int32_t>>*>(vec);
      ids = unsafe::UnsafeDictionaryVector::GetIds(dictVec);
      dictValues = reinterpret_cast<const uint8_t*>(
          unsafe::UnsafeDictionaryVector::GetDictionary<int32_t>(dictVec));
      break;
    }
    case 8: {
      auto* dictVec = reinterpret_cast<Vector<DictionaryContainer<int64_t>>*>(vec);
      ids = unsafe::UnsafeDictionaryVector::GetIds(dictVec);
      dictValues = reinterpret_cast<const uint8_t*>(
          unsafe::UnsafeDictionaryVector::GetDictionary<int64_t>(dictVec));
      break;
    }
    case 16: {
      auto* dictVec = reinterpret_cast<Vector<DictionaryContainer<Decimal128>>*>(vec);
      ids = unsafe::UnsafeDictionaryVector::GetIds(dictVec);
      dictValues = reinterpret_cast<const uint8_t*>(
          unsafe::UnsafeDictionaryVector::GetDictionary<Decimal128>(dictVec));
      break;
    }
    default:
      throw std::runtime_error("ExpandFixedDictToBuffers: unsupported width " +
                               std::to_string(width));
  }
  if (!dictValues) {
    throw std::runtime_error("ExpandFixedDictToBuffers: null dictionary values");
  }

  for (int32_t i = 0; i < rowCount; ++i) {
    int32_t dictIdx = ids[i];
    std::memcpy(dst + static_cast<size_t>(i) * width,
                dictValues + static_cast<size_t>(dictIdx) * width, width);
  }
  out.push_back(std::move(*buf));
}

/// Expand a dictionary-encoded string vector to flat Arrow buffers.
void ExpandStringDictToBuffers(BaseVector* vec, int32_t rowCount,
                               std::vector<std::shared_ptr<arrow::Buffer>>& out) {
  out.push_back(BuildArrowValidity(vec, rowCount));

  auto* dictVec = reinterpret_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>>*>(vec);
  int32_t* ids = unsafe::UnsafeDictionaryVector::GetIds(dictVec);
  char* dictValues = unsafe::UnsafeDictionaryVector::GetVarCharDictionary(dictVec);
  int32_t* dictOffsets = unsafe::UnsafeDictionaryVector::GetDictionaryOffsets(dictVec);

  if (!dictValues || !dictOffsets) {
    throw std::runtime_error("ExpandStringDictToBuffers: null dictionary data");
  }

  // Compute expanded offsets and total values size
  std::vector<int32_t> expandedOffsets(rowCount + 1);
  expandedOffsets[0] = 0;
  for (int32_t i = 0; i < rowCount; ++i) {
    int32_t dictIdx = ids[i];
    int32_t strLen = dictOffsets[dictIdx + 1] - dictOffsets[dictIdx];
    expandedOffsets[i + 1] = expandedOffsets[i] + strLen;
  }
  int32_t totalValuesSize = expandedOffsets[rowCount];

  // Offsets buffer
  {
    auto buf = arrow::AllocateBuffer(
        static_cast<size_t>(rowCount + 1) * sizeof(int32_t));
    if (!buf.ok())
      throw std::runtime_error("ExpandStringDictToBuffers: offsets alloc failed");
    std::memcpy((*buf)->mutable_data(), expandedOffsets.data(),
                static_cast<size_t>(rowCount + 1) * sizeof(int32_t));
    out.push_back(std::move(*buf));
  }

  // Values buffer
  if (totalValuesSize > 0) {
    auto buf = arrow::AllocateBuffer(totalValuesSize);
    if (!buf.ok())
      throw std::runtime_error("ExpandStringDictToBuffers: values alloc failed");
    uint8_t* dst = (*buf)->mutable_data();
    for (int32_t i = 0; i < rowCount; ++i) {
      int32_t dictIdx = ids[i];
      int32_t strLen = dictOffsets[dictIdx + 1] - dictOffsets[dictIdx];
      if (strLen > 0) {
        std::memcpy(dst + expandedOffsets[i],
                    dictValues + dictOffsets[dictIdx], strLen);
      }
    }
    out.push_back(std::move(*buf));
  } else {
    // Zero-length: push an empty buffer (Arrow convention: still need a buffer)
    auto buf = arrow::AllocateBuffer(0);
    if (!buf.ok())
      throw std::runtime_error("ExpandStringDictToBuffers: empty values alloc failed");
    out.push_back(std::move(*buf));
  }
}

/// Recursively build Arrow buffers for a single column.
void BuildBuffersForColumn(
    BaseVector* vec, const OmniTypeDescriptor& desc, int32_t rowCount,
    std::vector<std::shared_ptr<arrow::Buffer>>& result) {
  // Handle dictionary encoding: expand to flat first
  if (vec->GetEncoding() == 1 /* OMNI_DICTIONARY */) {
    auto pt = PhysicalTypeOf(desc);
    if (pt == OmniPhysicalType::BINARY) {
      ExpandStringDictToBuffers(vec, rowCount, result);
      return;
    }
    int32_t width = PhysicalTypeByteWidth(pt);
    if (width > 0) {
      ExpandFixedDictToBuffers(vec, rowCount, width, result);
      return;
    }
    throw std::runtime_error("BuildBuffersForColumn: unsupported dictionary physical type");
  }

  auto pt = PhysicalTypeOf(desc);

  if (pt == OmniPhysicalType::INT8 || pt == OmniPhysicalType::INT16 ||
      pt == OmniPhysicalType::INT32 || pt == OmniPhysicalType::INT64 ||
      pt == OmniPhysicalType::DECIMAL64 || pt == OmniPhysicalType::DECIMAL128 ||
      pt == OmniPhysicalType::BOOL) {
    // Fixed-width: validity + values
    result.push_back(BuildArrowValidity(vec, rowCount));

    int32_t width = PhysicalTypeByteWidth(pt);
    const void* src = VectorHelper::UnsafeGetValues(vec);
    auto buf = arrow::AllocateBuffer(static_cast<size_t>(width) * rowCount);
    if (!buf.ok())
      throw std::runtime_error("BuildBuffersForColumn: values alloc failed");
    std::memcpy((*buf)->mutable_data(), src,
                static_cast<size_t>(width) * rowCount);
    result.push_back(std::move(*buf));
  } else if (pt == OmniPhysicalType::BINARY) {
    // Variable-width: validity + offsets + values
    result.push_back(BuildArrowValidity(vec, rowCount));

    auto* charVec =
        reinterpret_cast<Vector<LargeStringContainer<std::string_view>>*>(vec);

    // Offsets: (rowCount+1) int32_t
    int32_t* srcOffsets =
        static_cast<int32_t*>(VectorHelper::UnsafeGetOffsetsAddr(vec));
    int32_t totalValuesSize = srcOffsets[rowCount];
    {
      auto buf = arrow::AllocateBuffer(
          static_cast<size_t>(rowCount + 1) * sizeof(int32_t));
      if (!buf.ok())
        throw std::runtime_error("BuildBuffersForColumn: offsets alloc failed");
      std::memcpy((*buf)->mutable_data(), srcOffsets,
                  static_cast<size_t>(rowCount + 1) * sizeof(int32_t));
      result.push_back(std::move(*buf));
    }

    // Values: raw char data
    if (totalValuesSize > 0) {
      const char* srcValues =
          unsafe::UnsafeStringVector::GetValues(charVec);
      auto buf = arrow::AllocateBuffer(totalValuesSize);
      if (!buf.ok())
        throw std::runtime_error("BuildBuffersForColumn: values alloc failed");
      std::memcpy((*buf)->mutable_data(), srcValues, totalValuesSize);
      result.push_back(std::move(*buf));
    } else {
      auto buf = arrow::AllocateBuffer(0);
      if (!buf.ok())
        throw std::runtime_error("BuildBuffersForColumn: empty values alloc failed");
      result.push_back(std::move(*buf));
    }
  } else if (pt == OmniPhysicalType::LIST) {
    // ARRAY: validity + offsets + recursively child elements
    result.push_back(BuildArrowValidity(vec, rowCount));

    auto* arrayVec = reinterpret_cast<ArrayVector*>(vec);

    // Offsets: use int32_t (matching the deserializer which reads int32_t offsets)
    {
      auto buf = arrow::AllocateBuffer(
          static_cast<size_t>(rowCount + 1) * sizeof(int32_t));
      if (!buf.ok())
        throw std::runtime_error("BuildBuffersForColumn: array offsets alloc failed");
      int32_t* dst = reinterpret_cast<int32_t*>((*buf)->mutable_data());
      for (int32_t j = 0; j <= rowCount; ++j) {
        dst[j] = static_cast<int32_t>(arrayVec->GetOffset(j));
      }
      result.push_back(std::move(*buf));
    }

    // Recurse into element vector
    auto elementVec = arrayVec->GetElementVector().get();
    int32_t totalElements = static_cast<int32_t>(arrayVec->GetOffset(rowCount));
    if (elementVec && !desc.children.empty()) {
      BuildBuffersForColumn(elementVec, desc.children[0], totalElements, result);
    }
  } else {
    throw std::runtime_error(
        "BuildBuffersForColumn: unsupported physical type for test helper");
  }
}

}  // anonymous namespace

// ============================================================================
//  Public API
// ============================================================================

std::vector<std::shared_ptr<arrow::Buffer>> BuildColumnarBuffersFromVectorBatch(
    const VectorBatch& vb, const std::vector<OmniTypeDescriptor>& schema) {
  int nCols = vb.GetVectorCount();
  int nSchemaCols = static_cast<int>(schema.size());
  if (nCols != nSchemaCols) {
    throw std::runtime_error(
        "BuildColumnarBuffersFromVectorBatch: column count mismatch: " +
        std::to_string(nCols) + " vs " + std::to_string(nSchemaCols));
  }

  int nRows = vb.GetRowCount();
  std::vector<std::shared_ptr<arrow::Buffer>> result;
  for (int col = 0; col < nCols; ++col) {
    auto* vec = vb.Get(col);
    BuildBuffersForColumn(vec, schema[col], nRows, result);
  }
  return result;
}

// ============================================================================
//  VectorBatchesEqual
// ============================================================================

bool VectorBatchesEqual(const VectorBatch& a, const VectorBatch& b) {
  int nCols = a.GetVectorCount();
  if (nCols != b.GetVectorCount()) return false;
  int nRows = a.GetRowCount();
  if (nRows != b.GetRowCount()) return false;

  for (int col = 0; col < nCols; ++col) {
    auto* va = a.Get(col);
    auto* vb = b.Get(col);
    if (va->GetTypeId() != vb->GetTypeId()) return false;

    int32_t typeId = va->GetTypeId();
    int32_t width = TypeIdByteWidth(typeId);

    for (int row = 0; row < nRows; ++row) {
      bool nullA = va->IsNull(row);
      bool nullB = vb->IsNull(row);
      if (nullA != nullB) return false;
      if (nullA) continue;

      if (IsStringType(typeId)) {
        auto* sva = reinterpret_cast<Vector<LargeStringContainer<std::string_view>>*>(va);
        auto* svb = reinterpret_cast<Vector<LargeStringContainer<std::string_view>>*>(vb);
        if (sva->GetValue(row) != svb->GetValue(row)) return false;
      } else if (typeId == OMNI_ARRAY) {
        auto* ava = reinterpret_cast<ArrayVector*>(va);
        auto* avb = reinterpret_cast<ArrayVector*>(vb);
        if (ava->GetSize(row) != avb->GetSize(row)) return false;
        // Compare element-wise for simple Array<INT> case
        int64_t offA = ava->GetOffset(row);
        int64_t offB = avb->GetOffset(row);
        int64_t sz = ava->GetSize(row);
        auto elemVecA = ava->GetElementVector().get();
        auto elemVecB = avb->GetElementVector().get();
        // Use a temporary slice to compare elements
        for (int64_t k = 0; k < sz; ++k) {
          if (elemVecA->IsNull(static_cast<int32_t>(offA + k)) !=
              elemVecB->IsNull(static_cast<int32_t>(offB + k)))
            return false;
          if (!elemVecA->IsNull(static_cast<int32_t>(offA + k))) {
            // Compare element values via raw bytes
            int32_t elemWidth = TypeIdByteWidth(elemVecA->GetTypeId());
            if (elemWidth > 0) {
              auto* rawA = VectorHelper::UnsafeGetValues(elemVecA);
              auto* rawB = VectorHelper::UnsafeGetValues(elemVecB);
              if (rawA && rawB) {
                if (std::memcmp(
                        static_cast<const char*>(rawA) + (offA + k) * elemWidth,
                        static_cast<const char*>(rawB) + (offB + k) * elemWidth,
                        elemWidth) != 0)
                  return false;
              }
            }
          }
        }
      } else if (width > 0) {
        // Fixed-width: byte-level comparison
        auto* rawA = VectorHelper::UnsafeGetValues(va);
        auto* rawB = VectorHelper::UnsafeGetValues(vb);
        if (rawA && rawB) {
          if (std::memcmp(static_cast<const char*>(rawA) + row * width,
                          static_cast<const char*>(rawB) + row * width,
                          width) != 0)
            return false;
        }
      }
    }
  }
  return true;
}

// ============================================================================
//  MakeBatch constructors
// ============================================================================

VectorBatch* MakeBatch_INT(int rowCount) {
  auto* vb = new VectorBatch(rowCount);
  auto* vec = new Vector<int32_t>(rowCount);
  for (int i = 0; i < rowCount; ++i)
    vec->SetValue(i, i + 1);
  vb->Append(vec);
  return vb;
}

VectorBatch* MakeBatch_INT_LONG_VARCHAR(int rowCount, uint32_t nullSeed) {
  auto* vb = new VectorBatch(rowCount);
  auto* vecInt = new Vector<int32_t>(rowCount);
  auto* vecLong = new Vector<int64_t>(rowCount);
  auto* vecStr = reinterpret_cast<Vector<LargeStringContainer<std::string_view>>*>(
      VectorHelper::CreateVector(OMNI_FLAT, OMNI_VARCHAR, rowCount));

  for (int i = 0; i < rowCount; ++i) {
    vecInt->SetValue(i, i + 1);
    vecLong->SetValue(i, static_cast<int64_t>(i + 1) * 10);
    std::string s = "str_" + std::to_string(i);
    vecStr->SetValue(i, std::string_view(s.data(), s.size()));

    if (nullSeed > 0 && i % static_cast<int>(nullSeed + 1) == 0) {
      vecInt->SetNull(i);
      vecLong->SetNull(i);
      vecStr->SetNull(i);
    }
  }
  vb->Append(vecInt);
  vb->Append(vecLong);
  vb->Append(vecStr);
  return vb;
}

VectorBatch* MakeBatch_DECIMAL64(int rowCount, uint32_t precision,
                                 uint32_t scale) {
  auto* vb = new VectorBatch(rowCount);
  auto* vec = reinterpret_cast<Vector<int64_t>*>(
      VectorHelper::CreateVector(OMNI_FLAT, OMNI_DECIMAL64, rowCount));
  for (int i = 0; i < rowCount; ++i)
    vec->SetValue(i, static_cast<int64_t>(i + 1));
  vb->Append(vec);
  return vb;
}

VectorBatch* MakeBatch_DECIMAL128(int rowCount, uint32_t precision,
                                  uint32_t scale) {
  auto* vb = new VectorBatch(rowCount);
  auto* vec = reinterpret_cast<Vector<Decimal128>*>(
      VectorHelper::CreateVector(OMNI_FLAT, OMNI_DECIMAL128, rowCount));
  for (int i = 0; i < rowCount; ++i)
    vec->SetValue(i, Decimal128(0, static_cast<uint64_t>(i + 1)));
  vb->Append(vec);
  return vb;
}

VectorBatch* MakeBatch_VARCHAR_CHAR(int rowCount, uint32_t nullSeed) {
  auto* vb = new VectorBatch(rowCount);
  auto* vecVarchar = reinterpret_cast<Vector<LargeStringContainer<std::string_view>>*>(
      VectorHelper::CreateVector(OMNI_FLAT, OMNI_VARCHAR, rowCount));
  auto* vecChar = reinterpret_cast<Vector<LargeStringContainer<std::string_view>>*>(
      VectorHelper::CreateVector(OMNI_FLAT, OMNI_CHAR, rowCount));

  for (int i = 0; i < rowCount; ++i) {
    std::string vs = "varchar_" + std::to_string(i);
    vecVarchar->SetValue(i, std::string_view(vs.data(), vs.size()));

    // CHAR: fixed-width padded (pad to 16 chars)
    std::string cs = "char_" + std::to_string(i);
    cs.resize(16, ' ');
    vecChar->SetValue(i, std::string_view(cs.data(), cs.size()));

    if (nullSeed > 0 && i % static_cast<int>(nullSeed + 1) == 0) {
      vecVarchar->SetNull(i);
      vecChar->SetNull(i);
    }
  }
  vb->Append(vecVarchar);
  vb->Append(vecChar);
  return vb;
}

VectorBatch* MakeBatch_ArrayInt(int rowCount, uint32_t nullSeed) {
  auto* vb = new VectorBatch(rowCount);

  // Column 0: row index (INT)
  auto* colIdx = new Vector<int32_t>(rowCount);
  for (int i = 0; i < rowCount; ++i)
    colIdx->SetValue(i, i);

  // Column 1: ARRAY<INT>
  // Compute total elements: 1 + 2 + ... + rowCount
  int totalElements = rowCount * (rowCount + 1) / 2;
  auto elementVec = std::make_shared<Vector<int32_t>>(totalElements);
  int elemIdx = 0;
  for (int i = 0; i < rowCount; ++i) {
    for (int j = 0; j <= i; ++j) {
      elementVec->SetValue(elemIdx++, i * 10 + j);
    }
  }
  auto* arrayVec = new ArrayVector(rowCount, elementVec);
  elemIdx = 0;
  arrayVec->SetOffset(0, 0);
  for (int i = 0; i < rowCount; ++i) {
    int sz = i + 1;  // row i has i+1 elements
    elemIdx += sz;
    arrayVec->SetOffset(i + 1, elemIdx);
  }

  // Apply nulls if specified
  if (nullSeed > 0) {
    for (int i = 0; i < rowCount; ++i) {
      if (i % static_cast<int>(nullSeed + 1) == 0) {
        colIdx->SetNull(i);
        arrayVec->SetNull(i);
      }
    }
  }

  vb->Append(colIdx);
  vb->Append(arrayVec);
  return vb;
}

VectorBatch* MakeBatch_DictionaryIntLong(int rowCount) {
  constexpr int kDictSize = 6;
  int32_t dictInt[kDictSize] = {111, 112, 113, 114, 115, 116};
  int64_t dictLong[kDictSize] = {221, 222, 223, 224, 225, 226};
  int32_t ids[kDictSize] = {0, 1, 2, 3, 4, 5};

  auto* vb = new VectorBatch(rowCount);

  // Column 0: flat INT row index
  auto* colIdx = new Vector<int32_t>(rowCount);
  for (int i = 0; i < rowCount; ++i)
    colIdx->SetValue(i, i);
  vb->Append(colIdx);

  // Column 1: Dictionary<INT>
  {
    auto dictVec = std::make_shared<Vector<int32_t>>(kDictSize);
    for (int j = 0; j < kDictSize; ++j)
      dictVec->SetValue(j, dictInt[j]);
    // Create dictionary encoding via VectorHelper
    int32_t* rowIds = new int32_t[rowCount];
    for (int i = 0; i < rowCount; ++i)
      rowIds[i] = ids[i % kDictSize];
    auto* dicIntVec = VectorHelper::CreateDictionary<int32_t>(
        rowIds, rowCount, dictVec.get());
    delete[] rowIds;
    vb->Append(dicIntVec);
  }

  // Column 2: Dictionary<LONG>
  {
    auto dictVec = std::make_shared<Vector<int64_t>>(kDictSize);
    for (int j = 0; j < kDictSize; ++j)
      dictVec->SetValue(j, dictLong[j]);
    int32_t* rowIds = new int32_t[rowCount];
    for (int i = 0; i < rowCount; ++i)
      rowIds[i] = ids[i % kDictSize];
    auto* dicLongVec = VectorHelper::CreateDictionary<int64_t>(
        rowIds, rowCount, dictVec.get());
    delete[] rowIds;
    vb->Append(dicLongVec);
  }

  return vb;
}
