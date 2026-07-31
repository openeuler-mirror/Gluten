#include "shuffle/arrow_columnar_deserializer.h"

#include <cstring>
#include <stdexcept>

#include "common/debug.h"
#include "shuffle/arrow_type_bridge.h"
#include <vector/vector_common.h>

using namespace omniruntime::vec;

namespace {

// Invert Arrow validity bitmap (set-bit = valid) into Omni null-mask
// (set-bit = null).  Both use LSB-first bit ordering.  rowCount rows
// require ceil(rowCount/8) bytes.
void WriteInvertedValidity(const uint8_t* arrowValidity, int32_t rowCount,
                           uint8_t* omniNulls) {
  int32_t fullBytes = rowCount / 8;
  for (int32_t i = 0; i < fullBytes; ++i) {
    omniNulls[i] = ~arrowValidity[i];
  }
  int32_t rem = rowCount % 8;
  if (rem > 0) {
    // Invert only the low rem bits; clear higher bits to zero.
    uint8_t mask = static_cast<uint8_t>((1u << rem) - 1u);
    omniNulls[fullBytes] = (~arrowValidity[fullBytes]) & mask;
  }
}

}  // anonymous namespace

void DeserializeArrowBufferToOmniVector(
    const OmniTypeDescriptor& desc, int32_t rowCount,
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    std::size_t& bufIdx, BaseVector* omniVec) {
  auto pt = PhysicalTypeOf(desc);

  if (bufIdx >= buffers.size()) {
    LogsError("DeserializeArrowBufferToOmniVector validity buffer index out of range: "
              "bufIdx=%zu buffersSize=%zu typeId=%d rowCount=%d",
              bufIdx, buffers.size(), desc.typeId, rowCount);
    throw std::runtime_error("DeserializeArrowBufferToOmniVector: validity buffer index out of range");
  }

  // ---- validity (may be nullptr = all-valid) ----
  std::shared_ptr<arrow::Buffer> validity =
      (bufIdx < buffers.size()) ? buffers[bufIdx] : nullptr;
  ++bufIdx;

  if (pt == OmniPhysicalType::INT8 || pt == OmniPhysicalType::INT16 ||
      pt == OmniPhysicalType::INT32 || pt == OmniPhysicalType::INT64 ||
      pt == OmniPhysicalType::DECIMAL64 || pt == OmniPhysicalType::DECIMAL128 ||
      pt == OmniPhysicalType::BOOL) {
    // ---- fixed-width: values first, then validity ----
    if (bufIdx >= buffers.size()) {
      LogsError("DeserializeArrowBufferToOmniVector fixed-width values buffer index out of range: "
                "bufIdx=%zu buffersSize=%zu typeId=%d rowCount=%d",
                bufIdx, buffers.size(), desc.typeId, rowCount);
      throw std::runtime_error("DeserializeArrowBufferToOmniVector: fixed-width values buffer index out of range");
    }
    const auto& values = buffers[bufIdx++];
    if (values == nullptr) {
      LogsError("DeserializeArrowBufferToOmniVector fixed-width values buffer is null: "
                "typeId=%d rowCount=%d bufIdx=%zu",
                desc.typeId, rowCount, bufIdx);
      throw std::runtime_error("DeserializeArrowBufferToOmniVector: fixed-width values buffer is null");
    }
    int32_t width = PhysicalTypeByteWidth(pt);

    // Expand the vector to rowCount before writing.
    // Top-level columns are created with size=rowCount (no-op), but child element
    // vectors inside ARRAY/MAP/ROW are created with size=0 by CreateComplexVector.
    // Without Expand, memcpy writes into a 0-capacity buffer → heap overflow.
    omniVec->Expand(rowCount);

    std::memcpy(VectorHelper::UnsafeGetValues(omniVec), values->data(),
                static_cast<size_t>(width) * rowCount);

    if (validity != nullptr) {
      uint8_t* omniNulls = unsafe::UnsafeBaseVector::GetNulls(omniVec);
      WriteInvertedValidity(validity->data(), rowCount, omniNulls);
    } else {
      // validity == nullptr 表示全有效（Arrow 约定）。
      // 必须显式清零 nulls mask：CreateVector 不保证 nulls buffer 已清零，
      // 未清零会导致 IsNullAt() 读到垃圾值，把 null 行误判为有效 → 读到垃圾值。
      // nulls 是 bitmap 格式（每行 1 bit），字节数 = (rowCount+7)/8，与 WriteInvertedValidity 一致。
      uint8_t* omniNulls = unsafe::UnsafeBaseVector::GetNulls(omniVec);
      if (omniNulls != nullptr) {
        std::memset(omniNulls, 0, static_cast<size_t>((rowCount + 7) / 8));
      }
    }
  } else if (pt == OmniPhysicalType::BINARY) {
    // ---- variable-width: offsets + values ----
    auto charVec =
        reinterpret_cast<Vector<LargeStringContainer<std::string_view>>*>(
            omniVec);
    charVec->Expand(rowCount);

    if (bufIdx + 1 >= buffers.size()) {
      LogsError("DeserializeArrowBufferToOmniVector BINARY offsets/values buffer index out of range: "
                "bufIdx=%zu buffersSize=%zu typeId=%d rowCount=%d",
                bufIdx, buffers.size(), desc.typeId, rowCount);
      throw std::runtime_error("DeserializeArrowBufferToOmniVector: BINARY buffer index out of range");
    }
    const auto& offsets = buffers[bufIdx++];
    const auto& values = buffers[bufIdx++];
    if (offsets == nullptr || values == nullptr) {
      LogsError("DeserializeArrowBufferToOmniVector BINARY offsets/values buffer is null: "
                "typeId=%d rowCount=%d offsetsNull=%d valuesNull=%d bufIdx=%zu",
                desc.typeId, rowCount, offsets == nullptr, values == nullptr, bufIdx);
      throw std::runtime_error("DeserializeArrowBufferToOmniVector: BINARY buffer is null");
    }

    char* valuesAddress =
        unsafe::UnsafeStringVector::ExpandStringBuffer(charVec,
                                                       values->size());
    auto offsetsAddress =
        static_cast<uint8_t*>(VectorHelper::UnsafeGetOffsetsAddr(omniVec));

    std::memcpy(valuesAddress, values->data(), values->size());
    std::memcpy(offsetsAddress, offsets->data(), offsets->size());

    if (validity != nullptr) {
      uint8_t* omniNulls = unsafe::UnsafeBaseVector::GetNulls(omniVec);
      WriteInvertedValidity(validity->data(), rowCount, omniNulls);
    } else {
      // validity == nullptr 表示全有效（Arrow 约定）。
      // 必须显式清零 nulls mask：CreateVector 不保证 nulls buffer 已清零，
      // 未清零会导致 IsNullAt() 读到垃圾值，把 null 行误判为有效 → 读到垃圾值。
      // nulls 是 bitmap 格式（每行 1 bit），字节数 = (rowCount+7)/8，与 WriteInvertedValidity 一致。
      uint8_t* omniNulls = unsafe::UnsafeBaseVector::GetNulls(omniVec);
      if (omniNulls != nullptr) {
        std::memset(omniNulls, 0, static_cast<size_t>((rowCount + 7) / 8));
      }
    }
  } else if (pt == OmniPhysicalType::LIST) {
    // ---- ARRAY: offsets + recurse into element vector ----
    auto arrayVec = reinterpret_cast<ArrayVector*>(omniVec);
    arrayVec->Expand(rowCount);

    if (bufIdx >= buffers.size()) {
      LogsError("DeserializeArrowBufferToOmniVector LIST offsets buffer index out of range: "
                "bufIdx=%zu buffersSize=%zu typeId=%d rowCount=%d",
                bufIdx, buffers.size(), desc.typeId, rowCount);
      throw std::runtime_error("DeserializeArrowBufferToOmniVector: LIST offsets buffer index out of range");
    }
    const auto& offsets = buffers[bufIdx++];
    if (offsets == nullptr) {
      LogsError("DeserializeArrowBufferToOmniVector LIST offsets buffer is null: "
                "typeId=%d rowCount=%d bufIdx=%zu",
                desc.typeId, rowCount, bufIdx);
      throw std::runtime_error("DeserializeArrowBufferToOmniVector: LIST offsets buffer is null");
    }
    auto offsetsPtr = reinterpret_cast<const int32_t*>(offsets->data());
    for (int32_t j = 0; j <= rowCount; ++j) {
      arrayVec->SetOffset(j, offsetsPtr[j]);
    }

    auto elementVec = arrayVec->GetElementVector().get();
    if (elementVec && !desc.children.empty()) {
      DeserializeArrowBufferToOmniVector(desc.children[0],
                                         offsetsPtr[rowCount], buffers,
                                         bufIdx, elementVec);
    }

    if (validity != nullptr) {
      uint8_t* omniNulls = unsafe::UnsafeBaseVector::GetNulls(omniVec);
      WriteInvertedValidity(validity->data(), rowCount, omniNulls);
    } else {
      // validity == nullptr 表示全有效（Arrow 约定）。
      // 必须显式清零 nulls mask：CreateVector 不保证 nulls buffer 已清零，
      // 未清零会导致 IsNullAt() 读到垃圾值，把 null 行误判为有效 → 读到垃圾值。
      // nulls 是 bitmap 格式（每行 1 bit），字节数 = (rowCount+7)/8，与 WriteInvertedValidity 一致。
      uint8_t* omniNulls = unsafe::UnsafeBaseVector::GetNulls(omniVec);
      if (omniNulls != nullptr) {
        std::memset(omniNulls, 0, static_cast<size_t>((rowCount + 7) / 8));
      }
    }
  } else if (pt == OmniPhysicalType::MAP) {
    // ---- MAP: offsets + recurse into key + value vectors ----
    auto mapVec = reinterpret_cast<MapVector*>(omniVec);
    mapVec->Expand(rowCount);

    if (bufIdx >= buffers.size()) {
      LogsError("DeserializeArrowBufferToOmniVector MAP offsets buffer index out of range: "
                "bufIdx=%zu buffersSize=%zu typeId=%d rowCount=%d",
                bufIdx, buffers.size(), desc.typeId, rowCount);
      throw std::runtime_error("DeserializeArrowBufferToOmniVector: MAP offsets buffer index out of range");
    }
    const auto& offsets = buffers[bufIdx++];
    if (offsets == nullptr) {
      LogsError("DeserializeArrowBufferToOmniVector MAP offsets buffer is null: "
                "typeId=%d rowCount=%d bufIdx=%zu",
                desc.typeId, rowCount, bufIdx);
      throw std::runtime_error("DeserializeArrowBufferToOmniVector: MAP offsets buffer is null");
    }
    auto offsetsPtr = reinterpret_cast<const int32_t*>(offsets->data());
    for (int32_t j = 0; j <= rowCount; ++j) {
      mapVec->SetOffset(j, offsetsPtr[j]);
    }

    auto keyVec = mapVec->GetKeyVector().get();
    auto valueVec = mapVec->GetValueVector().get();
    if (keyVec && desc.children.size() >= 1) {
      DeserializeArrowBufferToOmniVector(desc.children[0],
                                         offsetsPtr[rowCount], buffers,
                                         bufIdx, keyVec);
    }
    if (valueVec && desc.children.size() >= 2) {
      DeserializeArrowBufferToOmniVector(desc.children[1],
                                         offsetsPtr[rowCount], buffers,
                                         bufIdx, valueVec);
    }

    if (validity != nullptr) {
      uint8_t* omniNulls = unsafe::UnsafeBaseVector::GetNulls(omniVec);
      WriteInvertedValidity(validity->data(), rowCount, omniNulls);
    } else {
      // validity == nullptr 表示全有效（Arrow 约定）。
      // 必须显式清零 nulls mask：CreateVector 不保证 nulls buffer 已清零，
      // 未清零会导致 IsNullAt() 读到垃圾值，把 null 行误判为有效 → 读到垃圾值。
      // nulls 是 bitmap 格式（每行 1 bit），字节数 = (rowCount+7)/8，与 WriteInvertedValidity 一致。
      uint8_t* omniNulls = unsafe::UnsafeBaseVector::GetNulls(omniVec);
      if (omniNulls != nullptr) {
        std::memset(omniNulls, 0, static_cast<size_t>((rowCount + 7) / 8));
      }
    }
  } else if (pt == OmniPhysicalType::STRUCT) {
    // ---- ROW: recurse into each child field ----
    auto rowVec = reinterpret_cast<RowVector*>(omniVec);
    rowVec->Expand(rowCount);

    for (size_t c = 0; c < desc.children.size(); ++c) {
      auto childVec = rowVec->ChildAt(static_cast<int32_t>(c)).get();
      if (childVec) {
        DeserializeArrowBufferToOmniVector(desc.children[c], rowCount,
                                           buffers, bufIdx, childVec);
      }
    }

    if (validity != nullptr) {
      uint8_t* omniNulls = unsafe::UnsafeBaseVector::GetNulls(omniVec);
      WriteInvertedValidity(validity->data(), rowCount, omniNulls);
    } else {
      // validity == nullptr 表示全有效（Arrow 约定）。
      // 必须显式清零 nulls mask：CreateVector 不保证 nulls buffer 已清零，
      // 未清零会导致 IsNullAt() 读到垃圾值，把 null 行误判为有效 → 读到垃圾值。
      // nulls 是 bitmap 格式（每行 1 bit），字节数 = (rowCount+7)/8，与 WriteInvertedValidity 一致。
      uint8_t* omniNulls = unsafe::UnsafeBaseVector::GetNulls(omniVec);
      if (omniNulls != nullptr) {
        std::memset(omniNulls, 0, static_cast<size_t>((rowCount + 7) / 8));
      }
    }
  } else {
    LogsError("DeserializeArrowBufferToOmniVector unsupported physical type: "
              "physicalType=%d typeId=%d rowCount=%d bufIdx=%zu",
              static_cast<int>(pt), desc.typeId, rowCount, bufIdx);
    throw std::runtime_error(
        "DeserializeArrowBufferToOmniVector: unsupported physical type");
  }
}
