#ifndef CPP_ARROW_COLUMNAR_DESERIALIZER_H
#define CPP_ARROW_COLUMNAR_DESERIALIZER_H

#include <arrow/buffer.h>
#include <cstddef>
#include <memory>
#include <vector>

#include "shuffle/arrow_type_bridge.h"
#include <vector/vector_common.h>

// Deserialize Arrow buffer list (ordered by column schema, recursively expanded
// for complex types) back into an Omni vector.
//
// bufIdx is the cursor into the buffers vector, advanced recursively by this
// function. Callers initialize bufIdx to 0.
//
// Validity convention: Arrow set-bit = valid; Omni set-bit = null.
// This function inverts the validity bitmap before writing into the Omni
// null-mask. If the validity buffer is nullptr (all-valid), the Omni null-mask
// is left untouched.
//
// Buffer order:
//   Fixed-width: [validity?][values]
//   Variable-width: [validity?][offsets][values]
//   Complex: [validity?] + child buffers (recursive)
void DeserializeArrowBufferToOmniVector(
    const OmniTypeDescriptor& desc,
    int32_t rowCount,
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    std::size_t& bufIdx,
    omniruntime::vec::BaseVector* omniVec);

#endif  // CPP_ARROW_COLUMNAR_DESERIALIZER_H
