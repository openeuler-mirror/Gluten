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

#ifndef SPARK_THESTRAL_PLUGIN_ARROW_TEST_VECTORS_H
#define SPARK_THESTRAL_PLUGIN_ARROW_TEST_VECTORS_H

#include <arrow/result.h>
#include <arrow/buffer.h>
#include <cstdint>
#include <memory>
#include <vector>

#include <vector/vector_common.h>
#include "shuffle/arrow_type_bridge.h"

/// Build Arrow buffer list from an Omni VectorBatch for round-trip testing.
///
/// This is a test helper, not a production serializer. For each column, the
/// function extracts validity (inverted from Omni to Arrow convention),
/// values, and offsets (for variable-width / complex types) and appends them
/// in the order expected by DeserializeArrowBufferToOmniVector.
///
/// Validity convention: Arrow set-bit = valid, Omni set-bit = null.
/// The bitmap is per-byte inverted. If no rows are null, a nullptr is pushed
/// in place of the validity buffer.
///
/// Dictionary-encoded columns are expanded to flat buffers before extraction.
std::vector<std::shared_ptr<arrow::Buffer>> BuildColumnarBuffersFromVectorBatch(
    const omniruntime::vec::VectorBatch& vb,
    const std::vector<OmniTypeDescriptor>& schema);

/// Compare two Omni VectorBatches column-by-column, row-by-row.
///
/// Returns true if both batches have the same vector count, row count, type
/// IDs, and identical per-row null/value for every column.
///
/// Fixed-width values are compared via the typed GetValue<T>() path.
/// Variable-width (VARCHAR/CHAR/VARBINARY) values are compared as string_view.
/// DECIMAL columns are compared via byte-level memcmp of the underlying value
/// buffers.
bool VectorBatchesEqual(const omniruntime::vec::VectorBatch& a,
                        const omniruntime::vec::VectorBatch& b);

// --- Constructors for test VectorBatches ---
// Each returns a heap-allocated VectorBatch; caller owns the pointer.
// These mirror the patterns in test/utils/test_utils.cpp but are standalone
// (no partition-id column), designed for round-trip serialization tests.

/// Single INT column with values 1..rowCount.
omniruntime::vec::VectorBatch* MakeBatch_INT(int rowCount);

/// Three columns: INT, LONG, VARCHAR.
/// nullSeed controls nullness: rows where i % (nullSeed+1) == 0 are set null.
omniruntime::vec::VectorBatch* MakeBatch_INT_LONG_VARCHAR(int rowCount,
                                                          uint32_t nullSeed);

/// Single DECIMAL64 column. Values are simple sequential numbers.
omniruntime::vec::VectorBatch* MakeBatch_DECIMAL64(int rowCount,
                                                   uint32_t precision,
                                                   uint32_t scale);

/// Single DECIMAL128 column. Values are simple sequential numbers.
omniruntime::vec::VectorBatch* MakeBatch_DECIMAL128(int rowCount,
                                                    uint32_t precision,
                                                    uint32_t scale);

/// Two columns: VARCHAR, CHAR (with padding).
/// nullSeed controls nullness same as MakeBatch_INT_LONG_VARCHAR.
omniruntime::vec::VectorBatch* MakeBatch_VARCHAR_CHAR(int rowCount,
                                                      uint32_t nullSeed);

/// Two columns: INT (row index), ARRAY<INT> (sequential elements).
/// nullSeed controls nullness of both columns.
omniruntime::vec::VectorBatch* MakeBatch_ArrayInt(int rowCount,
                                                  uint32_t nullSeed);

/// Three columns: INT (row index), Dictionary<INT>, Dictionary<LONG>.
/// No nulls; dictionary has 6 entries.
omniruntime::vec::VectorBatch* MakeBatch_DictionaryIntLong(int rowCount);

#endif  // SPARK_THESTRAL_PLUGIN_ARROW_TEST_VECTORS_H
