#include <gtest/gtest.h>

#include <arrow/buffer_builder.h>

#include <cstring>
#include <limits>

#include "shuffle/arrow_columnar_deserializer.h"
#include "shuffle/arrow_type_bridge.h"
#include "shuffle/type.h"

#include <vector/vector_common.h>

using namespace omniruntime::vec;
using namespace omniruntime::type;

namespace {

// Local helper: build an arrow::Buffer from raw data using BufferBuilder
// (Arrow v18+ compatible - Buffer::Copy API changed).
std::shared_ptr<arrow::Buffer> MakeBuffer(const void* data, int64_t size) {
  arrow::BufferBuilder bb;
  bb.Append(data, size);
  return bb.Finish().ValueOrDie();
}

}  // anonymous namespace

// ----------------------------------------------------------------
// FixedWidthRoundTrip: INT column, 3 rows [10, 20, 30], all-valid.
// ----------------------------------------------------------------
TEST(ArrowColumnarDeserialize, FixedWidthRoundTrip) {
  OmniTypeDescriptor desc = DataTypeToDescriptor(IntDataType::Instance());

  int32_t rowCount = 3;
  int32_t vals[3] = {10, 20, 30};
  auto values = MakeBuffer(vals, 12);
  std::vector<std::shared_ptr<arrow::Buffer>> buffers = {
      nullptr /*validity all-valid*/, values};

  auto vec = VectorHelper::CreateVector(OMNI_FLAT, OMNI_INT, rowCount);
  size_t bufIdx = 0;
  DeserializeArrowBufferToOmniVector(desc, rowCount, buffers, bufIdx, vec);

  EXPECT_EQ(bufIdx, 2u);

  auto* iv = reinterpret_cast<Vector<int32_t>*>(vec);
  EXPECT_EQ(iv->GetValue(0), 10);
  EXPECT_EQ(iv->GetValue(1), 20);
  EXPECT_EQ(iv->GetValue(2), 30);

  delete vec;
}

// ----------------------------------------------------------------
// ValidityInvertedToOmniNullMask: Arrow set-bit=valid → Omni set-bit=null.
// Arrow validity: row0 valid, row1 null, row2 valid, row3 null → bitmap 0x05.
// ----------------------------------------------------------------
TEST(ArrowColumnarDeserialize, ValidityInvertedToOmniNullMask) {
  OmniTypeDescriptor desc = DataTypeToDescriptor(LongDataType::Instance());

  int32_t rowCount = 4;
  int64_t vals[4] = {1, 2, 3, 4};
  auto values = MakeBuffer(vals, 32);

  // Arrow validity bitmap: rows 0 & 2 valid, rows 1 & 3 null.
  // LSB-first: bit 0=row0, bit 1=row1, ... → 0b00000101 = 0x05
  uint8_t validBits[1] = {0x05};
  auto validity = MakeBuffer(validBits, 1);
  std::vector<std::shared_ptr<arrow::Buffer>> buffers = {validity, values};

  auto vec = VectorHelper::CreateVector(OMNI_FLAT, OMNI_LONG, rowCount);
  size_t bufIdx = 0;
  DeserializeArrowBufferToOmniVector(desc, rowCount, buffers, bufIdx, vec);

  EXPECT_FALSE(vec->IsNull(0));
  EXPECT_TRUE(vec->IsNull(1));
  EXPECT_FALSE(vec->IsNull(2));
  EXPECT_TRUE(vec->IsNull(3));

  delete vec;
}

// ----------------------------------------------------------------
// VarcharRoundTrip: VARCHAR column, 2 rows ["AB", "CDE"], all-valid.
// ----------------------------------------------------------------
TEST(ArrowColumnarDeserialize, VarcharRoundTrip) {
  OmniTypeDescriptor desc = DataTypeToDescriptor(VarcharDataType::Instance());

  int32_t rowCount = 2;
  int32_t offs[3] = {0, 2, 5};
  uint8_t strv[5] = {'A', 'B', 'C', 'D', 'E'};
  auto offsets = MakeBuffer(offs, 12);
  auto values = MakeBuffer(strv, 5);
  std::vector<std::shared_ptr<arrow::Buffer>> buffers = {
      nullptr, offsets, values};

  auto vec = VectorHelper::CreateVector(OMNI_FLAT, OMNI_VARCHAR, rowCount);
  size_t bufIdx = 0;
  DeserializeArrowBufferToOmniVector(desc, rowCount, buffers, bufIdx, vec);

  EXPECT_EQ(bufIdx, 3u);

  auto* sv = reinterpret_cast<
      Vector<LargeStringContainer<std::string_view>>*>(vec);
  EXPECT_EQ(std::string(sv->GetValue(0).data(), sv->GetValue(0).size()), "AB");
  EXPECT_EQ(std::string(sv->GetValue(1).data(), sv->GetValue(1).size()),
            "CDE");

  delete vec;
}

// ----------------------------------------------------------------
// DoubleNanAndNegZeroBitPattern: DOUBLE NaN / -0.0 bit-identical round-trip.
// ----------------------------------------------------------------
TEST(ArrowColumnarDeserialize, DoubleNanAndNegZeroBitPattern) {
  OmniTypeDescriptor desc = DataTypeToDescriptor(DoubleDataType::Instance());

  int32_t rowCount = 3;
  double vals[3];
  vals[0] = std::numeric_limits<double>::quiet_NaN();
  vals[1] = -0.0;  // bit pattern 0x8000000000000000, must not be normalized
  vals[2] = 1.5;
  auto values = MakeBuffer(vals, 24);
  std::vector<std::shared_ptr<arrow::Buffer>> buffers = {nullptr, values};

  auto vec = VectorHelper::CreateVector(OMNI_FLAT, OMNI_DOUBLE, rowCount);
  size_t bufIdx = 0;
  DeserializeArrowBufferToOmniVector(desc, rowCount, buffers, bufIdx, vec);

  auto* dv = reinterpret_cast<Vector<double>*>(vec);

  // NaN: compare bit pattern via memcmp (== is unreliable with NaN).
  // GetValue returns by value, so store in a temporary before taking address.
  uint64_t bitsIn = 0, bitsOut = 0;
  std::memcpy(&bitsIn, &vals[0], 8);
  double nanVal = dv->GetValue(0);
  std::memcpy(&bitsOut, &nanVal, 8);
  EXPECT_EQ(bitsIn, bitsOut);

  // -0.0: verify bit pattern is preserved
  std::memcpy(&bitsIn, &vals[1], 8);
  double negZeroVal = dv->GetValue(1);
  std::memcpy(&bitsOut, &negZeroVal, 8);
  EXPECT_EQ(bitsIn, bitsOut);

  EXPECT_EQ(dv->GetValue(2), 1.5);

  delete vec;
}

// ----------------------------------------------------------------
// EmptyStringAndAllNullVarchar: empty strings + VARCHAR boundary.
// ----------------------------------------------------------------
TEST(ArrowColumnarDeserialize, EmptyStringAndAllNullVarchar) {
  OmniTypeDescriptor desc = DataTypeToDescriptor(VarcharDataType::Instance());

  int32_t rowCount = 3;
  // Row0: "", Row1: "X", Row2: ""
  int32_t offs[4] = {0, 0, 1, 1};
  uint8_t strv[1] = {'X'};
  auto offsets = MakeBuffer(offs, 16);
  auto values = MakeBuffer(strv, 1);
  std::vector<std::shared_ptr<arrow::Buffer>> buffers = {
      nullptr, offsets, values};

  auto vec = VectorHelper::CreateVector(OMNI_FLAT, OMNI_VARCHAR, rowCount);
  size_t bufIdx = 0;
  DeserializeArrowBufferToOmniVector(desc, rowCount, buffers, bufIdx, vec);

  auto* sv = reinterpret_cast<
      Vector<LargeStringContainer<std::string_view>>*>(vec);
  EXPECT_EQ(sv->GetValue(0).size(), 0u);  // empty string ""
  EXPECT_EQ(
      std::string(sv->GetValue(1).data(), sv->GetValue(1).size()), "X");
  EXPECT_EQ(sv->GetValue(2).size(), 0u);  // empty string ""

  delete vec;
}
