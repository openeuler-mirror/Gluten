/**
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

#include "gtest/gtest.h"
#include "../utils/test_utils.h"
#include "jni/OmniBlockAllocationListener.h"
#include "memory/AllocationListener.h"
#include "memory/omni_task_memory_pool.h"
#include "memory/OmniPoolAllocationListener.h"
#include "memory/reservation_denied.h"
#include "common/OmniStlAllocator.h"
#include "shuffle/arrow_frame.h"

#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>

#include <algorithm>
#include <cstring>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>

namespace {

TEST(OmniStlAllocatorTest, RejectsOverflowBeforeAllocating)
{
    OmniStlAllocator<int64_t> allocator;
    EXPECT_THROW(allocator.allocate(std::numeric_limits<std::size_t>::max()), std::bad_array_new_length);
    EXPECT_THROW(OmniStlAllocBytes(nullptr, std::numeric_limits<std::size_t>::max()), std::bad_alloc);
}

TEST(OmniStlAllocatorTest, NoPoolStorageRemainsUsable)
{
    std::vector<int64_t, OmniStlAllocator<int64_t>> values;
    for (int64_t i = 0; i < 100; ++i) {
        values.push_back(i);
    }
    for (int64_t i = 0; i < 100; ++i) {
        EXPECT_EQ(values[i], i);
    }
}

constexpr int64_t kMiB = int64_t{1} << 20;
std::string tmpTestingDir;
std::string tmpShuffleFilePath;

class ShuffleReservationTest : public testing::Test {
protected:
    static void SetUpTestSuite()
    {
        tmpTestingDir = s_shuffle_tests_dir;
        if (!IsFileExist(tmpTestingDir)) {
            mkdir(tmpTestingDir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
        }
    }

    void TearDown() override
    {
        if (IsFileExist(tmpShuffleFilePath)) {
            remove(tmpShuffleFilePath.c_str());
        }
    }
};

class CappedAllocationListener final : public gluten::AllocationListener {
public:
    explicit CappedAllocationListener(int64_t capacity) : capacity_(capacity) {}

    void allocationChanged(int64_t diff) override
    {
        ++callCount_;
        if (diff > 0 && current_ > capacity_ - diff) {
            throw std::runtime_error("test memory capacity exceeded");
        }
        current_ += diff;
        peak_ = std::max(peak_, current_);
    }

    int64_t currentBytes() override { return current_; }
    int64_t peakBytes() override { return peak_; }
    int64_t callCount() const { return callCount_; }

private:
    int64_t capacity_;
    int64_t current_{0};
    int64_t peak_{0};
    int64_t callCount_{0};
};

// While denyGrowth is set, denies growth above the usage it was set at, like Spark while it is
// spilling this task.
class DenyGrowthAllocationListener final : public gluten::AllocationListener {
public:
    void DenyGrowth(bool deny)
    {
        denyGrowth_ = deny;
        ceiling_ = current_;
    }

    void allocationChanged(int64_t diff) override
    {
        if (diff > 0 && denyGrowth_ && current_ + diff > ceiling_) {
            throw omniruntime::mem::ReservationDenied("test growth denied during reclaim");
        }
        current_ += diff;
    }

    int64_t currentBytes() override { return current_; }

private:
    bool denyGrowth_{false};
    int64_t ceiling_{0};
    int64_t current_{0};
};

class RejectFirstPositiveAllocationListener final : public gluten::AllocationListener {
public:
    void allocationChanged(int64_t diff) override
    {
        if (diff > 0 && !rejected_) {
            rejected_ = true;
            // The pool treats only ReservationDenied as an advisory denial;
            // any other exception is fatal by contract. Simulate the typed
            // Spark denial so the best-effort paths are exercised.
            throw omniruntime::mem::ReservationDenied("test speculative reservation denied");
        }
        current_ += diff;
        peak_ = std::max(peak_, current_);
    }

    int64_t currentBytes() override { return current_; }
    int64_t peakBytes() override { return peak_; }
    bool rejected() const { return rejected_; }

private:
    bool rejected_{false};
    int64_t current_{0};
    int64_t peak_{0};
};

std::unique_ptr<Splitter> MakeFixedWidthSplitter(
    int32_t numPartitions,
    const std::string &dataFile,
    omniruntime::mem::OmniTaskMemoryPool *pool)
{
    static int32_t inputVecTypeIds[] = {
        OMNI_BOOLEAN, OMNI_SHORT, OMNI_INT, OMNI_LONG, OMNI_DOUBLE};
    static uint32_t precisions[5] = {};
    static uint32_t scales[5] = {};
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = precisions;
    inputDataTypes.inputDataScales = scales;

    setenv("NATIVESQL_SPARK_LOCAL_DIRS", tmpTestingDir.c_str(), 1);
    auto options = SplitOptions::Defaults();
    options.buffer_size = 4096;
    options.compression_type = GetCompressionType("lz4");
    options.data_file = dataFile;
    return std::unique_ptr<Splitter>(Splitter::Make(
        "hash", inputDataTypes, 5, numPartitions, std::move(options), pool));
}

std::unique_ptr<Splitter> MakeWideLongSplitter(
    int32_t numPartitions,
    const std::string &dataFile,
    omniruntime::mem::OmniTaskMemoryPool *pool)
{
    static int32_t inputVecTypeIds[10] = {
        OMNI_LONG, OMNI_LONG, OMNI_LONG, OMNI_LONG, OMNI_LONG,
        OMNI_LONG, OMNI_LONG, OMNI_LONG, OMNI_LONG, OMNI_LONG};
    static uint32_t precisions[10] = {};
    static uint32_t scales[10] = {};
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = precisions;
    inputDataTypes.inputDataScales = scales;

    setenv("NATIVESQL_SPARK_LOCAL_DIRS", tmpTestingDir.c_str(), 1);
    auto options = SplitOptions::Defaults();
    options.buffer_size = 4096;
    options.compression_type = GetCompressionType("lz4");
    options.data_file = dataFile;
    return std::unique_ptr<Splitter>(Splitter::Make(
        "hash", inputDataTypes, 10, numPartitions, std::move(options), pool));
}

std::unique_ptr<Splitter> MakeVarcharSplitter(
    int32_t numPartitions,
    const std::string &dataFile,
    omniruntime::mem::OmniTaskMemoryPool *pool)
{
    static int32_t inputVecTypeIds[2] = {OMNI_VARCHAR, OMNI_LONG};
    static uint32_t precisions[2] = {256, 0};
    static uint32_t scales[2] = {};
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = precisions;
    inputDataTypes.inputDataScales = scales;

    setenv("NATIVESQL_SPARK_LOCAL_DIRS", tmpTestingDir.c_str(), 1);
    auto options = SplitOptions::Defaults();
    options.buffer_size = 4096;
    options.compression_type = GetCompressionType("uncompressed"); // CountColumnarRows reads raw frames
    options.data_file = dataFile;
    return std::unique_ptr<Splitter>(Splitter::Make(
        "hash", inputDataTypes, 2, numPartitions, std::move(options), pool));
}

VectorBatch *CreateVarcharBatch(int32_t numPartitions, int32_t numRows, int32_t width)
{
    auto *batch = new VectorBatch(numRows);
    auto *partitionIds = new Vector<int32_t>(numRows);
    auto *strings = new Vector<LargeStringContainer<std::string_view>>(numRows);
    auto *longs = new Vector<int64_t>(numRows);
    std::string value(width, 'v');
    for (int32_t row = 0; row < numRows; ++row) {
        partitionIds->SetValue(row, row % numPartitions);
        const std::string digits = std::to_string(row);
        value.replace(0, digits.size(), digits);
        strings->SetValue(row, std::string_view(value));
        longs->SetValue(row, row);
    }
    batch->Append(partitionIds);
    batch->Append(strings);
    batch->Append(longs);
    return batch;
}

// Rows in a columnar shuffle data file: each batch is [4-byte size][file header][batch frame].
int64_t CountColumnarRows(const std::string &path)
{
    std::ifstream file(path, std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
        return -1;
    }
    std::vector<uint8_t> bytes(static_cast<size_t>(file.tellg()));
    file.seekg(0);
    file.read(reinterpret_cast<char *>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    const auto size = static_cast<int64_t>(bytes.size());
    int64_t consumed = 0;
    int64_t rows = 0;
    while (consumed + 4 <= size) {
        consumed += 4;
        int64_t headerConsumed = 0;
        auto header = ReadFileHeader(bytes.data() + consumed, size - consumed, &headerConsumed);
        if (!header.ok()) {
            return -1;
        }
        consumed += headerConsumed;
        int64_t batchConsumed = 0;
        auto batch = ReadColumnarBatch(bytes.data() + consumed, size - consumed, (*header).schema, &batchConsumed);
        if (!batch.ok()) {
            return -1;
        }
        consumed += batchConsumed;
        rows += (*batch).rowCount;
    }
    return consumed == size ? rows : -1;
}

std::unique_ptr<Splitter> MakeIntLongSplitter(
    int32_t numPartitions,
    const std::string &dataFile,
    omniruntime::mem::OmniTaskMemoryPool *pool,
    int64_t taskOffHeapBytes)
{
    static int32_t inputVecTypeIds[2] = {OMNI_INT, OMNI_LONG};
    static uint32_t precisions[2] = {};
    static uint32_t scales[2] = {};
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = precisions;
    inputDataTypes.inputDataScales = scales;

    setenv("NATIVESQL_SPARK_LOCAL_DIRS", tmpTestingDir.c_str(), 1);
    auto options = SplitOptions::Defaults();
    options.buffer_size = 4096;
    options.compression_type = GetCompressionType("uncompressed"); // ForEachShuffledIntLongRow reads raw frames
    options.data_file = dataFile;
    options.task_offheap_bytes = taskOffHeapBytes;
    return std::unique_ptr<Splitter>(Splitter::Make(
        "hash", inputDataTypes, 2, numPartitions, std::move(options), pool));
}

// Row ids firstRow.. go to partition id % numPartitions. The INT column holds the id, null when
// nullEvery > 0 divides it; the LONG column holds 3 x id.
VectorBatch *CreateIntLongBatch(int32_t numPartitions, int32_t firstRow, int32_t numRows, int32_t nullEvery)
{
    auto *batch = new VectorBatch(numRows);
    auto *partitionIds = new Vector<int32_t>(numRows);
    auto *ints = new Vector<int32_t>(numRows);
    auto *longs = new Vector<int64_t>(numRows);
    for (int32_t row = 0; row < numRows; ++row) {
        const int32_t id = firstRow + row;
        partitionIds->SetValue(row, id % numPartitions);
        if (nullEvery > 0 && id % nullEvery == 0) {
            ints->SetNull(row);
        } else {
            ints->SetValue(row, id);
        }
        longs->SetValue(row, int64_t{3} * id);
    }
    batch->Append(partitionIds);
    batch->Append(ints);
    batch->Append(longs);
    return batch;
}

// Calls visit(partition, id, intIsNull, intValue) for every row of an uncompressed INT/LONG shuffle
// data file, where id is the LONG value / 3. Returns the number of batches, or -1 when the file
// does not parse into the splitter's partition lengths.
int64_t ForEachShuffledIntLongRow(const std::string &path, const std::vector<int64_t> &partitionLengths,
    const std::function<void(int32_t, int64_t, bool, int32_t)> &visit)
{
    std::ifstream file(path, std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
        return -1;
    }
    std::vector<uint8_t> bytes(static_cast<size_t>(file.tellg()));
    file.seekg(0);
    file.read(reinterpret_cast<char *>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    int64_t batches = 0;
    int64_t partitionStart = 0;
    for (int32_t pid = 0; pid < static_cast<int32_t>(partitionLengths.size()); ++pid) {
        const int64_t end = partitionStart + partitionLengths[pid];
        int64_t consumed = partitionStart;
        while (consumed < end) {
            consumed += 4;
            int64_t headerConsumed = 0;
            auto header = ReadFileHeader(bytes.data() + consumed, end - consumed, &headerConsumed);
            if (!header.ok()) {
                return -1;
            }
            consumed += headerConsumed;
            int64_t batchConsumed = 0;
            auto batch = ReadColumnarBatch(bytes.data() + consumed, end - consumed, (*header).schema, &batchConsumed);
            if (!batch.ok()) {
                return -1;
            }
            consumed += batchConsumed;
            ++batches;
            // Buffers: INT [validity][values], LONG [validity][values]. A null validity is all valid.
            const auto &buffers = (*batch).buffers;
            const uint8_t *intValidity = buffers[0] != nullptr ? buffers[0]->data() : nullptr;
            const auto *ints = reinterpret_cast<const int32_t *>(buffers[1]->data());
            const auto *longs = reinterpret_cast<const int64_t *>(buffers[3]->data());
            for (int32_t row = 0; row < (*batch).rowCount; ++row) {
                const bool isNull = intValidity != nullptr && (intValidity[row / 8] & (1u << (row % 8))) == 0;
                visit(pid, longs[row] / 3, isNull, ints[row]);
            }
        }
        if (consumed != end) {
            return -1;
        }
        partitionStart = end;
    }
    return partitionStart == static_cast<int64_t>(bytes.size()) ? batches : -1;
}

VectorBatch *CreateWideLongBatch(int32_t numPartitions, int32_t numRows)
{
    auto *batch = new VectorBatch(numRows);
    auto *partitionIds = new Vector<int32_t>(numRows);
    for (int32_t row = 0; row < numRows; ++row) {
        partitionIds->SetValue(row, numPartitions > 0 ? row % numPartitions : 0);
    }
    batch->Append(partitionIds);
    for (int32_t column = 0; column < 10; ++column) {
        auto *values = new Vector<int64_t>(numRows);
        for (int32_t row = 0; row < numRows; ++row) {
            values->SetValue(row, static_cast<int64_t>(column) * numRows + row);
        }
        batch->Append(values);
    }
    return batch;
}

} // namespace

TEST_F(ShuffleReservationTest, TeardownRetainsReservationWhenRepaymentFails)
{
    class ThrowingRepaymentListener final : public gluten::AllocationListener {
    public:
        bool rejectRepayment = false;
        int64_t current = 0;
        void allocationChanged(int64_t diff) override
        {
            if (diff < 0 && rejectRepayment) throw std::runtime_error("repayment failed");
            current += diff;
        }
        int64_t currentBytes() override { return current; }
    } listener;
    gluten::OmniPoolAllocationListener poolListenerBridge(listener);
    omniruntime::mem::OmniTaskMemoryPool pool(&poolListenerBridge);
    tmpShuffleFilePath = tmpTestingDir + "/shuffle_teardown_repayment_failure";
    auto splitter = MakeFixedWidthSplitter(2, tmpShuffleFilePath, &pool);
    ASSERT_TRUE(pool.reserve(kMiB));
    listener.rejectRepayment = true;
    splitter.reset();
    EXPECT_EQ(pool.currentBytes(), 0);
    EXPECT_GT(pool.reservedBytes(), 0);
    EXPECT_EQ(pool.reservedBytes(), listener.current);
    listener.rejectRepayment = false;
    pool.releaseUnusedReservation();
    EXPECT_EQ(listener.current, 0);
}

TEST_F(ShuffleReservationTest, PreReservationAllowsQ78ShapedFixedWidthBatch)
{
    tmpShuffleFilePath = tmpTestingDir + "/shuffle_split_pre_reservation_q78_shape";
    CappedAllocationListener delegate(8 * kMiB);
    gluten::OmniBlockAllocationListener blockListener(&delegate);
    gluten::OmniPoolAllocationListener poolListenerBridge(blockListener);
    omniruntime::mem::OmniTaskMemoryPool pool(&poolListenerBridge);
    auto splitter = MakeFixedWidthSplitter(48, tmpShuffleFilePath, &pool);

    auto *vb = CreateVectorBatch_5fixedCols_withPid(48, 999);
    EXPECT_NO_THROW(splitter->Split(*vb));
    EXPECT_GT(pool.currentBytes(), 0);
    EXPECT_LE(delegate.peakBytes(), 8 * kMiB);

    EXPECT_NO_THROW(splitter->Stop());
    splitter.reset();
    EXPECT_EQ(pool.currentBytes(), 0);
    EXPECT_EQ(delegate.currentBytes(), 0);
}

TEST_F(ShuffleReservationTest, PreReservationDenialIsBestEffort)
{
    tmpShuffleFilePath = tmpTestingDir + "/shuffle_split_pre_reservation_denied";
    RejectFirstPositiveAllocationListener delegate;
    gluten::OmniBlockAllocationListener blockListener(&delegate);
    gluten::OmniPoolAllocationListener poolListenerBridge(blockListener);
    omniruntime::mem::OmniTaskMemoryPool pool(&poolListenerBridge);
    auto splitter = MakeFixedWidthSplitter(48, tmpShuffleFilePath, &pool);

    auto *vb = CreateVectorBatch_5fixedCols_withPid(48, 999);
    EXPECT_NO_THROW(splitter->Split(*vb));
    EXPECT_TRUE(delegate.rejected());

    EXPECT_NO_THROW(splitter->Stop());
    splitter.reset();
    EXPECT_EQ(pool.currentBytes(), 0);
    EXPECT_EQ(pool.reservedBytes(), 0);
    EXPECT_EQ(delegate.currentBytes(), 0);
}

TEST_F(ShuffleReservationTest, WidePreReservationDenialIsBestEffort)
{
    tmpShuffleFilePath = tmpTestingDir + "/shuffle_split_pre_reservation_wide_fixed";
    RejectFirstPositiveAllocationListener delegate;
    gluten::OmniBlockAllocationListener blockListener(&delegate);
    gluten::OmniPoolAllocationListener poolListenerBridge(blockListener);
    omniruntime::mem::OmniTaskMemoryPool pool(&poolListenerBridge);
    auto splitter = MakeWideLongSplitter(48, tmpShuffleFilePath, &pool);

    auto *vb = CreateWideLongBatch(48, 999);
    EXPECT_NO_THROW(splitter->Split(*vb));
    EXPECT_TRUE(delegate.rejected());

    EXPECT_NO_THROW(splitter->Stop());
    splitter.reset();
    EXPECT_EQ(pool.currentBytes(), 0);
    EXPECT_EQ(pool.reservedBytes(), 0);
    EXPECT_EQ(delegate.currentBytes(), 0);
}

TEST_F(ShuffleReservationTest, AllowsWideFixedWidthFanoutAtEstimate)
{
    tmpShuffleFilePath = tmpTestingDir + "/shuffle_split_pre_reservation_wide_allowed";
    CappedAllocationListener delegate(16 * kMiB);
    gluten::OmniBlockAllocationListener blockListener(&delegate);
    gluten::OmniPoolAllocationListener poolListenerBridge(blockListener);
    omniruntime::mem::OmniTaskMemoryPool pool(&poolListenerBridge);
    auto splitter = MakeWideLongSplitter(48, tmpShuffleFilePath, &pool);

    auto *vb = CreateWideLongBatch(48, 999);
    EXPECT_NO_THROW(splitter->Split(*vb));
    EXPECT_GT(pool.currentBytes(), 8 * kMiB);
    EXPECT_LE(delegate.peakBytes(), 16 * kMiB);

    EXPECT_NO_THROW(splitter->Stop());
    splitter.reset();
    EXPECT_EQ(pool.currentBytes(), 0);
    EXPECT_EQ(delegate.currentBytes(), 0);
}

TEST_F(ShuffleReservationTest, SkipsReserveWhenFixedBuffersHaveRoom)
{
    tmpShuffleFilePath = tmpTestingDir + "/shuffle_split_pre_reservation_reuse";
    CappedAllocationListener delegate(8 * kMiB);
    gluten::OmniBlockAllocationListener blockListener(&delegate);
    gluten::OmniPoolAllocationListener poolListenerBridge(blockListener);
    omniruntime::mem::OmniTaskMemoryPool pool(&poolListenerBridge);
    auto splitter = MakeFixedWidthSplitter(48, tmpShuffleFilePath, &pool);

    EXPECT_NO_THROW(splitter->Split(*CreateVectorBatch_5fixedCols_withPid(48, 999)));
    const int64_t callsAfterFirstBatch = delegate.callCount();
    EXPECT_NO_THROW(splitter->Split(*CreateVectorBatch_5fixedCols_withPid(48, 999)));
    EXPECT_EQ(splitter->TotalBytesSpilled(), 0);
    EXPECT_LE(delegate.peakBytes(), 8 * kMiB);
    EXPECT_EQ(delegate.callCount(), callsAfterFirstBatch)
        << "reused buffers must retain bounded reservation headroom between batches";

    EXPECT_NO_THROW(splitter->Stop());
    EXPECT_EQ(pool.availableReservation(), 0)
        << "native stop must return batch-reuse headroom before the writer is destroyed";
    splitter.reset();
    EXPECT_EQ(pool.currentBytes(), 0);
    EXPECT_EQ(delegate.currentBytes(), 0);
}

TEST_F(ShuffleReservationTest, DestructorRepaysReservationWithoutStop)
{
    tmpShuffleFilePath = tmpTestingDir + "/shuffle_split_cancel_cleanup";
    CappedAllocationListener delegate(8 * kMiB);
    gluten::OmniBlockAllocationListener blockListener(&delegate);
    gluten::OmniPoolAllocationListener poolListenerBridge(blockListener);
    omniruntime::mem::OmniTaskMemoryPool pool(&poolListenerBridge);
    auto splitter = MakeFixedWidthSplitter(48, tmpShuffleFilePath, &pool);

    EXPECT_NO_THROW(splitter->Split(*CreateVectorBatch_5fixedCols_withPid(48, 999)));
    EXPECT_GT(pool.currentBytes(), 0);
    EXPECT_GT(pool.reservedBytes(), 0);

    // Cancellation closes the native writer without a preceding Stop().  All
    // Arrow buffers must be destroyed before the writer returns its unused
    // Spark reservation, otherwise reservedBytes remains pinned until the
    // task-wide memory manager itself is destroyed.
    splitter.reset();
    EXPECT_EQ(pool.currentBytes(), 0);
    EXPECT_EQ(pool.reservedBytes(), 0);
    EXPECT_EQ(delegate.currentBytes(), 0);
}

TEST_F(ShuffleReservationTest, RowFanoutReservesForRowsInsteadOfColumnarCapacity)
{
    for (int partitions : {200, 2000}) {
        tmpShuffleFilePath = tmpTestingDir + "/shuffle_row_fanout_reservation";
        CappedAllocationListener delegate(16 * kMiB);
        gluten::OmniBlockAllocationListener blockListener(&delegate);
        gluten::OmniPoolAllocationListener poolListenerBridge(blockListener);
        omniruntime::mem::OmniTaskMemoryPool pool(&poolListenerBridge);
        auto splitter = MakeWideLongSplitter(partitions, tmpShuffleFilePath, &pool);
        // Touch every partition. The old columnar estimate requests roughly
        // 65/655 MiB, although each row arena starts with only a 4 KiB chunk.
        EXPECT_NO_THROW(splitter->SplitByRow(CreateWideLongBatch(partitions, partitions)));
        EXPECT_GT(pool.currentBytes(), 0);
        EXPECT_LE(delegate.peakBytes(), 16 * kMiB);
        EXPECT_NO_THROW(splitter->StopByRow());
        splitter.reset();
        EXPECT_EQ(pool.currentBytes(), 0);
        EXPECT_EQ(delegate.currentBytes(), 0);
    }
}

TEST_F(ShuffleReservationTest, RowFanoutLaterBatchesReserveFromObservedRowSize)
{
    // Only the first row batch is measured row by row; later batches size the
    // pre-reservation from the observed average row length. The result must
    // stay within the same bound and still drain to zero.
    tmpShuffleFilePath = tmpTestingDir + "/shuffle_row_fanout_estimate";
    constexpr int partitions = 200;
    CappedAllocationListener delegate(16 * kMiB);
    gluten::OmniBlockAllocationListener blockListener(&delegate);
    gluten::OmniPoolAllocationListener poolListenerBridge(blockListener);
    omniruntime::mem::OmniTaskMemoryPool pool(&poolListenerBridge);
    auto splitter = MakeWideLongSplitter(partitions, tmpShuffleFilePath, &pool);
    for (int batch = 0; batch < 3; ++batch) {
        EXPECT_NO_THROW(splitter->SplitByRow(CreateWideLongBatch(partitions, partitions)));
    }
    EXPECT_GT(pool.currentBytes(), 0);
    EXPECT_LE(delegate.peakBytes(), 16 * kMiB);
    EXPECT_NO_THROW(splitter->StopByRow());
    splitter.reset();
    EXPECT_EQ(pool.currentBytes(), 0);
    EXPECT_EQ(delegate.currentBytes(), 0);
}

TEST_F(ShuffleReservationTest, ReclaimSpillBuildsBuffersOutsideTheTaskPool)
{
    // 64 partitions x 625 rows of 200-byte strings: 8 MiB of string bodies, below buffer_size, so
    // nothing has been cached before reclaim. Reclaim runs inside Spark's spill of this task with
    // no headroom left, so any growth of the task pool there is denied (mt_case q3 stage 9).
    tmpShuffleFilePath = tmpTestingDir + "/varchar-reclaim-spill";
    DenyGrowthAllocationListener delegate;
    gluten::OmniPoolAllocationListener poolListenerBridge(delegate);
    omniruntime::mem::OmniTaskMemoryPool pool(&poolListenerBridge);
    constexpr int32_t kPartitions = 64;
    constexpr int32_t kBatches = 4;
    constexpr int32_t kRowsPerBatch = 10000;
    auto splitter = MakeVarcharSplitter(kPartitions, tmpShuffleFilePath, &pool);
    for (int32_t batch = 0; batch < kBatches; ++batch) {
        auto *vb = CreateVarcharBatch(kPartitions, kRowsPerBatch, 200);
        ASSERT_EQ(splitter->Split(*vb), 0);
    }

    // Spark spills a task when it has no headroom left: drop the split pre-reservation so any
    // growth during reclaim reaches the listener.
    pool.releaseUnusedReservation();
    const int64_t before = pool.currentBytes();
    const int64_t peakBefore = pool.peakBytes();
    ASSERT_GT(splitter->reclaimableBytes(), 0u);
    delegate.DenyGrowth(true);
    int64_t freed = 0;
    EXPECT_NO_THROW(freed = splitter->reclaim(1));
    delegate.DenyGrowth(false);
    EXPECT_GT(freed, 0);
    EXPECT_EQ(pool.peakBytes(), peakBefore) << "reclaim grew the task pool from " << before;
    EXPECT_GT(splitter->TotalBytesSpilled(), 0);
    EXPECT_GT(splitter->ReclaimSpillScratchPeakBytes(), 0);

    EXPECT_NO_THROW(splitter->Stop());
    EXPECT_EQ(CountColumnarRows(tmpShuffleFilePath), int64_t{kBatches} * kRowsPerBatch);
    splitter.reset();
    EXPECT_EQ(pool.currentBytes(), 0);
}

TEST_F(ShuffleReservationTest, ForeignThreadReclaimDoesNotWaitForReservationCallback)
{
    class CallbackListener final : public gluten::AllocationListener {
    public:
        std::function<void()> onBorrow;
        void allocationChanged(int64_t delta) override {
            if (delta > 0 && onBorrow) {
                auto callback = std::move(onBorrow);
                onBorrow = nullptr;
                callback();
            }
        }
    } listener;
    gluten::OmniPoolAllocationListener bridge(listener);
    omniruntime::mem::OmniTaskMemoryPool pool(&bridge);
    tmpShuffleFilePath = tmpTestingDir + "/foreign-reclaim-callback";
    auto splitter = MakeWideLongSplitter(2, tmpShuffleFilePath, &pool);
    std::mutex mutex;
    std::condition_variable cv;
    bool completed = false;
    bool completedDuringCallback = false;
    uint64_t estimate = 1;
    int64_t freed = 1;
    std::thread worker;
    listener.onBorrow = [&] {
        worker = std::thread([&] {
            estimate = splitter->reclaimableBytes();
            freed = splitter->reclaim(1);
            {
                std::lock_guard<std::mutex> lock(mutex);
                completed = true;
            }
            cv.notify_one();
        });
        std::unique_lock<std::mutex> lock(mutex);
        completedDuringCallback = cv.wait_for(lock, std::chrono::seconds(2), [&] { return completed; });
    };
    auto *batch = CreateWideLongBatch(2, 64);
    try {
        splitter->Split(*batch);
    } catch (...) {
        if (worker.joinable()) worker.join();
        throw;
    }
    if (worker.joinable()) worker.join();
    EXPECT_TRUE(completedDuringCallback);
    EXPECT_EQ(estimate, 0);
    EXPECT_EQ(freed, 0);
    splitter->Stop();
}

TEST_F(ShuffleReservationTest, SmallTaskShareGrowsPartitionBuffersWithRows)
{
    // Full buffers for 600 partitions are 600 x 4096 rows x 14 bytes = 34 MB, mostly empty after
    // one batch. That is more than a quarter of a 64 MB task share, so the buffers start small
    // instead; with no share known they stay full size.
    constexpr int32_t kPartitions = 600;
    for (const int64_t taskOffHeapBytes : {int64_t{0}, 64 * kMiB}) {
        tmpShuffleFilePath = tmpTestingDir + "/shuffle_growable_partition_buffers";
        CappedAllocationListener delegate(256 * kMiB);
        gluten::OmniBlockAllocationListener blockListener(&delegate);
        gluten::OmniPoolAllocationListener poolListenerBridge(blockListener);
        omniruntime::mem::OmniTaskMemoryPool pool(&poolListenerBridge);
        auto splitter = MakeIntLongSplitter(kPartitions, tmpShuffleFilePath, &pool, taskOffHeapBytes);
        ASSERT_EQ(splitter->Split(*CreateIntLongBatch(kPartitions, 0, 2 * kPartitions, 0)), 0);
        if (taskOffHeapBytes == 0) {
            EXPECT_GT(pool.currentBytes(), 16 * kMiB);
        } else {
            EXPECT_LT(pool.currentBytes(), 2 * kMiB);
        }
        EXPECT_NO_THROW(splitter->Stop());
        splitter.reset();
        EXPECT_EQ(pool.currentBytes(), 0);
    }
}

TEST_F(ShuffleReservationTest, GrowablePartitionBuffersKeepEveryRow)
{
    // 200 rows per partition per batch: each partition's rows go through buffer growth, nulls that
    // first appear when a buffer already holds rows, a reclaim spill, and full buffers that are
    // cached and replaced.
    tmpShuffleFilePath = tmpTestingDir + "/shuffle_growable_partition_rows";
    CappedAllocationListener delegate(256 * kMiB);
    gluten::OmniBlockAllocationListener blockListener(&delegate);
    gluten::OmniPoolAllocationListener poolListenerBridge(blockListener);
    omniruntime::mem::OmniTaskMemoryPool pool(&poolListenerBridge);
    constexpr int32_t kPartitions = 50;
    constexpr int32_t kBatches = 30;
    constexpr int32_t kRowsPerBatch = 10000;
    constexpr int32_t kFirstBatchWithNulls = 5;
    constexpr int32_t kNullEvery = 7;
    auto splitter = MakeIntLongSplitter(kPartitions, tmpShuffleFilePath, &pool, kMiB);
    for (int32_t batch = 0; batch < kBatches; ++batch) {
        const int32_t nullEvery = batch < kFirstBatchWithNulls ? 0 : kNullEvery;
        ASSERT_EQ(splitter->Split(*CreateIntLongBatch(kPartitions, batch * kRowsPerBatch, kRowsPerBatch, nullEvery)), 0);
        if (batch == 12) {
            ASSERT_GT(splitter->reclaim(1), 0);
        }
    }
    EXPECT_GT(splitter->TotalBytesSpilled(), 0);
    ASSERT_EQ(splitter->Stop(), 0);

    constexpr int64_t kRows = int64_t{kBatches} * kRowsPerBatch;
    std::vector<bool> seen(kRows, false);
    int64_t unexpected = 0;
    int64_t misplaced = 0;
    int64_t wrong = 0;
    const int64_t batches = ForEachShuffledIntLongRow(tmpShuffleFilePath, splitter->PartitionLengths(),
        [&](int32_t pid, int64_t id, bool isNull, int32_t value) {
            if (id < 0 || id >= kRows || seen[id]) {
                ++unexpected;
                return;
            }
            seen[id] = true;
            misplaced += id % kPartitions != pid;
            const bool nullExpected = id >= int64_t{kFirstBatchWithNulls} * kRowsPerBatch && id % kNullEvery == 0;
            wrong += isNull != nullExpected || (!isNull && value != id);
        });
    EXPECT_GT(batches, 0);
    EXPECT_EQ(unexpected, 0);
    EXPECT_EQ(misplaced, 0);
    EXPECT_EQ(wrong, 0);
    EXPECT_EQ(std::count(seen.begin(), seen.end(), true), kRows);
    splitter.reset();
    EXPECT_EQ(pool.currentBytes(), 0);
}
