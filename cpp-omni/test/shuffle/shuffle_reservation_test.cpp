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

#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>

#include <algorithm>
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
