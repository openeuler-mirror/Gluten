/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Description: broadcast storage coalescing and denial rollback regressions.
 */
#include "gtest/gtest.h"
#include "memory/OmniBroadcastAllocationListener.h"
#include <thread>
#include <vector>

namespace {
constexpr int64_t kMiB = int64_t{1} << 20;
class StorageLedger final : public gluten::AllocationListener {
public:
    int64_t used = 0;
    int64_t cap = 8 * kMiB;
    int positiveCalls = 0;
    int negativeCalls = 0;
    bool failRepay = false;
    bool failTransport = false;
    void allocationChanged(int64_t delta) override {
        if (delta > 0) {
            ++positiveCalls;
            if (failTransport) throw std::runtime_error("transport failure");
            if (delta > cap - used) throw omniruntime::mem::ReservationDenied("storage full");
        } else {
            ++negativeCalls;
            if (failRepay) throw std::runtime_error("repayment failed");
        }
        if (used + delta < 0) throw std::underflow_error("storage over-repayment");
        used += delta;
    }
};

TEST(OmniBroadcastAllocationListenerTest, ThousandsOfSmallAllocationsUseBoundedStorageCalls)
{
    StorageLedger storage;
    gluten::OmniBroadcastAllocationListener listener(storage);
    for (int i = 0; i < 4096; ++i) listener.allocationChanged(1024);
    EXPECT_EQ(storage.used, 4 * kMiB);
    EXPECT_EQ(storage.positiveCalls, 4);
    EXPECT_EQ(listener.peakBytes(), 4 * kMiB);
    for (int i = 0; i < 4096; ++i) listener.allocationChanged(-1024);
    EXPECT_EQ(storage.negativeCalls, 4);
    EXPECT_EQ(storage.used, 0);
    EXPECT_EQ(listener.currentBytes(), 0);
}

TEST(OmniBroadcastAllocationListenerTest, QuantumDenialFallsBackToActualBytes)
{
    StorageLedger storage;
    storage.cap = 1500;
    gluten::OmniBroadcastAllocationListener listener(storage);
    listener.allocationChanged(1000);
    EXPECT_EQ(storage.used, 1000);
    listener.allocationChanged(500);
    EXPECT_EQ(storage.used, 1500);
    EXPECT_THROW(listener.allocationChanged(1), omniruntime::mem::ReservationDenied);
    EXPECT_EQ(listener.currentBytes(), 1500);
    listener.allocationChanged(-500);
    listener.allocationChanged(500); // Existing exact headroom is still usable.
    EXPECT_EQ(storage.used, 1500);
    listener.allocationChanged(-1500);
    EXPECT_EQ(storage.used, 0);
}

TEST(OmniBroadcastAllocationListenerTest, FailedRepaymentPreservesAccountingForRetry)
{
    StorageLedger storage;
    gluten::OmniBroadcastAllocationListener listener(storage);
    listener.allocationChanged(1024);
    storage.failRepay = true;
    EXPECT_THROW(listener.allocationChanged(-1024), std::runtime_error);
    EXPECT_EQ(listener.currentBytes(), kMiB);
    storage.failRepay = false;
    listener.allocationChanged(-1024);
    EXPECT_EQ(storage.used, 0);
}

TEST(OmniBroadcastAllocationListenerTest, TransportFailureIsNotRetriedAsCapacityDenial)
{
    StorageLedger storage;
    storage.failTransport = true;
    gluten::OmniBroadcastAllocationListener listener(storage);
    EXPECT_THROW(listener.allocationChanged(1024), std::runtime_error);
    EXPECT_EQ(storage.positiveCalls, 1);
    EXPECT_EQ(storage.used, 0);
    EXPECT_EQ(listener.currentBytes(), 0);
}

TEST(OmniBroadcastAllocationListenerTest, ConcurrentEntriesShareOneReservationDomain)
{
    StorageLedger storage;
    gluten::OmniBroadcastAllocationListener listener(storage);
    std::vector<std::thread> workers;
    for (int entry = 0; entry < 4; ++entry) {
        workers.emplace_back([&]() {
            for (int i = 0; i < 256; ++i) listener.allocationChanged(1024);
        });
    }
    for (auto& worker : workers) worker.join();
    EXPECT_EQ(storage.used, kMiB);
    EXPECT_EQ(storage.positiveCalls, 1);
    listener.allocationChanged(-kMiB);
    EXPECT_EQ(storage.used, 0);
}
TEST(OmniBroadcastAllocationListenerTest, ArithmeticBoundariesLeaveBothLedgersIntact)
{
    StorageLedger storage;
    storage.cap = std::numeric_limits<int64_t>::max();
    gluten::OmniBroadcastAllocationListener listener(storage);
    EXPECT_THROW(listener.allocationChanged(-1), std::underflow_error);
    EXPECT_THROW(listener.allocationChanged(std::numeric_limits<int64_t>::min()),
        std::underflow_error);
    EXPECT_EQ(storage.negativeCalls, 0);
    listener.allocationChanged(storage.cap);
    EXPECT_EQ(storage.used, storage.cap);
    EXPECT_THROW(listener.allocationChanged(1), std::overflow_error);
    EXPECT_EQ(storage.positiveCalls, 1);
    EXPECT_EQ(listener.currentBytes(), storage.cap);
    listener.allocationChanged(-storage.cap);
    EXPECT_EQ(listener.currentBytes(), 0);
    EXPECT_EQ(storage.used, 0);
}
} // namespace
