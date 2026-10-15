/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * OmniBlockAllocationListener unit tests:
 *   - Block reservation: whole 8 MiB blocks, like Gluten's Velox arbitrator
 *   - CurrentBytesReportsCoalesced: the bug fix
 *   - PeakBytesReportsHighWatermark: high-watermark tracking
 *
 * Upstream bug reference: gluten::BlockAllocationListener at
 * AllocationListener.h:74-88 never writes reservationBytes_, so its
 * currentBytes() silently returns 0 forever. OmniBlockAllocationListener
 * fixes this with reservationBytes_ += bytesGranted inside reserve().
 */
#include "gtest/gtest.h"
#include "jni/OmniBlockAllocationListener.h"
#include "memory/AllocationListener.h"
#include <stdexcept>
#include <functional>

namespace {

class CountingListener final : public gluten::AllocationListener {
 public:
    void allocationChanged(int64_t diff) override { total += diff; ++calls; }
    int64_t currentBytes() override { return total; }
    int64_t peakBytes() override { return total; }
    int64_t total = 0;
    int calls = 0;
};

} // namespace

class ThrowingListener final : public gluten::AllocationListener {
 public:
    void allocationChanged(int64_t) override { throw std::runtime_error("denied"); }
    int64_t currentBytes() override { return 0; }
    int64_t peakBytes() override { return 0; }
};

constexpr int64_t MiB(int64_t value) { return value << 20; }

TEST(OmniBlockAllocationListener, ReservesWholeBlocksLikeVeloxArbitrator) {
    CountingListener delegate;
    gluten::OmniBlockAllocationListener wrap(&delegate);

    wrap.allocationChanged(1);              // first byte -> one 8 MiB block
    EXPECT_EQ(delegate.total, MiB(8));
    wrap.allocationChanged(MiB(8) - 1);     // exactly 8 MiB still fits
    EXPECT_EQ(delegate.total, MiB(8));
    EXPECT_EQ(delegate.calls, 1);
    wrap.allocationChanged(1);              // 8 MiB + 1 -> second block
    EXPECT_EQ(delegate.total, MiB(16));
    EXPECT_EQ(delegate.calls, 2);
}

TEST(OmniBlockAllocationListener, BlockSizeIsConfigurableBeforeFirstReservation) {
    CountingListener delegate;
    gluten::OmniBlockAllocationListener wrap(&delegate);
    EXPECT_THROW(wrap.setBlockSize(0), std::invalid_argument);
    wrap.setBlockSize(MiB(2));
    wrap.allocationChanged(MiB(3));
    EXPECT_EQ(delegate.total, MiB(4));
    EXPECT_THROW(wrap.setBlockSize(MiB(8)), std::logic_error);
}

TEST(OmniBlockAllocationListener, CurrentBytesReportsCoalesced_ReqList04) {
    CountingListener delegate;
    gluten::OmniBlockAllocationListener wrap(&delegate);

    wrap.allocationChanged(MiB(16));
    EXPECT_EQ(delegate.calls, 1);
    EXPECT_EQ(delegate.total, MiB(16));

    // THE bug fix: upstream returns 0 here. Our wrap returns the actual coalesced reservation.
    EXPECT_EQ(wrap.currentBytes(), MiB(16));
}

TEST(OmniBlockAllocationListener, PeakBytesReportsHighWatermark) {
    CountingListener delegate;
    gluten::OmniBlockAllocationListener wrap(&delegate);

    wrap.allocationChanged(10 << 20);   // peak now 10 MiB usedBytes_
    wrap.allocationChanged(-5 << 20);   // back down to 5 MiB usedBytes_
    // peakBytes_ should still be >= 10 MiB (high-watermark usedBytes_ — not the current).
    EXPECT_GE(wrap.peakBytes(), static_cast<int64_t>(10) << 20);
}

TEST(OmniBlockAllocationListener, ExactExternalRepaySupportsLaterBlockCrossingsAndZero) {
    CountingListener delegate;
    gluten::OmniBlockAllocationListener wrap(&delegate);
    wrap.setBlockSize(MiB(1));
    wrap.allocationChanged(MiB(2)); // reservation 2 MiB
    // `self.repay(3)` has already credited Spark/the delegate before the
    // ledger-only native reconciliation callback arrives.
    delegate.total -= MiB(1) / 2;
    const int callsBeforeReconcile = delegate.calls;
    wrap.reconcileExternalRepay(MiB(1) / 2); // exact non-aligned reservation
    EXPECT_EQ(delegate.total, MiB(3) / 2);
    EXPECT_EQ(delegate.calls, callsBeforeReconcile);
    EXPECT_EQ(wrap.currentBytes(), MiB(3) / 2);

    wrap.allocationChanged(MiB(1)); // used 2.5 MiB -> reservation 3 MiB
    EXPECT_EQ(delegate.total, MiB(3));
    EXPECT_EQ(wrap.currentBytes(), MiB(3));
    wrap.allocationChanged(-MiB(1)); // used 1.5 MiB -> one unused block repaid
    EXPECT_EQ(delegate.total, MiB(2));
    EXPECT_EQ(wrap.currentBytes(), MiB(2));
    wrap.allocationChanged(-MiB(3) / 2); // free-to-zero returns exact remainder
    EXPECT_EQ(delegate.total, 0);
    EXPECT_EQ(wrap.currentBytes(), 0);
}

TEST(OmniBlockAllocationListener, RejectsOverReconcileWithoutChangingLedger) {
    CountingListener delegate;
    gluten::OmniBlockAllocationListener wrap(&delegate);
    wrap.setBlockSize(MiB(1));
    wrap.allocationChanged(MiB(1));
    EXPECT_THROW(wrap.reconcileExternalRepay(MiB(1) + 1), std::underflow_error);
    EXPECT_EQ(wrap.currentBytes(), MiB(1));
    wrap.allocationChanged(-MiB(1));
    EXPECT_EQ(delegate.total, 0);
}

TEST(OmniBlockAllocationListener, DelegateThrowRollsBackAllLedgerState) {
    ThrowingListener delegate;
    gluten::OmniBlockAllocationListener wrap(&delegate);
    EXPECT_THROW(wrap.allocationChanged(1), std::runtime_error);
    EXPECT_EQ(wrap.currentBytes(), 0);
    EXPECT_EQ(wrap.peakBytes(), 0);
}

namespace {
class ReentrantCountingListener final : public gluten::AllocationListener {
 public:
    int64_t total = 0;
    std::function<void()> onBorrow;
    void allocationChanged(int64_t diff) override {
        if (diff > 0 && onBorrow) {
            auto action = std::move(onBorrow);
            onBorrow = nullptr;
            action();
        }
        if (total + diff < 0) throw std::underflow_error("over-repayment");
        total += diff;
    }
};
} // namespace

TEST(OmniBlockAllocationListener, ReentrantExternalRepaySurvivesDeniedBorrow) {
    for (int64_t repaid : {MiB(8), MiB(4), MiB(1) / 2}) {
        ReentrantCountingListener delegate;
        gluten::OmniBlockAllocationListener wrap(&delegate);
        wrap.allocationChanged(MiB(8));
        delegate.onBorrow = [&] {
            delegate.total -= repaid;
            wrap.reconcileExternalRepay(repaid);
            throw std::runtime_error("denied");
        };
        EXPECT_THROW(wrap.allocationChanged(MiB(8)), std::runtime_error);
        EXPECT_EQ(wrap.currentBytes(), MiB(8) - repaid);
        EXPECT_EQ(wrap.currentBytes(), delegate.total);
        wrap.allocationChanged(-(MiB(8) - repaid));
        EXPECT_EQ(wrap.currentBytes(), 0);
        EXPECT_EQ(delegate.total, 0);
        EXPECT_EQ(wrap.peakBytes(), MiB(8));
    }
}

TEST(OmniBlockAllocationListener, NestedFreeCannotRepayProvisionalBlockRounding) {
    ReentrantCountingListener delegate;
    gluten::OmniBlockAllocationListener wrap(&delegate);
    wrap.setBlockSize(MiB(1));
    wrap.allocationChanged(MiB(15));
    delegate.onBorrow = [&] {
        wrap.allocationChanged(-MiB(3));
        throw std::runtime_error("denied");
    };
    EXPECT_THROW(wrap.allocationChanged(MiB(2)), std::runtime_error);
    EXPECT_EQ(wrap.currentBytes(), MiB(12));
    EXPECT_EQ(delegate.total, MiB(12));
    wrap.allocationChanged(-MiB(12));
    EXPECT_EQ(delegate.total, 0);
}

TEST(OmniBlockAllocationListener, SuccessfulNestedBorrowAndPeakSurviveOuterDenial) {
    ReentrantCountingListener delegate;
    gluten::OmniBlockAllocationListener wrap(&delegate);
    wrap.allocationChanged(MiB(8));
    delegate.onBorrow = [&] {
        wrap.allocationChanged(MiB(16));
        throw std::runtime_error("outer denied");
    };
    EXPECT_THROW(wrap.allocationChanged(MiB(8)), std::runtime_error);
    EXPECT_EQ(wrap.currentBytes(), MiB(24));
    EXPECT_EQ(delegate.total, MiB(24));
    EXPECT_EQ(wrap.peakBytes(), MiB(24));
    wrap.allocationChanged(-MiB(24));
    EXPECT_EQ(delegate.total, 0);
}

TEST(OmniBlockAllocationListener, SuccessfulBorrowRetainsPeakAcrossNestedFree) {
    ReentrantCountingListener delegate;
    gluten::OmniBlockAllocationListener wrap(&delegate);
    wrap.allocationChanged(MiB(15));
    delegate.onBorrow = [&] { wrap.allocationChanged(-MiB(3)); };
    wrap.allocationChanged(MiB(2));
    // The successful request crossed 17 MiB before the nested free. Its peak
    // remains valid even though only 14 MiB is used when the callback returns.
    EXPECT_EQ(wrap.peakBytes(), MiB(17));
    EXPECT_EQ(wrap.currentBytes(), delegate.total);
    wrap.allocationChanged(-MiB(14));
    EXPECT_EQ(wrap.currentBytes(), 0);
    EXPECT_EQ(delegate.total, 0);
}

TEST(OmniBlockAllocationListenerTest, ExternalRepaymentCommitsOnlyAfterDownstreamSuccess)
{
    CountingListener storage;
    gluten::OmniBlockAllocationListener block(&storage);
    block.setBlockSize(MiB(1));
    block.allocationChanged(MiB(2));
    EXPECT_THROW(block.reconcileExternalRepay(MiB(1), [] {
        throw std::underflow_error("downstream ledger rejected repayment");
    }), std::underflow_error);
    EXPECT_EQ(block.currentBytes(), MiB(2));
    EXPECT_EQ(storage.total, MiB(2));
    bool called = false;
    EXPECT_THROW(block.reconcileExternalRepay(MiB(3), [&] { called = true; }),
        std::underflow_error);
    EXPECT_FALSE(called);
    block.reconcileExternalRepay(MiB(1), [&] { storage.total -= MiB(1); });
    EXPECT_EQ(block.currentBytes(), MiB(1));
    EXPECT_EQ(storage.total, MiB(1));
    block.allocationChanged(-MiB(1));
    EXPECT_EQ(block.currentBytes(), 0);
    EXPECT_EQ(storage.total, 0);
}

TEST(OmniBlockAllocationListener, FreesRetainOneBlockBeforeRepaying) {
    CountingListener delegate;
    gluten::OmniBlockAllocationListener wrap(&delegate);
    wrap.allocationChanged(MiB(12));         // two blocks
    ASSERT_EQ(delegate.calls, 1);
    ASSERT_EQ(delegate.total, MiB(16));

    // Usage wobbling inside the reservation never calls Spark.
    for (int i = 0; i < 10; ++i) {
        wrap.allocationChanged(-MiB(3) / 2);
        wrap.allocationChanged(MiB(3) / 2);
    }
    EXPECT_EQ(delegate.calls, 1);

    // Draining repays only once a whole block is unused, and free-to-zero
    // repays the rest.
    for (int i = 0; i < 3; ++i) {
        wrap.allocationChanged(-MiB(1)); // used 11, 10, 9 MiB
    }
    EXPECT_EQ(delegate.calls, 1);
    EXPECT_EQ(delegate.total, MiB(16));
    wrap.allocationChanged(-MiB(1));     // used 8 MiB: one block unused
    EXPECT_EQ(delegate.calls, 2);
    EXPECT_EQ(delegate.total, MiB(8));
    wrap.allocationChanged(-MiB(8));
    EXPECT_EQ(delegate.calls, 3);
    EXPECT_EQ(delegate.total, 0);
    EXPECT_EQ(wrap.currentBytes(), 0);
}

TEST(OmniBlockAllocationListener, GrowthReusesRetainedReservation) {
    CountingListener delegate;
    gluten::OmniBlockAllocationListener wrap(&delegate);
    wrap.allocationChanged(MiB(10));   // 16 MiB reserved
    wrap.allocationChanged(-MiB(1));   // 9 MiB used, nothing repaid
    wrap.allocationChanged(MiB(6));    // 15 MiB fits the reservation
    EXPECT_EQ(delegate.calls, 1);
    EXPECT_EQ(delegate.total, MiB(16));
    wrap.allocationChanged(MiB(2));    // 17 MiB needs a third block
    EXPECT_EQ(delegate.calls, 2);
    EXPECT_EQ(delegate.total, MiB(24));
}
