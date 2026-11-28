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
 *
 * ---
 *
 * OmniBlockAllocationListener ledger implementation.
 *
 * Block coalescing also supports an exact, non-block-aligned reservation after
 * JVM-side repayment.
 * The one-line fix — reservationBytes_ += bytesGranted — which the upstream
 * gluten::BlockAllocationListener omits, causing currentBytes() to return 0 forever.
 * Location of upstream bug: AllocationListener.h:74-88, field declared at line 95.
 */
#include "OmniBlockAllocationListener.h"

#include <limits>
#include <stdexcept>

namespace gluten {

namespace {
int64_t RoundUpToBlock(int64_t value, int64_t block) {
    if (value > std::numeric_limits<int64_t>::max() - (block - 1)) {
        throw std::overflow_error("OmniBlockAllocationListener: block reservation overflow");
    }
    if (block > 0) {
        return ((value + block - 1) / block) * block;
    }
    throw std::invalid_argument("OmniBlockAllocationListener: block size must be positive");
}
} // namespace

void OmniBlockAllocationListener::setBlockSize(int64_t blockSize) {
    if (blockSize <= 0) {
        throw std::invalid_argument("OmniBlockAllocationListener: block size must be positive");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (reservationBytes_ != 0 || pendingReservationBytes_ != 0) {
        throw std::logic_error("OmniBlockAllocationListener: block size set after reserving");
    }
    blockSize_ = blockSize;
}

OmniBlockAllocationListener::ReservationUpdate
OmniBlockAllocationListener::updateReservation(int64_t diff) {
    std::lock_guard<std::mutex> lock(mutex_);
    const int64_t committedUsed = usedBytes_ - pendingUsedBytes_;
    const int64_t committedReservation = reservationBytes_ - pendingReservationBytes_;
    if (diff > 0 && (usedBytes_ > std::numeric_limits<int64_t>::max() - diff ||
                     committedUsed > std::numeric_limits<int64_t>::max() - diff)) {
        throw std::overflow_error("OmniBlockAllocationListener: usedBytes_ overflow");
    }
    if (diff < 0 && (diff == std::numeric_limits<int64_t>::min() ||
                     usedBytes_ < -diff || committedUsed < -diff)) {
        throw std::underflow_error("OmniBlockAllocationListener: usedBytes_ underflow");
    }
    const int64_t newCommittedUsed = committedUsed + diff;
    int64_t target = newCommittedUsed == 0 ? 0 : RoundUpToBlock(newCommittedUsed, blockSize_);
    if (target < committedReservation && newCommittedUsed > 0 &&
        (diff > 0 || committedReservation - target < blockSize_)) {
        // Growth never repays retained slack; a free repays only once a whole
        // unused block has accumulated, and then down to the block-rounded usage.
        target = committedReservation;
    }
    if (diff < 0 && target > committedReservation) {
        // A free never borrows. After an exact external repayment the
        // reservation can sit below the block-rounded usage; topping it up here
        // would let Spark deny a deallocation, which often runs in a destructor.
        target = committedReservation;
    }
    const int64_t granted = target - committedReservation;
    if (granted > 0 && reservationBytes_ > std::numeric_limits<int64_t>::max() - granted) {
        throw std::overflow_error("OmniBlockAllocationListener: reservation overflow");
    }
    if (granted < 0 && reservationBytes_ < -granted) {
        throw std::underflow_error("OmniBlockAllocationListener: reservation underflow");
    }
    usedBytes_ += diff;
    reservationBytes_ += granted;
    if (granted != 0) {
        pendingUsedBytes_ += diff;
        pendingReservationBytes_ += granted;
    } else {
        peakBytes_ = std::max(peakBytes_, newCommittedUsed);
    }
    return {diff, granted, newCommittedUsed};
}

void OmniBlockAllocationListener::finishReservation(
    const ReservationUpdate& update, bool succeeded) {
    std::lock_guard<std::mutex> lock(mutex_);
    pendingUsedBytes_ -= update.usedDelta;
    pendingReservationBytes_ -= update.granted;
    if (!succeeded) {
        // Undo this call only. Reentrant repayments and successful nested calls
        // have already changed both ledgers and must survive the denial.
        usedBytes_ -= update.usedDelta;
        reservationBytes_ -= update.granted;
    } else {
        // A successful request's high-water mark remains valid even if a
        // nested free lowered current usage before the delegate returned.
        peakBytes_ = std::max(peakBytes_, update.peakCandidate);
    }
    peakBytes_ = std::max(peakBytes_, usedBytes_ - pendingUsedBytes_);
}

void OmniBlockAllocationListener::reconcileExternalRepay(int64_t bytes) {
    reconcileExternalRepay(bytes, [] {});
}

void OmniBlockAllocationListener::reconcileExternalRepay(
    int64_t bytes, const std::function<void()>& reconcile) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (bytes <= 0) {
        throw std::invalid_argument("OmniBlockAllocationListener: external repay must be positive");
    }
    if (bytes > usedBytes_ || bytes > reservationBytes_ ||
        bytes > usedBytes_ - pendingUsedBytes_ ||
        bytes > reservationBytes_ - pendingReservationBytes_) {
        throw std::underflow_error("OmniBlockAllocationListener: external repay exceeds ledger");
    }
    reconcile();
    usedBytes_ -= bytes;
    reservationBytes_ -= bytes;
}

} // namespace gluten
