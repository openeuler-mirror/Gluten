/*
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

#include "gtest/gtest.h"
#include "compute/OmniBackend.h"
#include "memory/AllocationListener.h"

#include <atomic>
#include <chrono>
#include <future>
#include <memory>

namespace {
struct SnapshotState {
    std::promise<void> entered;
    std::promise<void> resume;
    std::shared_future<void> resumeFuture = resume.get_future().share();
    std::atomic<bool> first{true};
    std::atomic<bool> destroyed{false};
};

class BlockingSnapshotListener final : public gluten::AllocationListener {
public:
    explicit BlockingSnapshotListener(std::shared_ptr<SnapshotState> state) : state_(std::move(state)) {}
    ~BlockingSnapshotListener() override { state_->destroyed.store(true); }
    void allocationChanged(int64_t) override {}
    int64_t currentBytes() override
    {
        if (state_->first.exchange(false)) {
            state_->entered.set_value();
            state_->resumeFuture.wait();
        }
        return 0;
    }
private:
    std::shared_ptr<SnapshotState> state_;
};

TEST(OmniBroadcastLifecycleTest, SnapshotOwnsListenerAcrossConcurrentShutdown)
{
    auto state = std::make_shared<SnapshotState>();
    auto entered = state->entered.get_future();
    gluten::OmniBackend::create({});
    gluten::OmniBackend::configureBroadcastCache(std::make_unique<BlockingSnapshotListener>(state));
    auto snapshot = std::async(std::launch::async, [] {
        return gluten::OmniBackend::broadcastCacheOutstandingBytes();
    });
    const auto status = entered.wait_for(std::chrono::seconds(5));
    if (status != std::future_status::ready) {
        state->resume.set_value();
        snapshot.wait();
        gluten::OmniBackend::shutdownBroadcastCache();
        FAIL() << "snapshot did not enter listener";
    }
    gluten::OmniBackend::shutdownBroadcastCache();
    EXPECT_FALSE(state->destroyed.load());
    EXPECT_EQ(gluten::OmniBackend::broadcastCacheOutstandingBytes(), 0);
    state->resume.set_value();
    EXPECT_EQ(snapshot.get(), 0);
    EXPECT_TRUE(state->destroyed.load());
}
} // namespace
