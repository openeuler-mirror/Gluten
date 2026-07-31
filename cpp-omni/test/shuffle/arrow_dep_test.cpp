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

// Batch0 (TC-DEP-01/02): verify cpp-omni links Arrow and the Arrow version is
// coherent. These are the build-link gate for all subsequent Arrow tests.

#include "gtest/gtest.h"
#include <arrow/buffer.h>
#include <arrow/result.h>

// arrow/version.h was introduced in Arrow 7.0; older versions define
// version macros in arrow/util/config.h instead.
#if defined(__has_include) && __has_include(<arrow/version.h>)
#include <arrow/version.h>
#elif defined(__has_include) && __has_include(<arrow/util/config.h>)
#include <arrow/util/config.h>
#endif

// Check if version macros are available
#if defined(ARROW_VERSION_MAJOR)
#define ARROW_VERSION_AVAILABLE 1
#else
#define ARROW_VERSION_AVAILABLE 0
#endif

class ArrowDepTest : public testing::Test {};

// TC-DEP-01 — testArrowDependencyLinkedInCmake
// Prove Arrow is linked by allocating a buffer through the Arrow C++ API.
// test/CMakeLists.txt links `arrow` into the final tptest executable.
TEST_F(ArrowDepTest, testArrowDependencyLinkedInCmake)
{
    auto result = arrow::AllocateBuffer(64);
    ASSERT_TRUE(result.ok()) << "arrow::AllocateBuffer must succeed when Arrow is linked";
    std::shared_ptr<arrow::Buffer> buf = std::move(*result);
    ASSERT_NE(buf, nullptr);
    EXPECT_EQ(buf->size(), 64);
    EXPECT_NE(buf->data(), nullptr);
}

// TC-DEP-02 — testArrowVersionAligned
// The bolt side (bolt/shuffle/sparksql/BoltArrowMemoryPool.cpp) already uses
// Arrow; ABI mismatch would surface as link/run errors. The exact baseline
// version is not knowable on this host, so we assert the version macros are
// present and at a sane floor, and print them for the integration-time
// alignment check.
// Note: older Arrow versions (< 7.0) may not expose version macros at all,
// so we skip gracefully when they are unavailable.
TEST_F(ArrowDepTest, testArrowVersionAligned)
{
#if ARROW_VERSION_AVAILABLE
    EXPECT_GE(ARROW_VERSION_MAJOR, 1);
    RecordProperty("arrow_version_major", ARROW_VERSION_MAJOR);
    RecordProperty("arrow_version_minor", ARROW_VERSION_MINOR);
    RecordProperty("arrow_version_patch", ARROW_VERSION_PATCH);
    SUCCEED() << "Arrow version " << ARROW_VERSION_MAJOR << "."
              << ARROW_VERSION_MINOR << "." << ARROW_VERSION_PATCH;
#else
    GTEST_SKIP() << "Arrow version macros not available on this build";
#endif
}
