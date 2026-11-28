/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
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

package org.apache.gluten.memory.listener;

import org.apache.gluten.memory.SimpleMemoryUsageRecorder;
import org.apache.gluten.memory.memtarget.MemoryTarget;
import org.apache.gluten.memory.memtarget.MemoryTargetVisitor;
import org.apache.gluten.memory.memtarget.ThrowOnOomMemoryTarget;
import org.junit.Assert;
import org.junit.Test;

/**
 * Checks that an advisory denial leaves the listener's and the target's balances unchanged.
 *
 * @since 2026
 */
public class AdvisoryListenerTest {
    @Test
    public void denialLeavesSharedAndTargetBalancesUnchanged() {
        MemoryTarget target = new MemoryTarget() {
            private long used;

            @Override
            public long borrow(long size) {
                long granted = Math.min(size, 4);
                used += granted;
                return granted;
            }

            @Override
            public long repay(long size) {
                used -= size;
                return size;
            }

            @Override
            public long usedBytes() {
                return used;
            }

            @Override
            public <T> T accept(MemoryTargetVisitor<T> visitor) {
                throw new AssertionError("Unexpected statistics dump");
            }
        };
        SimpleMemoryUsageRecorder usage = new SimpleMemoryUsageRecorder();
        ReservationListener listener = new ManagedReservationListener(
                new ThrowOnOomMemoryTarget(target, true), usage, new Object());
        Assert.assertEquals(4, listener.reserve(4));
        for (int i = 0; i < 100; i++) {
            try {
                listener.reserve(8);
                Assert.fail("Expected denial");
            } catch (ThrowOnOomMemoryTarget.OutOfMemoryException e) {
                Assert.assertTrue(e.isAdvisory());
                Assert.assertEquals(0, e.getStackTrace().length);
            }
            Assert.assertEquals(4, listener.getUsedBytes());
            Assert.assertEquals(4, usage.current());
        }
        Assert.assertEquals(4, listener.unreserve(4));
        Assert.assertEquals(0, listener.getUsedBytes());
        Assert.assertEquals(0, usage.current());
    }
}
