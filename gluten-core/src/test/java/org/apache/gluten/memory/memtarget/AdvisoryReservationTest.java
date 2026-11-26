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

package org.apache.gluten.memory.memtarget;

import org.junit.Assert;
import org.junit.Test;

/**
 * Checks that an advisory reservation denial rolls the grant back and carries no diagnostics.
 *
 * @since 2026
 */
public class AdvisoryReservationTest {
    private static class Target implements MemoryTarget {
        long balance;
        long limit = 4L;
        IllegalStateException repaymentFailure;

        @Override
        public long borrow(long size) {
            long granted = Math.min(size, limit);
            balance += granted;
            return granted;
        }

        @Override
        public long repay(long size) {
            if (repaymentFailure != null) {
                throw repaymentFailure;
            }
            balance -= size;
            return size;
        }

        @Override
        public long usedBytes() {
            return balance;
        }

        @Override
        public <T> T accept(MemoryTargetVisitor<T> visitor) {
            throw new AssertionError("Advisory denial must not dump target statistics");
        }
    }

    @Test
    public void partialAndZeroGrantsRollbackWithoutStackOrStats() {
        for (long limit : new long[] {0L, 4L}) {
            Target target = new Target();
            target.limit = limit;
            try {
                new ThrowOnOomMemoryTarget(target, true).borrow(8);
                Assert.fail("Expected reservation denial");
            } catch (ThrowOnOomMemoryTarget.OutOfMemoryException e) {
                Assert.assertTrue(e.isAdvisory());
                Assert.assertEquals(0, e.getStackTrace().length);
                Assert.assertEquals(0, target.balance);
            }
        }
    }

    @Test
    public void successfulGrantAndRepaymentPreserveBalance() {
        Target target = new Target();
        MemoryTarget wrapped = new ThrowOnOomMemoryTarget(target, true);
        Assert.assertEquals(4, wrapped.borrow(4));
        Assert.assertEquals(4, wrapped.usedBytes());
        Assert.assertEquals(4, wrapped.repay(4));
        Assert.assertEquals(0, wrapped.usedBytes());
    }

    @Test
    public void rollbackFailureIsNotClassifiedAsAdvisory() {
        Target target = new Target();
        target.repaymentFailure = new IllegalStateException("repayment failed");
        try {
            new ThrowOnOomMemoryTarget(target, true).borrow(8);
            Assert.fail("Expected repayment failure");
        } catch (IllegalStateException e) {
            Assert.assertSame(target.repaymentFailure, e);
        }
    }

    @Test
    public void ordinaryExceptionsKeepDiagnostics() {
        ThrowOnOomMemoryTarget.OutOfMemoryException error =
                new ThrowOnOomMemoryTarget.OutOfMemoryException("fatal");
        Assert.assertFalse(error.isAdvisory());
        Assert.assertTrue(error.getStackTrace().length > 0);
    }
}
