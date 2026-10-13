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
 */
package org.apache.gluten.vectorized;

import org.junit.Test;
import static org.junit.Assert.*;

public class OmniColumnVectorPoolResolverTest {
    @Test
    public void taskFreeResolutionDoesNotCreateManager() {
        assertEquals(0L, OmniColumnVector.resolveColumnVectorPoolAddress(false, () -> {
            throw new AssertionError("task-free fallback must not resolve a manager");
        }));
    }

    @Test
    public void taskPoolAddressIsPreserved() {
        assertEquals(123L, OmniColumnVector.resolveColumnVectorPoolAddress(true, () -> 123L));
    }

    @Test
    public void activeTaskResolutionFailureAndErrorPropagateUnchanged() {
        RuntimeException failure = new IllegalStateException("NMM creation failed");
        try {
            OmniColumnVector.resolveColumnVectorPoolAddress(true, () -> { throw failure; });
            fail("must propagate NMM failure");
        } catch (RuntimeException caught) {
            assertSame(failure, caught);
        }
        AssertionError error = new AssertionError("JNI linkage failure");
        try {
            OmniColumnVector.resolveColumnVectorPoolAddress(true, () -> { throw error; });
            fail("must propagate JVM Error");
        } catch (AssertionError caught) {
            assertSame(error, caught);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void activeTaskMustHaveNonzeroPool() {
        OmniColumnVector.resolveColumnVectorPoolAddress(true, () -> 0L);
    }
}
