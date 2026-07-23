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
package org.apache.gluten.vectorized;

import org.apache.gluten.memory.memtarget.MemoryTarget;
import org.apache.gluten.memory.memtarget.Spiller;
import org.apache.gluten.memory.memtarget.Spillers;
import org.apache.gluten.runtime.Runtime;
import org.apache.gluten.runtime.Runtimes;
import org.apache.gluten.utils.DebugUtil;
import org.apache.gluten.validate.NativePlanValidationInfo;
import org.apache.spark.TaskContext;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Evaluates native execution plans and creates JNI-backed kernel iterators.
 *
 * @since 1.3.0
 */
public class NativePlanEvaluator {
    private static final AtomicInteger ID_GENERATOR = new AtomicInteger(0);

    private final Runtime runtime;
    private final PlanEvaluatorJniWrapper jniWrapper;

    private NativePlanEvaluator(Runtime runtime) {
        this.runtime = runtime;
        this.jniWrapper = PlanEvaluatorJniWrapper.create(runtime);
    }

    /**
     * Creates a native plan evaluator for the given backend.
     *
     * @param backendName backend identifier
     * @return evaluator bound to the backend runtime
     */
    public static NativePlanEvaluator create(String backendName) {
        return new NativePlanEvaluator(
                Runtimes.contextInstance(
                        backendName,
                        String.format(
                                Locale.ROOT,
                                "NativePlanEvaluator-%d",
                                ID_GENERATOR.getAndIncrement())));
    }

    /**
     * Validates a native sub-plan and returns the validation result.
     *
     * @param subPlan serialized sub-plan
     * @return validation info with failure reason details
     */
    public NativePlanValidationInfo doNativeValidateWithFailureReason(byte[] subPlan) {
        return jniWrapper.nativeValidateWithFailureReason(subPlan);
    }

    /**
     * Injects the temporary path used by native write-files operators.
     *
     * @param path temporary path passed to the native layer
     */
    public static void injectWriteFilesTempPath(String path) {
        PlanEvaluatorJniWrapper.injectWriteFilesTempPath(path.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Creates the native computing pipeline and returns its columnar output iterator.
     *
     * @param wsPlan serialized whole-stage plan
     * @param splitInfo serialized split info
     * @param iterList input batch iterators
     * @param partitionIndex Spark partition index
     * @param spillDirPath spill directory path
     * @return native output iterator
     */
    public ColumnarBatchOutIterator createKernelWithBatchIterator(
            byte[] wsPlan,
            byte[][] splitInfo,
            List<ColumnarBatchInIterator> iterList,
            int partitionIndex,
            String spillDirPath) {
        long iteratorHandle =
                jniWrapper.nativeCreateKernelWithIterator(
                        wsPlan,
                        splitInfo,
                        iterList.toArray(new ColumnarBatchInIterator[0]),
                        TaskContext.get().stageId(),
                        partitionIndex,
                        TaskContext.get().taskAttemptId(),
                        DebugUtil.saveInputToFile(),
                        spillDirPath);
        ColumnarBatchOutIterator out = createOutIterator(runtime, iteratorHandle);
        runtime.memoryManager().addSpiller(createKernelSpiller(out));
        return out;
    }

    private Spiller createKernelSpiller(ColumnarBatchOutIterator out) {
        return new Spiller() {
            @Override
            public long spill(MemoryTarget self, Spiller.Phase phase, long size) {
                if (!Spillers.PHASE_SET_SPILL_ONLY.contains(phase)) {
                    return 0L;
                }
                return out.spill(size);
            }
        };
    }

    private ColumnarBatchOutIterator createOutIterator(Runtime runtime, long iteratorHandle) {
        return new ColumnarBatchOutIterator(runtime, iteratorHandle);
    }
}
