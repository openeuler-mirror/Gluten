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

import org.apache.gluten.substrait.type.StructNode;
import org.apache.gluten.substrait.type.TypeNode;
import org.apache.gluten.utils.DebugUtil;
import org.apache.gluten.validate.NativePlanValidationInfo;
import org.apache.spark.TaskContext;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class OmniNativePlanEvaluator {
  private final OmniPlanEvaluatorJniWrapper jniWrapper;

  private OmniNativePlanEvaluator() {
    this.jniWrapper = OmniPlanEvaluatorJniWrapper.create();
  }

  public static OmniNativePlanEvaluator create(String backendName) {
    return new OmniNativePlanEvaluator();
  }

  public NativePlanValidationInfo doNativeValidateWithFailureReason(byte[] subPlan) {
    return jniWrapper.nativeValidateWithFailureReason(subPlan);
  }

  public static void injectWriteFilesTempPath(String path) {
    PlanEvaluatorJniWrapper.injectWriteFilesTempPath(path.getBytes(StandardCharsets.UTF_8));
  }

  // Used by WholeStageTransform to create the native computing pipeline and
  // return a columnar result iterator.
  public OmniColumnarBatchOutIterator createKernelWithBatchIterator(
      byte[] wsPlan,
      byte[][] splitInfo,
      List<ColumnarBatchInIterator> iterList,
      int partitionIndex,
      String spillDirPath,
      TypeNode typeNode)
      throws RuntimeException {
    final long itrHandle =
        jniWrapper.nativeCreateKernelWithIterator(
            wsPlan,
            splitInfo,
            iterList.toArray(new OmniColumnarBatchInIterator[0]),
            TaskContext.get().stageId(),
            partitionIndex, // TaskContext.getPartitionId(),
            TaskContext.get().taskAttemptId(),
            DebugUtil.saveInputToFile(),
            spillDirPath);
    List<TypeNode> outputTypes = ((StructNode) typeNode).getFieldTypes();
    final OmniColumnarBatchOutIterator out = createOutIterator(itrHandle, outputTypes);
    // todo add spill
    // runtime
    //     .memoryManager()
    //     .addSpiller(
    //         new Spiller() {
    //           @Override
    //           public long spill(MemoryTarget self, Spiller.Phase phase, long size) {
    //             if (!Spillers.PHASE_SET_SPILL_ONLY.contains(phase)) {
    //               return 0L;
    //             }
    //             return out.spill(size);
    //           }
    //         });
    return out;
  }

  private OmniColumnarBatchOutIterator createOutIterator(long itrHandle,  List<TypeNode> outputTypes) {
    OmniColumnarBatchOutIterator columnarBatchOutIterator = new OmniColumnarBatchOutIterator(itrHandle);
    columnarBatchOutIterator.setOutputTypes(outputTypes);
    return columnarBatchOutIterator;
  }
}
