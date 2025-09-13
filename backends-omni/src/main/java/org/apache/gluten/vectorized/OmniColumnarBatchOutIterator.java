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

import nova.hetu.omniruntime.vector.VecBatch;

import org.apache.gluten.iterator.ClosableIterator;
import org.apache.gluten.runtime.RuntimeAware;
import org.apache.gluten.substrait.type.TypeNode;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.List;

public class OmniColumnarBatchOutIterator extends ClosableIterator implements RuntimeAware {
  private final long iterHandle;

  private List<TypeNode> outputTypes;

  public OmniColumnarBatchOutIterator(long iterHandle) {
    super();
    this.iterHandle = iterHandle;
  }

  public void setOutputTypes(List<TypeNode> outputTypes) {
    this.outputTypes = outputTypes;
  }

  @Override
  public long rtHandle() {
    return -1;
  }

  public long itrHandle() {
    return iterHandle;
  }

  private native boolean nativeHasNext(long iterHandle);

  private native long nativeNext(long iterHandle);

  private native long nativeSpill(long iterHandle, long size);

  private native void nativeClose(long iterHandle);

  @Override
  public boolean hasNext0() throws IOException {
    return nativeHasNext(iterHandle);
  }

  @Override
  public ColumnarBatch next0() throws IOException {
    long batchHandle = nativeNext(iterHandle);
    if (batchHandle == -1L) {
      return null; // stream ended
    }
    VecBatch vecBatch = transform(batchHandle);
    OmniColumnVector[] omniColumnVectors = OmniColumnVector.allocateColumns(vecBatch.getRowCount(),
        outputTypes.toArray(new TypeNode[0]), false);
    for (int i = 0; i < omniColumnVectors.length; i++) {
      omniColumnVectors[i].setVec(vecBatch.getVector(i));
    }
    return new ColumnarBatch(omniColumnVectors, vecBatch.getRowCount());
  }


  private VecBatch transform(long nativeBatch) {
    return nativeTransform(nativeBatch);
  }

  public native VecBatch nativeTransform(long nativeBatch);


  public long spill(long size) {
    if (!closed.get()) {
      return nativeSpill(iterHandle, size);
    } else {
      return 0L;
    }
  }

  @Override
  public void close0() {
    // To make sure the outputted batches are still accessible after the iterator is closed.
    // TODO: Remove this API if we have other choice, e.g., hold the pools in native code.
    nativeClose(iterHandle);
  }
}
