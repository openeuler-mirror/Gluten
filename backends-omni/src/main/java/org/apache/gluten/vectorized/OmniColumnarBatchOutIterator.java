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

import nova.hetu.omniruntime.vector.MixedVec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.apache.gluten.iterator.ClosableIterator;
import org.apache.gluten.runtime.RuntimeAware;
import org.apache.gluten.substrait.type.TypeNode;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.types.DataTypes;

import java.io.IOException;
import java.util.List;

/**
 * Iterator that pulls columnar batches from the Omni native runtime and wraps them as Spark
 * {@link ColumnarBatch} instances for Gluten execution.
 *
 * @since 2026/04/14
 */
public class OmniColumnarBatchOutIterator extends ClosableIterator implements RuntimeAware {
    private final long iterHandle;

    private List<TypeNode> outputTypes;

    /**
     * Constructs an iterator bound to the given native iterator handle.
     *
     * @param iterHandle native iterator handle from the Omni backend
     */
    public OmniColumnarBatchOutIterator(long iterHandle) {
        super();
        this.iterHandle = iterHandle;
    }

    /**
     * Sets the logical output types used when materializing column vectors from native batches.
     *
     * @param outputTypes Substrait type nodes for each output column
     */
    public void setOutputTypes(List<TypeNode> outputTypes) {
        this.outputTypes = outputTypes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long rtHandle() {
        return -1;
    }

    /**
     * Returns the native iterator handle backing this iterator.
     *
     * @return native iterator handle
     */
    public long itrHandle() {
        return iterHandle;
    }

    private native boolean nativeHasNext(long iterHandle);

    private native long nativeNext(long iterHandle);

    private native long nativeSpill(long iterHandle, long size);

    private native void nativeClose(long iterHandle);

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext0() throws IOException {
        return nativeHasNext(iterHandle);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ColumnarBatch next0() throws IOException {
        long batchHandle = nativeNext(iterHandle);
        if (batchHandle == -1L) {
            return null; // stream ended
        }
        VecBatch vecBatch = transform(batchHandle);

        boolean isMixed = vecBatch.getMixType() != 0;
        List<TypeNode> typeNodes =
                isMixed ? outputTypes.subList(0, vecBatch.getVectorCount())
                        : outputTypes;
        OmniColumnVector[] omniColumnVectors = OmniColumnVector.allocateColumns(vecBatch.getRowCount(),
                typeNodes.toArray(new TypeNode[0]), false, isMixed, vecBatch.getVectorCount());
        for (int i = 0; i < typeNodes.size(); i++) {
            omniColumnVectors[i].setVec(vecBatch.getVector(i));
        }
        if (isMixed) {
            omniColumnVectors[omniColumnVectors.length - 1] = new OmniColumnVector(1, DataTypes.LongType, false);
            omniColumnVectors[omniColumnVectors.length - 1].setVec(new MixedVec(batchHandle));
        }

        return new ColumnarBatch(omniColumnVectors, vecBatch.getRowCount());
    }

    private VecBatch transform(long nativeBatch) {
        return nativeTransform(nativeBatch);
    }

    /**
     * Converts a native batch handle into a {@link VecBatch} owned by the Java side.
     *
     * @param nativeBatch native batch handle from Omni
     * @return materialized vector batch
     */
    public native VecBatch nativeTransform(long nativeBatch);

    /**
     * Requests the native iterator to spill up to the given number of bytes.
     *
     * @param size maximum bytes to spill
     * @return number of bytes spilled, or zero if this iterator is already closed
     */
    public long spill(long size) {
        if (!closed.get()) {
            return nativeSpill(iterHandle, size);
        } else {
            return 0L;
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Releases native resources while keeping already produced batches usable until their own
     * lifecycle ends.
     */
    @Override
    protected void close0() {
        nativeClose(iterHandle);
    }
}
