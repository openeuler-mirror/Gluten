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
package com.huawei.boostkit.spark.jni;

import com.huawei.boostkit.spark.vectorized.SplitResult;
import org.apache.gluten.vectorized.NativePartitioning;
import org.apache.gluten.vectorized.JniByteInputStream;

public class SparkJniWrapper {
  public SparkJniWrapper() {
  }

  public long make(
      NativePartitioning part,
      int bufferSize,
      String codec,
      String dataFile,
      int subDirsPerLocalDir,
      String localDirs,
      long shuffleCompressBlockSize,
      int shuffleSpillBatchRowNum,
      long shuffleTaskSpillMemoryThreshold,
      long shuffleExecutorSpillMemoryThreshold) {
    return nativeMake(
        part.getShortName(),
        part.getNumPartitions(),
        new String(part.getRequiredFields()),
        part.getSchema().length,
        bufferSize,
        codec,
        dataFile,
        subDirsPerLocalDir,
        localDirs,
        shuffleCompressBlockSize,
        shuffleSpillBatchRowNum,
        shuffleTaskSpillMemoryThreshold,
        shuffleExecutorSpillMemoryThreshold);
  }

  public native long nativeMake(
      String shortName,
      int numPartitions,
      String inputTypes,
      int numCols,
      int bufferSize,
      String codec,
      String dataFile,
      int subDirsPerLocalDir,
      String localDirs,
      long shuffleCompressBlockSize,
      int shuffleSpillBatchRowNum,
      long shuffleTaskSpillMemoryThreshold,
      long shuffleExecutorSpillMemoryThreshold);

  /**
   * Split one record batch represented by bufAddrs and bufSizes into several batches. The batch is
   * split according to the first column as partition id. During splitting, the data in native
   * buffers will be write to disk when the buffers are full.
   *
   * @param nativeVectorBatch Addresses of nativeVectorBatch
   */
  public native void split(long splitterId, long nativeVectorBatch);

  /**
   * Split one record batch represented by bufAddrs and bufSizes into several batches. The batch is
   * converted to row formats for split according to the first column as partition id. During
   * splitting, the data in native buffers will be written to disk when the buffers are full.
   *
   * @param splitterId Addresses of splitter
   * @param nativeVectorBatch Addresses of nativeVectorBatch
   */
  public native void rowSplit(long splitterId, long nativeVectorBatch);

  /**
   * Write the data remained in the buffers hold by native splitter to each partition's temporary
   * file. And stop processing splitting
   *
   * @param splitterId splitter instance id
   * @return SplitResult
   */
  public native SplitResult stop(long splitterId);

  /**
   * Write the data remained in the row buffers hold by native splitter to each partition's
   * temporary file. And stop processing splitting
   *
   * @param splitterId splitter instance id
   * @return SplitResult
   */
  public native SplitResult rowStop(long splitterId);

  /**
   * Release resources associated with designated splitter instance.
   *
   * @param splitterId splitter instance id
   */
  public native void close(long splitterId);

    /**
     * Split one MixedVectorBatch into several batches.
     *
     * @param splitterId Addresses of splitter
     * @param nativeVectorBatch Addresses of MixedVectorBatch
     */
    public native void mixedSplit(long splitterId, long nativeVectorBatch);

    /**
     * Write the data remained in the MixedVectorBatch buffers and stop processing
     *
     * @param splitterId splitter instance id
     * @param isMixed whether the batch is mixed
     * @return SplitResult
     */
    public native SplitResult mixedStop(long splitterId, boolean isMixed);

    /**
     * make shuffle deserializer hold by native
     *
     * @param inputStream              JniByteInputStream
     * @param compressCodec            compress Codec
     * @param shuffleCompressBlockSize configured compress block size
     * @param isRowShuffle             if support row shuffle
     * @param isMixedEnabled           if enable mixed storage
     * @return Deserializer result
     */
    public native long makeNativeDeserializer(JniByteInputStream inputStream, String compressCodec,
                                              int shuffleCompressBlockSize, boolean isRowShuffle,
                                              boolean isMixedEnabled);

    /**
     * Release resources
     *
     * @param shuffleReaderHandle handler id
     */
    public native void closeDeserializer(long shuffleReaderHandle);
}
