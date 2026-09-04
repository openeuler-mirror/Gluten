/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

package org.apache.gluten.vectorized;

import nova.hetu.omniruntime.vector.VecBatch;

/**
 * JNI bindings for the Omni native row-to-columnar converter.
 *
 * @since 2026
 */
public class OmniRowToColumnarJniWrapper {
    /**
     * Initializes a native row-to-columnar converter.
     *
     * @param schemaJson JSON representation of the input Spark schema
     * @return native converter handle
     */
    public native long init(String schemaJson);

    /**
     * Converts rows in the supplied off-heap buffer into an Omni vector batch.
     *
     * @param r2cHandle native converter handle
     * @param rowLength length of each input row
     * @param memoryAddress address of the input row buffer
     * @return converted Omni vector batch
     */
    public native VecBatch nativeConvertRowToColumnar(
            long r2cHandle, long[] rowLength, long memoryAddress);

    /**
     * Releases a native row-to-columnar converter.
     *
     * @param r2cHandle native converter handle
     */
    public native void close(long r2cHandle);

    /**
     * Allocates an off-heap staging buffer from the Omni native allocator. The returned address is
     * accounted in Omni's ThreadMemoryManager; callers must release it with {@link #freeRowBuffer}.
     *
     * @param size requested buffer size in bytes
     * @return address of the allocated buffer
     */
    public native long allocateRowBuffer(long size);

    /**
     * Releases an off-heap row staging buffer.
     *
     * @param address address returned by {@link #allocateRowBuffer(long)}
     * @param size buffer size in bytes
     */
    public native void freeRowBuffer(long address, long size);
}
