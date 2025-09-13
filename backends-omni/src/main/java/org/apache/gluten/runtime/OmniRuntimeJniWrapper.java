/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.apache.gluten.runtime;

/**
 * The Runtime for OmniOperator.
 *
 * @since 2025-05-26
 */
public class OmniRuntimeJniWrapper {
    private OmniRuntimeJniWrapper() {}

    public static native long createRuntime(String backendType, long nmm, byte[] sessionConf);

    public static native void releaseRuntime(long handle);
}
