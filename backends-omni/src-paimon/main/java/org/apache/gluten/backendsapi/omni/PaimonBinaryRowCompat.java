/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.backendsapi.omni;

import org.apache.paimon.data.BinaryRowWriter;

import java.lang.reflect.Method;

/**
 * Adapts {@code BinaryRowWriter.writeBinary} across Paimon 1.2 (two-arg) and later (four-arg)
 * overloads when compile-time and runtime jars disagree.
 *
 * @since 2026
 */
public final class PaimonBinaryRowCompat {
    private static final Method WRITE_BINARY_2;
    private static final Method WRITE_BINARY_4;

    static {
        Method twoArg = null;
        Method fourArg = null;
        try {
            twoArg = BinaryRowWriter.class.getMethod("writeBinary", int.class, byte[].class);
        } catch (NoSuchMethodException ignored) {
            // Paimon 1.3+
        }
        try {
            fourArg = BinaryRowWriter.class.getMethod(
                    "writeBinary", int.class, byte[].class, int.class, int.class);
        } catch (NoSuchMethodException ignored) {
            // Paimon 1.2
        }
        WRITE_BINARY_2 = twoArg;
        WRITE_BINARY_4 = fourArg;
    }

    private PaimonBinaryRowCompat() {}

    /**
     * Writes a binary partition or bucket field using the available Paimon overload.
     *
     * @param writer Paimon binary row writer
     * @param pos field index
     * @param bytes binary value
     */
    public static void writeBinary(BinaryRowWriter writer, int pos, byte[] bytes) {
        try {
            if (WRITE_BINARY_2 != null) {
                WRITE_BINARY_2.invoke(writer, pos, bytes);
            } else if (WRITE_BINARY_4 != null) {
                WRITE_BINARY_4.invoke(writer, pos, bytes, 0, bytes.length);
            } else {
                throw new IllegalStateException(
                        "BinaryRowWriter.writeBinary is missing on " + BinaryRowWriter.class.getName());
            }
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Paimon BinaryRowWriter.writeBinary failed", e);
        }
    }
}
