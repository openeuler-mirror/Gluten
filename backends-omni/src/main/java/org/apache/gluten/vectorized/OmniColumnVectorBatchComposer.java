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

package org.apache.gluten.vectorized;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Horizontally compose Omni batches with copied passthrough columns.
 *
 * @since 2026-2026
 */
public final class OmniColumnVectorBatchComposer {
    private OmniColumnVectorBatchComposer() {}

    /**
     * Compose a batch in final output order.
     *
     * <p>For each output position, {@code inputIndexes[i] >= 0} means take that column from
     * {@code input}; otherwise {@code extraIndexes[i]} selects a materialized JVM column from
     * {@code extraColumns}.
     *
     * @param input native project result batch
     * @param extraColumns materialized fallback columns
     * @param inputIndexes native input column index for each output column
     * @param extraIndexes fallback column index for each output column
     * @return composed final output batch
     * @throws IllegalArgumentException if index mapping is invalid
     */
    public static ColumnarBatch compose(
            ColumnarBatch input,
            OmniColumnVector[] extraColumns,
            int[] inputIndexes,
            int[] extraIndexes) {
        int numRows = input.numRows();
        if (inputIndexes.length != extraIndexes.length) {
            throw new IllegalArgumentException("Input and extra index arrays must have the same length");
        }

        ColumnVector[] cols = new ColumnVector[inputIndexes.length];
        for (int i = 0; i < inputIndexes.length; i++) {
            int inputIndex = inputIndexes[i];
            if (inputIndex >= 0) {
                cols[i] = copyInputColumn(input, inputIndex, numRows);
            } else {
                int extraIndex = extraIndexes[i];
                if (extraIndex < 0 || extraIndex >= extraColumns.length) {
                    throw new IllegalArgumentException("Invalid extra column index: " + extraIndex);
                }
                cols[i] = extraColumns[extraIndex];
            }
        }
        return new ColumnarBatch(cols, numRows);
    }

    private static OmniColumnVector copyInputColumn(ColumnarBatch input, int inputIndex, int numRows) {
        ColumnVector src = input.column(inputIndex);
        DataType dt = src.dataType();
        OmniColumnVector out = new OmniColumnVector(numRows, dt, true);
        if (dt instanceof ArrayType) {
            copyArrayColumn(src, out, (ArrayType) dt, numRows);
        } else {
            copyColumn(src, out, dt, numRows);
        }
        return out;
    }

    private static void copyColumn(ColumnVector src, OmniColumnVector dst, DataType dt, int numRows) {
        for (int rowId = 0; rowId < numRows; rowId++) {
            if (src.isNullAt(rowId)) {
                dst.putNull(rowId);
            } else if (dt instanceof BooleanType) {
                dst.putBoolean(rowId, src.getBoolean(rowId));
            } else if (dt instanceof ByteType) {
                dst.putByte(rowId, src.getByte(rowId));
            } else if (dt instanceof ShortType) {
                dst.putShort(rowId, src.getShort(rowId));
            } else if (dt instanceof IntegerType || dt instanceof DateType) {
                dst.putInt(rowId, src.getInt(rowId));
            } else if (dt instanceof LongType || dt instanceof TimestampType) {
                dst.putLong(rowId, src.getLong(rowId));
            } else if (dt instanceof FloatType) {
                dst.putFloat(rowId, src.getFloat(rowId));
            } else if (dt instanceof DoubleType) {
                dst.putDouble(rowId, src.getDouble(rowId));
            } else if (dt instanceof StringType) {
                UTF8String value = src.getUTF8String(rowId);
                byte[] bytes = value.getBytes();
                dst.putBytes(rowId, bytes.length, bytes, 0);
            } else if (dt instanceof BinaryType) {
                byte[] bytes = src.getBinary(rowId);
                dst.putBytes(rowId, bytes.length, bytes, 0);
            } else if (dt instanceof DecimalType) {
                DecimalType decimalType = (DecimalType) dt;
                Decimal value = src.getDecimal(rowId, decimalType.precision(), decimalType.scale());
                dst.putDecimal(rowId, value, decimalType.precision());
            } else if (dt instanceof NullType) {
                dst.putNull(rowId);
            } else {
                throw new UnsupportedOperationException("Unsupported passthrough column type: " + dt);
            }
        }
    }

    private static void copyArrayColumn(
            ColumnVector src, OmniColumnVector dst, ArrayType arrayType, int numRows) {
        int[] offsets = new int[numRows + 1];
        byte[] nulls = new byte[numRows];
        int totalElements = 0;
        offsets[0] = 0;
        for (int rowId = 0; rowId < numRows; rowId++) {
            if (src.isNullAt(rowId)) {
                nulls[rowId] = 1;
            } else {
                totalElements += src.getArray(rowId).numElements();
            }
            offsets[rowId + 1] = totalElements;
        }

        DataType elementType = arrayType.elementType();
        OmniColumnVector elements = new OmniColumnVector(totalElements, elementType, true);
        int elementRowId = 0;
        for (int rowId = 0; rowId < numRows; rowId++) {
            if (!src.isNullAt(rowId)) {
                ColumnarArray array = src.getArray(rowId);
                for (int i = 0; i < array.numElements(); i++) {
                    copyArrayElement(array, i, elements, elementRowId, elementType);
                    elementRowId++;
                }
            }
        }

        dst.setChild(elements, 0);
        dst.setOffsets(offsets);
        dst.updateVec();
        dst.putNulls(0, nulls, numRows);
    }

    private static void copyArrayElement(
            ColumnarArray src, int srcIndex, OmniColumnVector dst, int dstIndex, DataType dt) {
        if (src.isNullAt(srcIndex)) {
            dst.putNull(dstIndex);
        } else if (dt instanceof BooleanType) {
            dst.putBoolean(dstIndex, src.getBoolean(srcIndex));
        } else if (dt instanceof ByteType) {
            dst.putByte(dstIndex, src.getByte(srcIndex));
        } else if (dt instanceof ShortType) {
            dst.putShort(dstIndex, src.getShort(srcIndex));
        } else if (dt instanceof IntegerType || dt instanceof DateType) {
            dst.putInt(dstIndex, src.getInt(srcIndex));
        } else if (dt instanceof LongType || dt instanceof TimestampType) {
            dst.putLong(dstIndex, src.getLong(srcIndex));
        } else if (dt instanceof FloatType) {
            dst.putFloat(dstIndex, src.getFloat(srcIndex));
        } else if (dt instanceof DoubleType) {
            dst.putDouble(dstIndex, src.getDouble(srcIndex));
        } else if (dt instanceof StringType) {
            UTF8String value = src.getUTF8String(srcIndex);
            byte[] bytes = value.getBytes();
            dst.putBytes(dstIndex, bytes.length, bytes, 0);
        } else if (dt instanceof BinaryType) {
            byte[] bytes = src.getBinary(srcIndex);
            dst.putBytes(dstIndex, bytes.length, bytes, 0);
        } else if (dt instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) dt;
            Decimal value = src.getDecimal(srcIndex, decimalType.precision(), decimalType.scale());
            dst.putDecimal(dstIndex, value, decimalType.precision());
        } else if (dt instanceof NullType) {
            dst.putNull(dstIndex);
        } else {
            throw new UnsupportedOperationException("Unsupported array element type: " + dt);
        }
    }
}
