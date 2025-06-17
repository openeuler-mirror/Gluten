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

import io.substrait.proto.Type;
import nova.hetu.omniruntime.vector.BooleanVec;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.ShortVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;
import org.apache.gluten.substrait.type.*;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class VectorTransferUtils {
    private static int MAX_LONG_DIGITS = 18;

    public static VecBatch nativeColumnarBatch2VecBatch(ColumnarBatch nativeColumnarBatch) {
        return nativeColumnarBatch2VecBatch(nativeColumnarBatch, false);
    }

    public static VecBatch nativeColumnarBatch2VecBatch(
            ColumnarBatch nativeColumnarBatch, boolean isSlice) {
        int vecNum = nativeColumnarBatch.numCols();
        Vec[] vecs = new Vec[vecNum];
        try {
            for (int i = 0; i < vecNum; i++) {
                if (!isSlice) {
                    vecs[i] = ((OmniColumnVector) nativeColumnarBatch.column(i)).getVec();
                } else {
                    vecs[i] =
                            ((OmniColumnVector) nativeColumnarBatch.column(i))
                                    .getVec()
                                    .slice(0, nativeColumnarBatch.numRows());
                }
            }
        } catch (Exception exception) {
            for (int i = 0; i < vecNum; i++) {
                if (isSlice && vecs[i] != null) {
                    vecs[i].close();
                }
                if (nativeColumnarBatch.column(i) instanceof OmniColumnVector) {
                    nativeColumnarBatch.column(i).close();
                }
            }
            throw exception;
        }
        return new VecBatch(vecs, nativeColumnarBatch.numRows());
    }

    public static ColumnarBatch vectorsToNativeColumnarBatch(
            Vec[] vecs, List<TypeNode> colTypes, int batchSize) {
        int count = colTypes.size();
        ColumnVector[] nColumnarVectors = new OmniColumnVector[count];
        for (int i = 0; i < count; i++) {
            nColumnarVectors[i] = vecToOmniNativeColumnVector(vecs[i], colTypes.get(i));
        }

        return new ColumnarBatch(nColumnarVectors, batchSize);
    }

    private static OmniColumnVector vecToOmniNativeColumnVector(Vec vec, TypeNode type) {
        int size = vec.getSize();
        OmniColumnVector vector = new OmniColumnVector(size, OmniColumnVector.populateVec(type), false);
        vector.setVec(vec);
        return vector;
    }

    public static StructNode getStructNodeFromProto(Type.Struct struct) {
        List<TypeNode> types = new ArrayList<>();
        for (int i = 0; i < struct.getTypesCount(); i++) {
            types.add(getTypeNode(struct.getTypes(i)));
        }
        return new StructNode(false, types);
    }

    private static TypeNode getTypeNode(Type type) {
        Type.KindCase kindCase = type.getKindCase();
        switch (kindCase) {
            case BOOL:
                return new BooleanTypeNode(isNullable(type.getBool().getNullability()));
            case I8:
                return new I8TypeNode(isNullable(type.getI8().getNullability()));
            case I16:
                return new I16TypeNode(isNullable(type.getI16().getNullability()));
            case I32:
                return new I32TypeNode(isNullable(type.getI32().getNullability()));
            case I64:
                return new I64TypeNode(isNullable(type.getI64().getNullability()));
            case FP64:
                return new FP64TypeNode(isNullable(type.getFp64().getNullability()));
            case DATE:
                return new DateTypeNode(isNullable(type.getDate().getNullability()));
            case DECIMAL:
                return new DecimalTypeNode(
                        isNullable(type.getDecimal().getNullability()),
                        type.getDecimal().getPrecision(),
                        type.getDecimal().getScale());
            case STRING:
                return new StringTypeNode(isNullable(type.getString().getNullability()));
            default:
                throw new RuntimeException("Unsupported TypeNode: " + type);
        }
    }

    private static Boolean isNullable(Type.Nullability nullability) {
        return nullability == Type.Nullability.NULLABILITY_NULLABLE;
    }

    public static Vec populateVec(TypeNode typeNode, int size, String value) {
        String simpleName = typeNode.getClass().getSimpleName();
        switch (simpleName) {
            case "BooleanTypeNode":
                BooleanVec booleanVec = new BooleanVec(size);
                if (value == null) {
                    byte[] nulls = new byte[size];
                    Arrays.fill(nulls, (byte) 1);
                    booleanVec.setNulls(0, nulls, 0, size);
                } else {
                    boolean[] values = new boolean[size];
                    Arrays.fill(values, Boolean.parseBoolean(value));
                    booleanVec.put(values, 0, 0, size);
                }
                return booleanVec;
            case "I16TypeNode":
                ShortVec shortVec = new ShortVec(size);
                if (value == null) {
                    byte[] nulls = new byte[size];
                    Arrays.fill(nulls, (byte) 1);
                    shortVec.setNulls(0, nulls, 0, size);
                } else {
                    short[] values = new short[size];
                    Arrays.fill(values, Short.parseShort(value));
                    shortVec.put(values, 0, 0, size);
                }
                return shortVec;
            case "I32TypeNode":
                IntVec intVec = new IntVec(size);
                if (value == null) {
                    byte[] nulls = new byte[size];
                    Arrays.fill(nulls, (byte) 1);
                    intVec.setNulls(0, nulls, 0, size);
                } else {
                    int[] values = new int[size];
                    Arrays.fill(values, Integer.parseInt(value));
                    intVec.put(values, 0, 0, size);
                }
                return intVec;
            case "DateTypeNode":
                IntVec dateVec = new IntVec(size);
                if (value == null) {
                    byte[] nulls = new byte[size];
                    Arrays.fill(nulls, (byte) 1);
                    dateVec.setNulls(0, nulls, 0, size);
                } else {
                    int[] values = new int[size];
                    // Use LocalDate here
                    Arrays.fill(values, (int) LocalDate.parse(value).toEpochDay());
                    dateVec.put(values, 0, 0, size);
                }
                return dateVec;
            case "I64TypeNode":
                LongVec longVec = new LongVec(size);
                if (value == null) {
                    byte[] nulls = new byte[size];
                    Arrays.fill(nulls, (byte) 1);
                    longVec.setNulls(0, nulls, 0, size);
                } else {
                    long[] values = new long[size];
                    Arrays.fill(values, Long.parseLong(value));
                    longVec.put(values, 0, 0, size);
                }
                return longVec;
            case "VarCharTypeNode":
            case "StringTypeNode":
            case "FixedCharTypeNode":
                VarcharVec varcharVec = new VarcharVec(size);
                if (value == null) {
                    byte[] nulls = new byte[size];
                    Arrays.fill(nulls, (byte) 1);
                    varcharVec.setNulls(0, nulls, 0, size);
                } else {
                    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                    for (int i = 0; i < size; i++) {
                        varcharVec.set(i, bytes);
                    }
                }
                return varcharVec;
            case "DecimalTypeNode":
                DecimalTypeNode decimalTypeNode = (DecimalTypeNode) typeNode;
                if (decimalTypeNode.precision > MAX_LONG_DIGITS) {
                    Decimal128Vec decimal128Vec = new Decimal128Vec(size);
                    if (value == null) {
                        byte[] nulls = new byte[size];
                        Arrays.fill(nulls, (byte) 1);
                        decimal128Vec.setNulls(0, nulls, 0, size);
                    } else {
                        BigDecimal d = new BigDecimal(value);
                        for (int i = 0; i < size; i++) {
                            decimal128Vec.setBigInteger(i, d.unscaledValue());
                        }
                    }
                    return decimal128Vec;
                } else {
                    LongVec decimal64Vec = new LongVec(size);
                    if (value == null) {
                        byte[] nulls = new byte[size];
                        Arrays.fill(nulls, (byte) 1);
                        decimal64Vec.setNulls(0, nulls, 0, size);
                    } else {
                        long[] values = new long[size];
                        BigDecimal d = new BigDecimal(value);
                        Arrays.fill(values, d.unscaledValue().longValueExact());
                        decimal64Vec.put(values, 0, 0, size);
                    }
                    return decimal64Vec;
                }
            case "FP64TypeNode":
                DoubleVec doubleVec = new DoubleVec(size);
                if (value == null) {
                    byte[] nulls = new byte[size];
                    Arrays.fill(nulls, (byte) 1);
                    doubleVec.setNulls(0, nulls, 0, size);
                } else {
                    double[] values = new double[size];
                    Arrays.fill(values, Double.parseDouble(value));
                    doubleVec.put(values, 0, 0, size);
                }
                return doubleVec;
            case "TimestampTypeNode":
                LongVec timestampVec = new LongVec(size);
                if (value == null) {
                    byte[] nulls = new byte[size];
                    Arrays.fill(nulls, (byte) 1);
                    timestampVec.setNulls(0, nulls, 0, size);
                } else {
                    long[] values = new long[size];
                    Arrays.fill(values, Long.parseLong(value));
                    timestampVec.put(values, 0, 0, size);
                }
            default:
                throw new RuntimeException("Not supported partition type: " + simpleName);
        }
    }
}
