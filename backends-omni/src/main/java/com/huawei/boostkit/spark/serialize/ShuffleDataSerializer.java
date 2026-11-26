/**
 * Copyright (C) 2025-2026. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.boostkit.spark.serialize;

import nova.hetu.omniruntime.type.DataType.DataTypeId;
import nova.hetu.omniruntime.utils.OmniRuntimeException;
import nova.hetu.omniruntime.type.ArrayDataType;
import nova.hetu.omniruntime.type.DecimalDataType;
import nova.hetu.omniruntime.type.MapDataType;
import nova.hetu.omniruntime.type.StructDataType;
import nova.hetu.omniruntime.vector.BooleanVec;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.ShortVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.FloatVec;
import nova.hetu.omniruntime.vector.ByteVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.ComplexVec;
import nova.hetu.omniruntime.vector.ArrayVec;
import nova.hetu.omniruntime.vector.MapVec;
import nova.hetu.omniruntime.vector.StructVec;
import org.apache.gluten.vectorized.OmniColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class ShuffleDataSerializer {
    private static final Unsafe unsafe;
    private static final long BYTE_ARRAY_BASE_OFFSET;

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            BYTE_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(byte[].class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("create unsafe object failed. errmsg:" + e.getMessage());
        }
    }

    /**
     * Deserializes one shuffle block into a columnar batch.
     *
     * The task pool address is handed to the native parse context so the JNI vector
     * constructors in deserializer.cpp charge the vectors to the task. This method has no
     * callers in the current gluten tree: the read path goes through
     * ShuffleColumnarBatchOutIterator, makeNativeDeserializer and Decompression.cc, so the
     * parameter is plumbed for symmetry.
     *
     * @param isRowShuffle whether the block holds row-shuffle data
     * @param bytes buffer holding the block
     * @param readSize number of valid bytes in the buffer
     * @param taskPoolAddress address of the task's Omni memory pool; 0 outside a Spark task,
     *        where the vectors are created without a pool
     * @return the deserialized batch
     */
    public static ColumnarBatch deserialize(boolean isRowShuffle, byte[] bytes, int readSize,
                                            long taskPoolAddress) {
        ColumnVector[] vecs = null;
        long address = -1;
        ShuffleDataSerializerUtils deserializer = null;
        try {
            address = unsafe.allocateMemory(readSize);
            unsafe.copyMemory(bytes, BYTE_ARRAY_BASE_OFFSET, null, address, readSize);

            deserializer = new ShuffleDataSerializerUtils();
            deserializer.init(address, readSize, isRowShuffle, taskPoolAddress);
            int vecCount = deserializer.getVecCount();
            int rowCount = deserializer.getRowCount();

            int[] typeIdArray = new int[vecCount];
            int[] precisionArray = new int[vecCount];
            int[] scaleArray = new int[vecCount];
            long[] vecNativeIdArray = new long[vecCount];
            deserializer.parse(typeIdArray, precisionArray, scaleArray, vecNativeIdArray);
            vecs = new ColumnVector[vecCount];
            for (int i = 0; i < vecCount; i++) {
                vecs[i] = buildVec(typeIdArray[i], vecNativeIdArray[i], rowCount, precisionArray[i], scaleArray[i]);
            }
            deserializer.close();
            unsafe.freeMemory(address);
            return new ColumnarBatch(vecs, rowCount);
        } catch (OmniRuntimeException e) {
            if (vecs != null) {
                for (int i = 0; i < vecs.length; i++) {
                    ColumnVector vec = vecs[i];
                    if (vec != null) {
                        vec.close();
                    }
                }
            }
            if (deserializer != null) {
                deserializer.close();
            }
            if (address != -1) {
                unsafe.freeMemory(address);
            }
            throw new RuntimeException("deserialize failed. errmsg:" + e.getMessage());
        }
    }

    /**
     * build ColumnVector
     *
     * @param typeId type id
     * @param vecNativeId native vector id
     * @param vecSize vector size
     * @param precision decimal type precision
     * @param scale decimal type scale
     *
     * @return return ColumnVector instance
     */
    public static ColumnVector buildVector(int typeId, long vecNativeId, int vecSize, int precision, int scale) {
        return buildVec(typeId, vecNativeId, vecSize, precision, scale);
    }

    private static ColumnVector buildVec(int typeId, long vecNativeId, int vecSize, int precision, int scale) {
        Vec vec;
        DataType type;
        switch (DataTypeId.fromValue(typeId)) {
            case OMNI_INT:
                type = DataTypes.IntegerType;
                vec = new IntVec(vecNativeId);
                break;
            case OMNI_DATE32:
                type = DataTypes.DateType;
                vec = new IntVec(vecNativeId);
                break;
            case OMNI_LONG:
                type = DataTypes.LongType;
                vec = new LongVec(vecNativeId);
                break;
            case OMNI_TIMESTAMP:
                type = DataTypes.TimestampType;
                vec = new LongVec(vecNativeId);
                break;
            case OMNI_DATE64:
                type = DataTypes.DateType;
                vec = new LongVec(vecNativeId);
                break;
            case OMNI_DECIMAL64:
                type = DataTypes.createDecimalType(precision, scale);
                vec = new LongVec(vecNativeId);
                break;
            case OMNI_SHORT:
                type = DataTypes.ShortType;
                vec = new ShortVec(vecNativeId);
                break;
            case OMNI_BOOLEAN:
                type = DataTypes.BooleanType;
                vec = new BooleanVec(vecNativeId);
                break;
            case OMNI_DOUBLE:
                type = DataTypes.DoubleType;
                vec = new DoubleVec(vecNativeId);
                break;
            case OMNI_FLOAT:
                type = DataTypes.FloatType;
                vec = new FloatVec(vecNativeId);
                break;
            case OMNI_VARBINARY:
            case OMNI_VARCHAR:
            case OMNI_CHAR:
                type = DataTypes.StringType;
                vec = new VarcharVec(vecNativeId);
                break;
            case OMNI_DECIMAL128:
                type = DataTypes.createDecimalType(precision, scale);
                vec = new Decimal128Vec(vecNativeId);
                break;
            case OMNI_BYTE:
                type = DataTypes.ByteType;
                vec = new ByteVec(vecNativeId);
                break;
            case OMNI_ARRAY:
                ArrayDataType arrayDataType = (ArrayDataType) ComplexVec.getComplexDataType(vecNativeId);
                nova.hetu.omniruntime.type.DataType dataType = arrayDataType.getElementType();
                type = DataTypes.createArrayType(createDataTypeFromOmniType(dataType, precision, scale));
                vec = new ArrayVec(vecNativeId, arrayDataType);
                ArrayVec arrayVec = (ArrayVec) vec;
                break;
            case OMNI_MAP:
                MapDataType mapDataType = (MapDataType) ComplexVec.getComplexDataType(vecNativeId);
                nova.hetu.omniruntime.type.DataType keyDataType = mapDataType.getKeyType();
                nova.hetu.omniruntime.type.DataType valueDataType = mapDataType.getValueType();
                DataType keyType = createDataTypeFromOmniType(keyDataType, precision, scale);
                DataType valueType = createDataTypeFromOmniType(valueDataType, precision, scale);
                type = DataTypes.createMapType(keyType, valueType);
                vec = new MapVec(vecNativeId, mapDataType);
                MapVec mapVec = (MapVec) vec;
                break;
            case OMNI_ROW:
                StructDataType structDataType = (StructDataType) ComplexVec.getComplexDataType(vecNativeId);
                nova.hetu.omniruntime.type.DataType[] fieldTypes = structDataType.getFieldTypes();
                String[] fieldNames = structDataType.getFieldNames();

                // construct the list of StructField for Spark
                StructField[] fields = new StructField[fieldTypes.length];
                for (int i = 0; i < fieldTypes.length; i++) {
                    DataType fieldType = createDataTypeFromOmniType(fieldTypes[i], precision, scale);
                    String fieldName = (fieldNames != null && i < fieldNames.length) ? fieldNames[i] : "col" + i;
                    fields[i] = DataTypes.createStructField(fieldName, fieldType, true);
                }

                type = DataTypes.createStructType(fields);
                vec = new StructVec(vecNativeId, structDataType);
                break;
            case OMNI_TIME32:
            case OMNI_TIME64:
            case OMNI_INTERVAL_DAY_TIME:
            case OMNI_INTERVAL_MONTHS:
            default:
                throw new IllegalStateException("Unexpected value: " + typeId);
        }
        OmniColumnVector vecTmp = new OmniColumnVector(vecSize, type, false);
        vecTmp.setVec(vec);
        return vecTmp;
    }

    private static DataType createDataTypeFromOmniType(nova.hetu.omniruntime.type.DataType omniDataType,
                                                       int precision, int scale) {
        if (omniDataType == null) {
            throw new IllegalArgumentException("omniDataType is null");
        }

        DataTypeId dataTypeId = omniDataType.getId();
        switch (dataTypeId) {
            case OMNI_INT:
                return DataTypes.IntegerType;
            case OMNI_DATE32:
                return DataTypes.DateType;
            case OMNI_LONG:
                return DataTypes.LongType;
            case OMNI_TIMESTAMP:
                return DataTypes.TimestampType;
            case OMNI_DATE64:
                return DataTypes.DateType;
            case OMNI_DECIMAL64:
                if (omniDataType instanceof DecimalDataType) {
                    DecimalDataType dec64Type = (DecimalDataType) omniDataType;
                    return DataTypes.createDecimalType(dec64Type.getPrecision(), dec64Type.getScale());
                }
                return DataTypes.createDecimalType(precision, scale);
            case OMNI_SHORT:
                return DataTypes.ShortType;
            case OMNI_BOOLEAN:
                return DataTypes.BooleanType;
            case OMNI_DOUBLE:
                return DataTypes.DoubleType;
            case OMNI_FLOAT:
                return DataTypes.FloatType;
            case OMNI_VARBINARY:
            case OMNI_VARCHAR:
            case OMNI_CHAR:
                return DataTypes.StringType;
            case OMNI_DECIMAL128:
                if (omniDataType instanceof DecimalDataType) {
                    DecimalDataType dec128Type = (DecimalDataType) omniDataType;
                    return DataTypes.createDecimalType(dec128Type.getPrecision(), dec128Type.getScale());
                }
                return DataTypes.createDecimalType(precision, scale);
            case OMNI_BYTE:
                return DataTypes.ByteType;
            case OMNI_ARRAY:
                ArrayDataType arrayType = (ArrayDataType) omniDataType;
                nova.hetu.omniruntime.type.DataType elementType = arrayType.getElementType();
                DataType sparkElementType = createDataTypeFromOmniType(elementType, precision, scale);
                return DataTypes.createArrayType(sparkElementType);
            case OMNI_MAP:
                MapDataType mapType = (MapDataType) omniDataType;
                nova.hetu.omniruntime.type.DataType keyType = mapType.getKeyType();
                nova.hetu.omniruntime.type.DataType valueType = mapType.getValueType();
                DataType sparkKeyType = createDataTypeFromOmniType(keyType, precision, scale);
                DataType sparkValueType = createDataTypeFromOmniType(valueType, precision, scale);
                return DataTypes.createMapType(sparkKeyType, sparkValueType);
            case OMNI_ROW:
                StructDataType structType = (StructDataType) omniDataType;
                nova.hetu.omniruntime.type.DataType[] fieldTypes = structType.getFieldTypes();
                String[] fieldNames = structType.getFieldNames();

                StructField[] fields = new StructField[fieldTypes.length];
                for (int i = 0; i < fieldTypes.length; i++) {
                    DataType sparkFieldType = createDataTypeFromOmniType(fieldTypes[i], precision, scale);
                    String fieldName = (fieldNames != null && i < fieldNames.length) ? fieldNames[i] : "col" + i;
                    fields[i] = DataTypes.createStructField(fieldName, sparkFieldType, true);
                }
                return DataTypes.createStructType(fields);
            default:
                throw new IllegalStateException("Unexpected value: " + dataTypeId);
        }
    }
}
