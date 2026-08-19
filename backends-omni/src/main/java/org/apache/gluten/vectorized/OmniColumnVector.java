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

import nova.hetu.omniruntime.utils.OmniRuntimeException;
import nova.hetu.omniruntime.vector.*;

import org.apache.gluten.expression.OmniExpressionAdaptor;
import org.apache.gluten.substrait.type.DecimalTypeNode;
import org.apache.gluten.substrait.type.ListNode;
import org.apache.gluten.substrait.type.MapNode;
import org.apache.gluten.substrait.type.StructNode;
import org.apache.gluten.substrait.type.TypeNode;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** OmniColumnVector */
public class OmniColumnVector extends WritableColumnVector {
    private static final boolean BIG_ENDIAN_PLATFORM =
            ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

    private static String resolveDuplicateFieldName(String fieldName, Set<String> usedFieldNames) {
        if (!usedFieldNames.contains(fieldName)) {
            usedFieldNames.add(fieldName);
            return fieldName;
        }
        int suffix = 1;
        String newFieldName;
        do {
            newFieldName = fieldName + "_" + suffix;
            suffix++;
        } while (usedFieldNames.contains(newFieldName));
        usedFieldNames.add(newFieldName);
        return newFieldName;
    }

    /**
     * Allocates columns to store elements of each field of the schema on heap. Capacity is the
     * initial capacity of the vector and it will grow as necessary. Capacity is in number of
     * elements, not number of bytes.
     */
    public static OmniColumnVector[] allocateColumns(
            int capacity, StructType schema, boolean initVec) {
        return allocateColumns(capacity, schema.fields(), initVec);
    }

    /**
     * Allocates columns to store elements of each field on heap. Capacity is the initial capacity of
     * the vector and it will grow as necessary. Capacity is in number of elements, not number of
     * bytes.
     */
    public static OmniColumnVector[] allocateColumns(
            int capacity, StructField[] fields, boolean initVec) {
        OmniColumnVector[] vectors = new OmniColumnVector[fields.length];
        try {
            for (int i = 0; i < fields.length; i++) {
                vectors[i] = new OmniColumnVector(capacity, fields[i].dataType(), initVec);
            }
            return vectors;
        } catch (Exception e) {
            if (initVec) {
                for (int i = 0; i < fields.length; i++) {
                    OmniColumnVector vec = vectors[i];
                    if (vec != null) {
                        vec.close();
                    }
                }
            }
            throw new RuntimeException("allocate columns failed. errmsg:" + e.getMessage());
        }
    }

    public static OmniColumnVector[] allocateColumns(int capacity, TypeNode[] typeNode, boolean initVec) {
        OmniColumnVector[] vectors = new OmniColumnVector[typeNode.length];
        try {
            for (int i = 0; i < typeNode.length; i++) {
                vectors[i] = new OmniColumnVector(capacity, populateVec(typeNode[i]), initVec);
            }
            return vectors;
        } catch (Exception e) {
            if (initVec) {
                for (int i = 0; i < typeNode.length; i++) {
                    OmniColumnVector vec = vectors[i];
                    if (vec != null) {
                        vec.close();
                    }
                }
            }
            throw new RuntimeException("allocate columns failed. errmsg:" + e.getMessage());
        }
    }

    public static DataType populateVec(TypeNode typeNode) {
        String simpleName = typeNode.getClass().getSimpleName();
        switch (simpleName) {
            case "NothingNode":
            case "BooleanTypeNode":
                return DataTypes.BooleanType;
            case "I16TypeNode":
                return DataTypes.ShortType;
            case "I8TypeNode":
                return DataTypes.ByteType;
            case "I32TypeNode":
                return DataTypes.IntegerType;
            case "DateTypeNode":
                return DataTypes.DateType;
            case "I64TypeNode":
                return DataTypes.LongType;
            case "VarCharTypeNode":
            case "StringTypeNode":
            case "FixedCharTypeNode":
                return DataTypes.StringType;
            case "BinaryTypeNode":
                return DataTypes.BinaryType;
            case "DecimalTypeNode":
                DecimalTypeNode decimalTypeNode = (DecimalTypeNode) typeNode;
                return new DecimalType(decimalTypeNode.precision, decimalTypeNode.scale);
            case "FP64TypeNode":
                return DataTypes.DoubleType;
            case "FP32TypeNode":
                return DataTypes.FloatType;
            case "TimestampTypeNode":
                return DataTypes.TimestampType;
            case "ListNode":
                ListNode listNode = (ListNode) typeNode;
                return DataTypes.createArrayType(populateVec(listNode.getNestedType()));
            case "MapNode":
                MapNode mapNode = (MapNode) typeNode;
                return new MapType(populateVec(mapNode.getKeyType()), populateVec(mapNode.getValueType()), true);
            case "StructNode":
                StructNode structNode = (StructNode) typeNode;
                List<TypeNode> fieldTypeNodes = structNode.getFieldTypes();
                List<DataType> fieldDataTypes = new ArrayList<>();
                for (TypeNode fieldTypeNode : fieldTypeNodes) {
                    fieldDataTypes.add(populateVec(fieldTypeNode));
                }
                List<StructField> structFields = new ArrayList<>();
                Set<String> usedFieldNames = new HashSet<>();
                boolean isNullable = structNode.getNullable() != null ? structNode.getNullable() : true;
                for (int i = 0; i < fieldDataTypes.size(); i++) {
                    String fieldName;
                    if (structNode.getNames() != null && i < structNode.getNames().size()) {
                        fieldName = resolveDuplicateFieldName(structNode.getNames().get(i), usedFieldNames);
                    } else {
                        fieldName = "field_" + i;
                    }

                    StructField structField = DataTypes.createStructField(
                        fieldName,
                        fieldDataTypes.get(i),
                        isNullable
                    );
                    structFields.add(structField);
                }
                return DataTypes.createStructType(structFields);
            default:
                throw new RuntimeException("Not supported partition type: " + simpleName);
        }
    }

    // The data stored in these arrays need to maintain binary compatible. We can
    // directly pass this buffer to external components.
    // This is faster than a boolean array and we optimize this over memory
    // footprint.
    // Array for each type. Only 1 is populated for any type.
    private BooleanVec booleanDataVec;
    private ByteVec byteDataVec;
    private ShortVec shortDataVec;
    private IntVec intDataVec;
    private LongVec longDataVec;
    private DoubleVec doubleDataVec;
    private FloatVec floatDataVec;
    private Decimal128Vec decimal128DataVec;
    private VarcharVec charsTypeDataVec;
    private DictionaryVec dictionaryData;
    private ArrayVec arrayDataVec;
    private MapVec mapDataVec;
    private StructVec structVec;
    private OmniColumnVector emptyStructDummyChild;

    private ConstVec constVec;

    // init vec
    private boolean initVec;
    private int[] offsets;

    public OmniColumnVector(int capacity, DataType type, boolean initVec) {
        super(capacity, type);
        this.initVec = initVec;
        if (this.initVec) {
            reserveInternal(capacity);
        }
        reset();
    }

    /**
     * get vec
     *
     * @return Vec
     */
    public Vec getVec() {
        if (constVec != null) {
            return constVec;
        }
        if (dictionaryData != null) {
            return dictionaryData;
        }

        if (type instanceof LongType || type instanceof TimestampType) {
            return longDataVec;
        } else if (type instanceof BooleanType || type instanceof NullType) {
            return booleanDataVec;
        } else if (type instanceof ShortType) {
            return shortDataVec;
        } else if (type instanceof IntegerType) {
            return intDataVec;
        } else if (type instanceof DecimalType) {
            if (DecimalType.is64BitDecimalType(type)) {
                return longDataVec;
            } else {
                return decimal128DataVec;
            }
        } else if (type instanceof DoubleType) {
            return doubleDataVec;
        } else if (type instanceof FloatType) {
            return floatDataVec;
        } else if (type instanceof StringType) {
            return charsTypeDataVec;
        } else if (type instanceof BinaryType) {
            return charsTypeDataVec;
        } else if (type instanceof DateType) {
            return intDataVec;
        } else if (type instanceof ByteType) {
            return byteDataVec;
        } else if (type instanceof ArrayType) {
            return arrayDataVec;
        } else if (type instanceof MapType) {
            return mapDataVec;
        } else if (type instanceof StructType) {
            return structVec;
        } else {
            return null;
        }
    }

    /**
     * set Vec
     *
     * @param vec Vec
     */
    public void setVec(Vec vec) {
        if (vec instanceof ConstVec) {
            this.constVec = (ConstVec) vec;
            return;
        }
        if (vec instanceof DictionaryVec) {
            dictionaryData = (DictionaryVec) vec;
        } else if (type instanceof LongType || type instanceof TimestampType) {
            this.longDataVec = (LongVec) vec;
        } else if (type instanceof DecimalType) {
            if (DecimalType.is64BitDecimalType(type)) {
                this.longDataVec = (LongVec) vec;
            } else {
                this.decimal128DataVec = (Decimal128Vec) vec;
            }
        } else if (type instanceof BooleanType || type instanceof NullType) {
            this.booleanDataVec = (BooleanVec) vec;
        } else if (type instanceof ShortType) {
            this.shortDataVec = (ShortVec) vec;
        } else if (type instanceof IntegerType) {
            this.intDataVec = (IntVec) vec;
        } else if (type instanceof DoubleType) {
            this.doubleDataVec = (DoubleVec) vec;
        } else if (type instanceof FloatType) {
            this.floatDataVec = (FloatVec) vec;
        } else if (type instanceof StringType) {
            this.charsTypeDataVec = (VarcharVec) vec;
        } else if (type instanceof BinaryType) {
            this.charsTypeDataVec = (VarcharVec) vec;
        } else if (type instanceof DateType) {
            this.intDataVec = (IntVec) vec;
        } else if (type instanceof ByteType) {
            this.byteDataVec = (ByteVec) vec;
        } else if (type instanceof ArrayType) {
            this.arrayDataVec = (ArrayVec) vec;
            ((OmniColumnVector)(getChild(0))).setVec(arrayDataVec.getElementVec());
        } else if (type instanceof MapType) {
            this.mapDataVec = (MapVec) vec;
            ((OmniColumnVector)(getChild(0))).setVec(mapDataVec.getKeyVec());
            ((OmniColumnVector)(getChild(1))).setVec(mapDataVec.getValueVec());
        } else if (type instanceof StructType) {
            bindStructVec(vec);
        } else {
            return;
        }
    }

    private void bindStructVec(Vec vec) {
        if (!(vec instanceof StructVec)) {
            throw new IllegalArgumentException("Expected StructVec");
        }
        if (!(type instanceof StructType)) {
            throw new IllegalStateException("Expected StructType");
        }
        StructType structType = (StructType) type;
        this.structVec = (StructVec) vec;
        for (int i = 0; i < structType.fields().length; i++) {
            ColumnVector child = getChild(i);
            if (!(child instanceof OmniColumnVector)) {
                throw new IllegalStateException("Expected OmniColumnVector child");
            }
            ((OmniColumnVector) child).setVec(structVec.getChild(i));
        }
        if (structType.fields().length == 0) {
            emptyStructDummyChild = new OmniColumnVector(capacity, DataTypes.BooleanType, true);
            structVec.setChild(0, emptyStructDummyChild.getVec());
        }
    }

    @Override
    public void close() {
        childColumns = null;
        super.close();
        if (constVec != null) {
            constVec.close();
            constVec = null;
        }
        if (booleanDataVec != null) {
            booleanDataVec.close();
            booleanDataVec = null;
        }
        if (byteDataVec != null) {
            byteDataVec.close();
            byteDataVec = null;
        }
        if (shortDataVec != null) {
            shortDataVec.close();
            shortDataVec = null;
        }
        if (intDataVec != null) {
            intDataVec.close();
            intDataVec = null;
        }
        if (longDataVec != null) {
            longDataVec.close();
            longDataVec = null;
        }
        if (doubleDataVec != null) {
            doubleDataVec.close();
            doubleDataVec = null;
        }
        if (floatDataVec != null) {
            floatDataVec.close();
            floatDataVec = null;
        }
        if (decimal128DataVec != null) {
            decimal128DataVec.close();
            decimal128DataVec = null;
        }
        if (charsTypeDataVec != null) {
            charsTypeDataVec.close();
            charsTypeDataVec = null;
        }
        if (dictionaryData != null) {
            dictionaryData.close();
            dictionaryData = null;
        }
        if (arrayDataVec != null) {
            arrayDataVec.close();
            arrayDataVec = null;
        }
        if (mapDataVec != null) {
            mapDataVec.close();
            mapDataVec = null;
        }
        if (structVec != null) {
            structVec.close();
            structVec = null;
        }
        if (emptyStructDummyChild != null) {
            emptyStructDummyChild.close();
            emptyStructDummyChild = null;
        }
    }

    //
    // APIs dealing with nulls
    //

    @Override
    public boolean hasNull() {
        if (constVec != null) {
            return constVec.hasNull();
        }
        if (dictionaryData != null) {
            return dictionaryData.hasNull();
        }
        if (type instanceof BooleanType || type instanceof NullType) {
            return booleanDataVec.hasNull();
        } else if (type instanceof ByteType) {
            return byteDataVec.hasNull();
        } else if (type instanceof ShortType) {
            return shortDataVec.hasNull();
        } else if (type instanceof IntegerType) {
            return intDataVec.hasNull();
        } else if (type instanceof DecimalType) {
            if (DecimalType.is64BitDecimalType(type)) {
                return longDataVec.hasNull();
            } else {
                return decimal128DataVec.hasNull();
            }
        } else if (type instanceof LongType
                || DecimalType.is64BitDecimalType(type)
                || type instanceof TimestampType) {
            return longDataVec.hasNull();
        } else if (type instanceof FloatType) {
            return floatDataVec.hasNull();
        } else if (type instanceof DoubleType) {
            return doubleDataVec.hasNull();
        } else if (type instanceof StringType) {
            return charsTypeDataVec.hasNull();
        } else if (type instanceof BinaryType) {
            return charsTypeDataVec.hasNull();
        } else if (type instanceof DateType) {
            return intDataVec.hasNull();
        } else if (type instanceof ArrayType) {
            return arrayDataVec.hasNull();
        } else if (type instanceof MapType) {
            return mapDataVec.hasNull();
        } else if (type instanceof StructType) {
            return structVec.hasNull();
        }
        throw new UnsupportedOperationException("hasNull is not supported for type:" + type);
    }

    @Override
    public int numNulls() {
        throw new UnsupportedOperationException("numNulls is not supported");
    }

    @Override
    public void putNotNull(int rowId) {}

    @Override
    public void putNull(int rowId) {
        if (dictionaryData != null) {
            dictionaryData.setNull(rowId);
            return;
        }
        if (type instanceof BooleanType || type instanceof NullType) {
            booleanDataVec.setNull(rowId);
        } else if (type instanceof ByteType) {
            byteDataVec.setNull(rowId);
        } else if (type instanceof ShortType) {
            shortDataVec.setNull(rowId);
        } else if (type instanceof IntegerType) {
            intDataVec.setNull(rowId);
        } else if (type instanceof DecimalType) {
            if (DecimalType.is64BitDecimalType(type)) {
                longDataVec.setNull(rowId);
            } else {
                decimal128DataVec.setNull(rowId);
            }
        } else if (type instanceof LongType
                || DecimalType.is64BitDecimalType(type)
                || type instanceof TimestampType) {
            longDataVec.setNull(rowId);
        } else if (type instanceof FloatType) {
            floatDataVec.setNull(rowId);
        } else if (type instanceof DoubleType) {
            doubleDataVec.setNull(rowId);
        } else if (type instanceof StringType) {
            charsTypeDataVec.setNull(rowId);
        } else if (type instanceof BinaryType) {
            charsTypeDataVec.setNull(rowId);
        } else if (type instanceof DateType) {
            intDataVec.setNull(rowId);
        } else if (type instanceof ArrayType) {
            arrayDataVec.setNull(rowId);
        } else if (type instanceof MapType) {
            mapDataVec.setNull(rowId);
        } else if (type instanceof StructType) {
            structVec.setNull(rowId);
        }
    }

    @Override
    public void putNulls(int rowId, int count) {
        byte[] nullValue = new byte[count];
        Arrays.fill(nullValue, (byte) 1);
        if (dictionaryData != null) {
            dictionaryData.setNulls(rowId, nullValue, 0, count);
            return;
        }
        if (type instanceof BooleanType || type instanceof NullType) {
            booleanDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof ByteType) {
            byteDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof ShortType) {
            shortDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof IntegerType) {
            intDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof DecimalType) {
            if (DecimalType.is64BitDecimalType(type)) {
                longDataVec.setNulls(rowId, nullValue, 0, count);
            } else {
                decimal128DataVec.setNulls(rowId, nullValue, 0, count);
            }
        } else if (type instanceof LongType
                || DecimalType.is64BitDecimalType(type)
                || type instanceof TimestampType) {
            longDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof FloatType) {
            floatDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof DoubleType) {
            doubleDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof StringType) {
            charsTypeDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof BinaryType) {
            charsTypeDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof DateType) {
            intDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof ArrayType) {
            arrayDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof MapType) {
            mapDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof StructType) {
            structVec.setNulls(rowId, nullValue, 0, count);
        }
    }

    public void putNulls(int rowId, byte[] nullValue, int count) {
        if (dictionaryData != null) {
            dictionaryData.setNulls(rowId, nullValue, 0, count);
            return;
        }
        if (type instanceof BooleanType || type instanceof NullType) {
            booleanDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof ByteType) {
            charsTypeDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof ShortType) {
            shortDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof IntegerType) {
            intDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof DecimalType) {
            if (DecimalType.is64BitDecimalType(type)) {
                longDataVec.setNulls(rowId, nullValue, 0, count);
            } else {
                decimal128DataVec.setNulls(rowId, nullValue, 0, count);
            }
        } else if (type instanceof LongType || DecimalType.is64BitDecimalType(type) || type instanceof TimestampType) {
            longDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof FloatType) {
            return;
        } else if (type instanceof DoubleType) {
            doubleDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof StringType) {
            charsTypeDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof BinaryType) {
            charsTypeDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof DateType) {
            intDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof ArrayType) {
            arrayDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof MapType) {
            mapDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof StructType) {
            structVec.setNulls(rowId, nullValue, 0, count);
        }
    }

    @Override
    public void putNotNulls(int rowId, int count) {}

    @Override
    public boolean isNullAt(int rowId) {
        if (constVec != null) {
            return constVec.isNull(rowId);
        }
        if (dictionaryData != null) {
            return dictionaryData.isNull(rowId);
        }
        if (type instanceof BooleanType || type instanceof NullType) {
            return booleanDataVec.isNull(rowId);
        } else if (type instanceof ByteType) {
            return byteDataVec.isNull(rowId);
        } else if (type instanceof ShortType) {
            return shortDataVec.isNull(rowId);
        } else if (type instanceof IntegerType) {
            return intDataVec.isNull(rowId);
        } else if (type instanceof DecimalType) {
            if (DecimalType.is64BitDecimalType(type)) {
                return longDataVec.isNull(rowId);
            } else {
                return decimal128DataVec.isNull(rowId);
            }
        } else if (type instanceof LongType || DecimalType.is64BitDecimalType(type)
                || type instanceof TimestampType) {
            return longDataVec.isNull(rowId);
        } else if (type instanceof FloatType) {
            return floatDataVec.isNull(rowId);
        } else if (type instanceof DoubleType) {
            return doubleDataVec.isNull(rowId);
        } else if (type instanceof StringType) {
            return charsTypeDataVec.isNull(rowId);
        } else if (type instanceof BinaryType) {
            return charsTypeDataVec.isNull(rowId);
        } else if (type instanceof DateType) {
            return intDataVec.isNull(rowId);
        } else if (type instanceof ArrayType) {
            return arrayDataVec.isNull(rowId);
        } else if (type instanceof MapType) {
            return mapDataVec.isNull(rowId);
        } else if (type instanceof StructType) {
            return structVec.isNull(rowId);
        } else {
            throw new UnsupportedOperationException("isNullAt is not supported for type:" + type);
        }
    }

    //
    // APIs dealing with Booleans
    //

    @Override
    public void putBoolean(int rowId, boolean value) {
        booleanDataVec.set(rowId, value);
    }

    @Override
    public void putBooleans(int rowId, int count, boolean value) {
        for (int i = 0; i < count; ++i) {
            booleanDataVec.set(i + rowId, value);
        }
    }

    public void putBooleans(int rowId, byte src) {
        booleanDataVec.set(rowId, (src & 1) == 1);
        booleanDataVec.set(rowId + 1, (src >>> 1 & 1) == 1);
        booleanDataVec.set(rowId + 2, (src >>> 2 & 1) == 1);
        booleanDataVec.set(rowId + 3, (src >>> 3 & 1) == 1);
        booleanDataVec.set(rowId + 4, (src >>> 4 & 1) == 1);
        booleanDataVec.set(rowId + 5, (src >>> 5 & 1) == 1);
        booleanDataVec.set(rowId + 6, (src >>> 6 & 1) == 1);
        booleanDataVec.set(rowId + 7, (src >>> 7 & 1) == 1);
    }

    @Override
    public boolean getBoolean(int rowId) {
        if (constVec != null) {
            return constVec.getConstBoolean();
        }
        if (dictionaryData != null) {
            return dictionaryData.getBoolean(rowId);
        }
        return booleanDataVec.get(rowId);
    }

    @Override
    public boolean[] getBooleans(int rowId, int count) {
        assert (dictionary == null);
        boolean[] array = new boolean[count];
        for (int i = 0; i < count; ++i) {
            array[i] = booleanDataVec.get(rowId + i);
        }
        return array;
    }

    //

    //
    // APIs dealing with Bytes
    //

    @Override
    public void putByte(int rowId, byte value) {
        if (type instanceof ByteType) {
            byteDataVec.set(rowId, value);
        } else {
            charsTypeDataVec.set(rowId, new byte[] {value});
        }
    }

    @Override
    public void putBytes(int rowId, int count, byte value) {
        for (int i = 0; i < count; ++i) {
            if (type instanceof ByteType) {
                byteDataVec.set(rowId + i, value);
            } else {
                charsTypeDataVec.set(rowId, new byte[] {value});
            }
        }
    }

    @Override
    public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
        if (type instanceof ByteType) {
            byteDataVec.put(src, rowId, srcIndex, count);
        } else {
            byte[] array = new byte[count];
            System.arraycopy(src, srcIndex, array, 0, count);
            charsTypeDataVec.set(rowId, array);
        }
    }

    /**
     * @param length length of string value
     * @param src src value
     * @param offset offset value
     * @return return count of elements
     */
    public final int appendString(int length, byte[] src, int offset) {
        reserve(elementsAppended + 1);
        int result = elementsAppended;
        putBytes(elementsAppended, length, src, offset);
        elementsAppended++;
        return result;
    }

    @Override
    public byte getByte(int rowId) {
        if (constVec != null) {
            return constVec.getConstByte();
        }
        if (dictionary != null) {
            return (byte) dictionary.decodeToInt(dictionaryIds.getDictId(rowId));
        } else if (dictionaryData != null) {
            return dictionaryData.getByte(rowId);
        } else if (type instanceof ByteType) {
            return byteDataVec.get(rowId);
        } else {
            return charsTypeDataVec.get(rowId)[0];
        }
    }

    @Override
    public byte[] getBytes(int rowId, int count) {
        assert (dictionary == null);
        byte[] array = new byte[count];
        for (int i = 0; i < count; i++) {
            if (type instanceof StringType) {
                array[i] = ((VarcharVec) ((OmniColumnVector) getChild(0)).getVec()).get(rowId + i)[0];
            } else if (type instanceof ByteType) {
                array[i] = byteDataVec.get(rowId + i);
            } else {
                throw new UnsupportedOperationException("getBytes is not supported for type:" + type);
            }
        }
        return array;
    }

    @Override
    public UTF8String getUTF8String(int rowId) {
        if (constVec != null) {
            return UTF8String.fromBytes(constVec.getConstBytes());
        }
        if (dictionaryData != null) {
            return UTF8String.fromBytes(dictionaryData.getBytes(rowId));
        } else {
            return UTF8String.fromBytes(charsTypeDataVec.get(rowId));
        }
    }

    @Override
    protected UTF8String getBytesAsUTF8String(int rowId, int count) {
        return UTF8String.fromBytes(getBytes(rowId, count), rowId, count);
    }

    public ByteBuffer getByteBuffer(int rowId, int count) {
        throw new UnsupportedOperationException("getByteBuffer is not supported");
    }

    @Override
    public byte[] getBinary(int rowId) {
        // binary and varchar are implemented the same way
        if (constVec != null) {
            return constVec.getConstBytes();
        }
        if (dictionaryData != null) {
            return dictionaryData.getBytes(rowId);
        } else {
            return charsTypeDataVec.get(rowId);
        }
    }

    //
    // APIs dealing with Shorts
    //

    @Override
    public void putShort(int rowId, short value) {
        shortDataVec.set(rowId, value);
    }

    @Override
    public void putShorts(int rowId, int count, short value) {
        for (int i = 0; i < count; ++i) {
            shortDataVec.set(i + rowId, value);
        }
    }

    @Override
    public void putShorts(int rowId, int count, short[] src, int srcIndex) {
        shortDataVec.put(src, rowId, srcIndex, count);
    }

    @Override
    public void putShorts(int rowId, int count, byte[] src, int srcIndex) {
        throw new UnsupportedOperationException("putShorts is not supported");
    }

    @Override
    public short getShort(int rowId) {
        if (constVec != null) {
            return constVec.getConstShort();
        }
        if (dictionary != null) {
            return (short) dictionary.decodeToInt(dictionaryIds.getDictId(rowId));
        } else if (dictionaryData != null) {
            return dictionaryData.getShort(rowId);
        } else {
            return shortDataVec.get(rowId);
        }
    }

    @Override
    public short[] getShorts(int rowId, int count) {
        assert (dictionary == null);
        short[] array = new short[count];
        for (int i = 0; i < count; i++) {
            array[i] = shortDataVec.get(rowId + i);
        }
        return array;
    }

    //
    // APIs dealing with Ints
    //

    @Override
    public void putInt(int rowId, int value) {
        intDataVec.set(rowId, value);
    }

    @Override
    public void putInts(int rowId, int count, int value) {
        for (int i = 0; i < count; ++i) {
            intDataVec.set(rowId + i, value);
        }
    }

    @Override
    public void putInts(int rowId, int count, int[] src, int srcIndex) {
        intDataVec.put(src, rowId, srcIndex, count);
    }

    @Override
    public void putInts(int rowId, int count, byte[] src, int srcIndex) {
        throw new UnsupportedOperationException("putInts is not supported");
    }

    @Override
    public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
        int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
        for (int i = 0; i < count; ++i, srcOffset += 4) {
            intDataVec.set(rowId + i, Platform.getInt(src, srcOffset));
            if (BIG_ENDIAN_PLATFORM) {
                intDataVec.set(rowId + i, Integer.reverseBytes(intDataVec.get(i + rowId)));
            }
        }
    }

    @Override
    public int getInt(int rowId) {
        if (constVec != null) {
            return constVec.getConstInt();
        }
        if (dictionary != null) {
            return dictionary.decodeToInt(dictionaryIds.getDictId(rowId));
        } else if (dictionaryData != null) {
            return dictionaryData.getInt(rowId);
        } else {
            return intDataVec.get(rowId);
        }
    }

    @Override
    public int[] getInts(int rowId, int count) {
        assert (dictionary == null);
        int[] array = new int[count];
        for (int i = 0; i < count; i++) {
            array[i] = intDataVec.get(rowId + i);
        }
        return array;
    }

    /**
     * Returns the dictionary Id for rowId. This should only be called when the ColumnVector is
     * dictionaryIds. We have this separate method for dictionaryIds as per SPARK-16928.
     */
    public int getDictId(int rowId) {
        assert (dictionary == null)
                : "A ColumnVector dictionary should not have a dictionary for itself.";
        return intDataVec.get(rowId);
    }

    //
    // APIs dealing with Longs
    //

    @Override
    public void putLong(int rowId, long value) {
        longDataVec.set(rowId, value);
    }

    @Override
    public void putLongs(int rowId, int count, long value) {
        for (int i = 0; i < count; ++i) {
            longDataVec.set(i + rowId, value);
        }
    }

    @Override
    public void putLongs(int rowId, int count, long[] src, int srcIndex) {
        longDataVec.put(src, rowId, srcIndex, count);
    }

    @Override
    public void putLongs(int rowId, int count, byte[] src, int srcIndex) {
        throw new UnsupportedOperationException("putLongs is not supported");
    }

    @Override
    public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
        int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
        for (int i = 0; i < count; ++i, srcOffset += 8) {
            longDataVec.set(i + rowId, Platform.getLong(src, srcOffset));
            if (BIG_ENDIAN_PLATFORM) {
                longDataVec.set(i + rowId, Long.reverseBytes(longDataVec.get(i + rowId)));
            }
        }
    }

    @Override
    public long getLong(int rowId) {
        if (constVec != null) {
            return constVec.getConstLong();
        }
        if (dictionary != null) {
            return dictionary.decodeToLong(dictionaryIds.getDictId(rowId));
        } else if (dictionaryData != null) {
            return dictionaryData.getLong(rowId);
        } else {
            return longDataVec.get(rowId);
        }
    }

    @Override
    public long[] getLongs(int rowId, int count) {
        assert (dictionary == null);
        long[] array = new long[count];
        for (int i = 0; i < count; i++) {
            array[i] = longDataVec.get(rowId + i);
        }
        return array;
    }

    //
    // APIs dealing with floats, omni-vector not support float data type
    //

    @Override
    public void putFloat(int rowId, float value) {
        floatDataVec.set(rowId, value);
    }

    @Override
    public void putFloats(int rowId, int count, float value) {
        for (int i = 0; i < count; i++) {
            floatDataVec.set(rowId + i, value);
        }
    }

    @Override
    public void putFloats(int rowId, int count, float[] src, int srcIndex) {
        throw new UnsupportedOperationException("putFloats is not supported");
    }

    @Override
    public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
        throw new UnsupportedOperationException("putFloats is not supported");
    }

    @Override
    public void putFloatsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
        if (!BIG_ENDIAN_PLATFORM) {
            putFloats(rowId, count, src, srcIndex);
        } else {
            ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; i < count; ++i) {
                floatDataVec.set(i + rowId, bb.getFloat(srcIndex + (8 * i)));
            }
        }
    }

    @Override
    public float getFloat(int rowId) {
        if (constVec != null) {
            return constVec.getConstFloat();
        }
        if (dictionary != null) {
            return dictionary.decodeToFloat(dictionaryIds.getDictId(rowId));
        } else if (dictionaryData != null) {
            return dictionaryData.getFloat(rowId);
        } else {
            return floatDataVec.get(rowId);
        }
    }

    @Override
    public float[] getFloats(int rowId, int count) {
        float[] array = new float[count];
        for (int i = 0; i < count; i++) {
            array[i] = floatDataVec.get(rowId + i);
        }
        return array;
    }

    //
    // APIs dealing with doubles
    //

    @Override
    public void putDouble(int rowId, double value) {
        doubleDataVec.set(rowId, value);
    }

    @Override
    public void putDoubles(int rowId, int count, double value) {
        for (int i = 0; i < count; i++) {
            doubleDataVec.set(rowId + i, value);
        }
    }

    @Override
    public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
        throw new UnsupportedOperationException("putDoubles is not supported");
    }

    @Override
    public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
        throw new UnsupportedOperationException("putDoubles is not supported");
    }

    @Override
    public void putDoublesLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
        if (!BIG_ENDIAN_PLATFORM) {
            putDoubles(rowId, count, src, srcIndex);
        } else {
            ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; i < count; ++i) {
                doubleDataVec.set(i + rowId, bb.getDouble(srcIndex + (8 * i)));
            }
        }
    }

    @Override
    public double getDouble(int rowId) {
        if (constVec != null) {
            return constVec.getConstDouble();
        }
        if (dictionary != null) {
            return dictionary.decodeToDouble(dictionaryIds.getDictId(rowId));
        } else if (dictionaryData != null) {
            return dictionaryData.getDouble(rowId);
        } else {
            return doubleDataVec.get(rowId);
        }
    }

    @Override
    public double[] getDoubles(int rowId, int count) {
        assert (dictionary == null);
        double[] array = new double[count];
        for (int i = 0; i < count; i++) {
            array[i] = doubleDataVec.get(rowId + i);
        }
        return array;
    }

    //
    // APIs dealing with Arrays
    //

    @Override
    public int getArrayLength(int rowId) {
        if (type instanceof ArrayType) {
            return (int)arrayDataVec.getSize(rowId);
        } else if (type instanceof MapType) {
            return (int)mapDataVec.getSize(rowId);
        } else {
            throw new UnsupportedOperationException("getArrayLength is not supported for other types");
        }
    }

    @Override
    public int getArrayOffset(int rowId) {
        if (type instanceof ArrayType) {
            return (int)arrayDataVec.getOffset(rowId);
        } else if (type instanceof MapType) {
            return (int)mapDataVec.getOffset(rowId);
        } else {
            throw new UnsupportedOperationException("getArrayOffset is not supported for other types");
        }
    }

    @Override
    public void putArray(int rowId, int offset, int length) {
        throw new UnsupportedOperationException("putArray is not supported");
    }

    //
    // APIs dealing with Byte Arrays
    //

    @Override
    public int putByteArray(int rowId, byte[] value, int offset, int length) {
        if (type instanceof StringType) {
            putBytes(rowId, length, value, offset);
            return length;
        } else if (type instanceof DecimalType && DecimalType.isByteArrayDecimalType(type)) {
            byte[] array = new byte[length];
            System.arraycopy(value, offset, array, 0, length);
            BigInteger bigInteger = new BigInteger(array);
            decimal128DataVec.setBigInteger(rowId, bigInteger);
            return length;
        } else {
            throw new UnsupportedOperationException("putByteArray is not supported for type" + type);
        }
    }

    /**
     * @param value BigDecimal
     * @return return count of elements
     */
    public final int appendDecimal(Decimal value) {
        reserve(elementsAppended + 1);
        int result = elementsAppended;
        if (value.precision() <= Decimal.MAX_LONG_DIGITS()) {
            longDataVec.set(elementsAppended, value.toUnscaledLong());
        } else {
            decimal128DataVec.setBigInteger(elementsAppended, value.toJavaBigDecimal().unscaledValue());
        }
        elementsAppended++;
        return result;
    }

    @Override
    public void putDecimal(int rowId, Decimal value, int precision) {
        if (precision <= Decimal.MAX_LONG_DIGITS()) {
            longDataVec.set(rowId, value.toUnscaledLong());
        } else {
            decimal128DataVec.setBigInteger(rowId, value.toJavaBigDecimal().unscaledValue());
        }
    }

    @Override
    public Decimal getDecimal(int rowId, int precision, int scale) {
        if (isNullAt(rowId)) return null;
        if (constVec != null) {
            if (precision <= Decimal.MAX_LONG_DIGITS()) {
                return Decimal.apply(constVec.getConstLong(), precision, scale);
            } else {
                BigInteger value = constVec.getConstDecimal128();
                return Decimal.apply(new BigDecimal(value, scale), precision, scale);
            }
        }
        if (precision <= Decimal.MAX_LONG_DIGITS()) {
            return Decimal.apply(getLong(rowId), precision, scale);
        } else {
            BigInteger value;
            if (dictionaryData != null) {
                value = Decimal128Vec.getDecimal(dictionaryData.getDecimal128(rowId));
            } else {
                value = decimal128DataVec.getBigInteger(rowId);
            }
            return Decimal.apply(new BigDecimal(value, scale), precision, scale);
        }
    }

    @Override
    public boolean isArray() {
        return type instanceof ArrayType;
    }

    // Spilt this function out since it is the slow path.
    @Override
    protected void reserveInternal(int newCapacity) {
        if (type instanceof BooleanType || type instanceof NullType) {
            booleanDataVec = new BooleanVec(newCapacity);
        } else if (type instanceof ByteType) {
            byteDataVec = new ByteVec(newCapacity);
        } else if (type instanceof ShortType) {
            shortDataVec = new ShortVec(newCapacity);
        } else if (type instanceof IntegerType) {
            intDataVec = new IntVec(newCapacity);
        } else if (type instanceof DecimalType) {
            if (DecimalType.is64BitDecimalType(type)) {
                longDataVec = new LongVec(newCapacity);
            } else {
                decimal128DataVec = new Decimal128Vec(newCapacity);
            }
        } else if (type instanceof LongType || type instanceof TimestampType) {
            longDataVec = new LongVec(newCapacity);
        } else if (type instanceof FloatType) {
            floatDataVec = new FloatVec(newCapacity);
        } else if (type instanceof DoubleType) {
            doubleDataVec = new DoubleVec(newCapacity);
        } else if (type instanceof StringType) {
            // need to set with real column size, suppose char(200) utf8
            charsTypeDataVec = new VarcharVec(newCapacity);
        } else if (type instanceof BinaryType) {
            // need to set with real column size, suppose char(200) utf8
            charsTypeDataVec = new VarcharVec(newCapacity);
        } else if (type instanceof DateType) {
            intDataVec = new IntVec(newCapacity);
        } else if (type instanceof ArrayType) {
            nova.hetu.omniruntime.type.ArrayDataType dataType = (nova.hetu.omniruntime.type.ArrayDataType) OmniExpressionAdaptor.sparkTypeToOmniTypeWithComplex(type, Metadata.empty());
            arrayDataVec = new ArrayVec(dataType, newCapacity, true);
        } else if (type instanceof MapType){
            nova.hetu.omniruntime.type.MapDataType dataType = (nova.hetu.omniruntime.type.MapDataType) OmniExpressionAdaptor.sparkTypeToOmniTypeWithComplex(type, Metadata.empty());
            mapDataVec = new MapVec(dataType, newCapacity, true);
        } else if (type instanceof StructType) {
            nova.hetu.omniruntime.type.StructDataType dataType = (nova.hetu.omniruntime.type.StructDataType) OmniExpressionAdaptor.sparkTypeToOmniTypeWithComplex(type, Metadata.empty());
            structVec = new StructVec(dataType, newCapacity, true);
            if (((StructType) type).fields().length == 0) {
                emptyStructDummyChild = new OmniColumnVector(newCapacity, DataTypes.BooleanType, true);
                structVec.setChild(0, emptyStructDummyChild.getVec());
            }
        } else {
            throw new UnsupportedOperationException("reserveInternal is not supported for type:" + type);
        }
        capacity = newCapacity;
    }

    @Override
    protected OmniColumnVector reserveNewColumn(int capacity, DataType type) {
        return new OmniColumnVector(capacity, type, false);
    }

    public void setChild(WritableColumnVector child, int index) {
        this.childColumns[index] = child;
    }

    public boolean isConstant() {
        return isConstant;
    }

    public int getCapacity() {
        return capacity;
    }

    public void updateVec() {
        if (type instanceof MapType) {
            mapDataVec.AddKeys(((OmniColumnVector) (getChild(0))).getVec());
            mapDataVec.AddValues(((OmniColumnVector) (getChild(1))).getVec());
            if (offsets == null) {
                throw new OmniRuntimeException("MapVec need offsets!");
            }
            mapDataVec.AddOffsets(offsets);
        } else if (type instanceof ArrayType) {
            arrayDataVec.addElements(((OmniColumnVector) (getChild(0))).getVec());
            if (offsets == null) {
                throw new OmniRuntimeException("ArrayVec need offsets!");
            }
            arrayDataVec.addOffsets(offsets);
        } else if (type instanceof StructType) {
            for (int i = 0; i < ((StructType) type).fields().length; i++) {
                structVec.setChild(i, ((OmniColumnVector) (getChild(i))).getVec());
            }
            if (((StructType) type).fields().length == 0 && emptyStructDummyChild != null) {
                structVec.setChild(0, emptyStructDummyChild.getVec());
            }
        }
    }

    public void setOffsets(int[] offsets) {
        this.offsets = offsets;
    }
}
