/*
 * Copyright (C) 2021-2023. Huawei Technologies Co., Ltd. All rights reserved.
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

import com.huawei.boostkit.spark.predicate.AndPredicateCondition;
import com.huawei.boostkit.spark.predicate.LeafPredicateCondition;
import com.huawei.boostkit.spark.predicate.NotPredicateCondition;
import com.huawei.boostkit.spark.predicate.OrPredicateCondition;
import com.huawei.boostkit.spark.predicate.PredicateCondition;
import com.huawei.boostkit.spark.predicate.PredicateOperatorType;
import com.huawei.boostkit.spark.timestamp.JulianGregorianRebase;
import com.huawei.boostkit.spark.timestamp.TimestampUtil;

import nova.hetu.omniruntime.type.DataType;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gluten.expression.OmniExpressionAdaptor;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.sql.catalyst.util.RebaseDateTime;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Parquet PushFilterBuilder
 *
 * @since 2025/12/23
 */
public class ParquetPushFilterBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetPushFilterBuilder.class);
    private static final int MAX_LEAF_THRESHOLD = 256;

    // All Parquet fieldNames
    private ArrayList<String> allFieldsNames = new ArrayList<>();

    // Actual columns to read
    private ArrayList<String> includedColumns = new ArrayList<>();

    // max threshold for leaf node
    private int leafIndex;

    // spark data schema
    private StructType dataSchema;

    // spark required schema
    private StructType requiredSchema;

    // Parquet rebase modes
    private String datetimeRebaseMode;
    private String int96RebaseMode;

    private int[] vecTypeIds;
    private List<DataType> dataTypes = new ArrayList<>();

    public ParquetPushFilterBuilder(StructType dataSchema, StructType requiredSchema,
                                    String datetimeRebaseMode,
                                    String int96RebaseMode) {
        this.dataSchema = dataSchema;
        this.requiredSchema = requiredSchema;
        this.datetimeRebaseMode = datetimeRebaseMode;
        this.int96RebaseMode = int96RebaseMode;

        for (StructField field : dataSchema.fields()) {
            allFieldsNames.add(field.name());
        }
        initDataColIds();
    }

    /**
     * Add parquet datetime and int96 rebase info to the json object.
     *
     * @param job JSONObject to store the parquet datetime rebase related information
     */
    private void addParquetRebaseInfo(JSONObject job) {
        boolean hasTimeType = Arrays.stream(requiredSchema.fields()).anyMatch(field ->
                field.dataType() instanceof TimestampType || field.dataType() instanceof DateType);
        if (!hasTimeType) {
            return;
        }

        TimestampUtil instance = TimestampUtil.getInstance();
        if (datetimeRebaseMode != null) {
            JulianGregorianRebase julianObject = instance.getJulianObject(null);
            if (julianObject != null) {
                JSONObject timestampRebase = new JSONObject();
                timestampRebase.put("tz", julianObject.getTz());

                if (julianObject.getSwitches() != null && julianObject.getSwitches().length > 0) {
                    String switchesStr = StringUtils.join(
                            Arrays.stream(julianObject.getSwitches())
                                    .mapToObj(String::valueOf)
                                    .toArray(), ",");
                    timestampRebase.put("switches", switchesStr);
                } else {
                    timestampRebase.put("switches", "");
                }

                if (julianObject.getDiffs() != null && julianObject.getDiffs().length > 0) {
                    String diffsStr = StringUtils.join(
                            Arrays.stream(julianObject.getDiffs())
                                    .mapToObj(String::valueOf)
                                    .toArray(), ",");
                    timestampRebase.put("diffs", diffsStr);
                } else {
                    timestampRebase.put("diffs", "");
                }

                timestampRebase.put("mode", rebaseModeId(datetimeRebaseMode));
                job.put("timestampRebase", timestampRebase);
            }
        }
        if (int96RebaseMode != null) {
            JulianGregorianRebase julianObject = instance.getJulianObject(null);
            if (julianObject != null) {
                JSONObject int96Rebase = new JSONObject();
                int96Rebase.put("tz", julianObject.getTz());

                if (julianObject.getSwitches() != null && julianObject.getSwitches().length > 0) {
                    String switchesStr = StringUtils.join(
                            Arrays.stream(julianObject.getSwitches())
                                    .mapToObj(String::valueOf)
                                    .toArray(), ",");
                    int96Rebase.put("switches", switchesStr);
                } else {
                    int96Rebase.put("switches", "");
                }

                if (julianObject.getDiffs() != null && julianObject.getDiffs().length > 0) {
                    String diffsStr = StringUtils.join(
                            Arrays.stream(julianObject.getDiffs())
                                    .mapToObj(String::valueOf)
                                    .toArray(), ",");
                    int96Rebase.put("diffs", diffsStr);
                } else {
                    int96Rebase.put("diffs", "");
                }

                int96Rebase.put("mode", rebaseModeId(int96RebaseMode));
                job.put("int96Rebase", int96Rebase);
            }
        }
        job.put("lastSwitchJulianTs", RebaseDateTime.lastSwitchJulianTs());
    }

    private int rebaseModeId(String mode) {
        if ("LEGACY".equals(mode)) {
            return 1;
        }
        if ("CORRECTED".equals(mode)) {
            return 2;
        }
        return 0;
    }

    private void initDataColIds() {
        // find requiredS fieldNames
        String[] requiredFieldNames = requiredSchema.fieldNames();
        // save valid cols and numbers of valid cols
        int[] colsToGet = new int[requiredFieldNames.length];
        // collect read cols types
        ArrayList<Integer> typeBuilder = new ArrayList<>();
        Stack<String> prefix = new Stack<>();
        for (int i = 0; i < requiredFieldNames.length; i++) {
            String target = requiredFieldNames[i];
            // if not find, set colsToGet value -1, else set colsToGet 0
            if (allFieldsNames.contains(target)) {
                colsToGet[i] = 0;
                StructField field = requiredSchema.fields()[i];
                if (field.dataType() instanceof StructType) {
                    prefix.push(field.name());
                    buildRequireSchema((StructType) field.dataType(), includedColumns, prefix);
                    prefix.pop();
                } else {
                    includedColumns.add(target);
                }
                nova.hetu.omniruntime.type.DataType dataType =
                        OmniExpressionAdaptor.sparkTypeToOmniType(field.dataType(), field.metadata());

                typeBuilder.add(dataType.getId().toValue());
                dataTypes.add(dataType);
            } else {
                colsToGet[i] = -1;
            }
        }
        vecTypeIds = typeBuilder.stream().mapToInt(Integer::intValue).toArray();
    }

    private void buildRequireSchema(StructType structType, List<String> includedColumns, Stack<String> prefix) {
        StringBuilder sb = new StringBuilder();
        for (String s : prefix) {
            sb.append(s).append(".");
        }
        for (StructField field : structType.fields()) {
            String name = field.name();
            if (field.dataType() instanceof StructType) {
                prefix.push(name);
                buildRequireSchema((StructType) field.dataType(), includedColumns, prefix);
                prefix.pop();
            } else {
                includedColumns.add(new StringBuilder(sb).append(name).toString());
            }
        }
    }

    /**
     * buildPushFilterJson
     *
     * @param pushedFilter       Filter
     * @param isVecPredicateFilter Boolean
     * @param isFilterPushDown     Boolean
     * @return json
     */
    public String buildPushFilterJson(Filter pushedFilter,
                                      Boolean isVecPredicateFilter, Boolean isFilterPushDown) {
        JSONObject job = new JSONObject();
        try {
            if (pushedFilter != null) {
                if (isFilterPushDown != null && isFilterPushDown) {
                    JSONObject jsonExpressionTree = new JSONObject();
                    boolean canFilterPushDown = buildCppCompatibleExprTree(pushedFilter, jsonExpressionTree);
                    if (canFilterPushDown) {
                        job.put("expressionTree", jsonExpressionTree);
                    }
                }
                if (isVecPredicateFilter != null && isVecPredicateFilter) {
                    String vecPredicateCondition = buildVecPredicateCondition(pushedFilter);
                    if (vecPredicateCondition != null) {
                        job.put("vecPredicateCondition", vecPredicateCondition);
                    }
                }
            }
        } catch (UnsupportedOperationException e) {
            LOGGER.debug(e.getMessage());
        }

        job.put("includedColumns", StringUtils.join(includedColumns, ","));
        String ugi = null;
        try {
            ugi = UserGroupInformation.getCurrentUser().toString();
        } catch (IOException e) {
            throw new UnsupportedOperationException("Failed to get current UserGroupInformation", e);
        }
        job.put("ugi", ugi);
        addParquetRebaseInfo(job);

        return job.toString();
    }

    private enum CppOperator {
        AND(0), // C++: And = 0
        OR(1), // C++: Or = 1
        NOT(2), // C++: Not = 2
        EQ(3), // C++: Eq = 3
        GT(4), // C++: Gt = 4
        GT_EQ(5), // C++: GtEq = 5
        LT(6), // C++: Lt = 6
        LT_EQ(7), // C++: LtEq = 7
        IS_NOT_NULL(8), // C++: IsNotNull = 8
        IS_NULL(9), // C++: IsNull = 9
        IN(10); // C++: IN = 10

        private final int code;
        CppOperator(int code) {
            this.code = code;
        }
        public int getCode() {
            return code;
        }
    }

    private enum ParquetPredicateDataType {
        LONG, FLOAT, STRING, DATE, DECIMAL, TIMESTAMP, BOOLEAN
    }

    private Pair<String, ParquetPredicateDataType> getParquetPredicateDataType(
            String attribute, StructType inputSchema) {
        if (attribute.contains(".")) {
            String[] args = attribute.split("\\.");
            StructField field = inputSchema.apply(args[0]);
            if (field.dataType() instanceof StructType) {
                StructType struct = (StructType) field.dataType();
                return getParquetPredicateDataType(args[1], struct);
            } else {
                throw new UnsupportedOperationException("Unsupported Parquet field type: "
                        + field.dataType().getClass().getSimpleName());
            }
        }
        StructField field = inputSchema.apply(attribute);
        return Pair.of(attribute, getParquetType(field));
    }

    private ParquetPredicateDataType getParquetType(StructField field) {
        org.apache.spark.sql.types.DataType dataType = field.dataType();
        if (dataType instanceof ByteType || dataType instanceof ShortType || dataType instanceof IntegerType
                || dataType instanceof LongType) {
            return ParquetPredicateDataType.LONG;
        } else if (dataType instanceof DoubleType) {
            return ParquetPredicateDataType.FLOAT;
        } else if (dataType instanceof StringType) {
            return ParquetPredicateDataType.STRING;
        } else if (dataType instanceof DateType) {
            return ParquetPredicateDataType.DATE;
        } else if (dataType instanceof DecimalType) {
            return ParquetPredicateDataType.DECIMAL;
        } else if (dataType instanceof BooleanType) {
            return ParquetPredicateDataType.BOOLEAN;
        } else if (dataType instanceof TimestampType) {
            return ParquetPredicateDataType.TIMESTAMP;
        } else {
            throw new UnsupportedOperationException("Unsupported Parquet data type: "
                    + dataType.getClass().getSimpleName());
        }
    }

    private boolean buildCppCompatibleExprTree(Filter pushedFilter, JSONObject jsonExpressionTree) {
        try {
            leafIndex = 0;
            buildExprTree(pushedFilter, jsonExpressionTree);
            if (leafIndex > MAX_LEAF_THRESHOLD) {
                throw new UnsupportedOperationException("Leaf node count exceeds threshold: " + leafIndex);
            }
            return true;
        } catch (UnsupportedOperationException e) {
            LOGGER.warn("Parquet filter push down failed: {}", e.getMessage(), e);
            return false;
        }
    }

    private void buildExprTree(Filter filterPredicate, JSONObject jsonExpressionTree) {
        if (filterPredicate instanceof And) {
            jsonExpressionTree.put("op", CppOperator.AND.getCode());
            JSONObject left = new JSONObject();
            buildExprTree(((And) filterPredicate).left(), left);
            jsonExpressionTree.put("left", left);

            JSONObject right = new JSONObject();
            buildExprTree(((And) filterPredicate).right(), right);
            jsonExpressionTree.put("right", right);
        } else if (filterPredicate instanceof Or) {
            jsonExpressionTree.put("op", CppOperator.OR.getCode());
            JSONObject left = new JSONObject();
            buildExprTree(((Or) filterPredicate).left(), left);
            jsonExpressionTree.put("left", left);

            JSONObject right = new JSONObject();
            buildExprTree(((Or) filterPredicate).right(), right);
            jsonExpressionTree.put("right", right);
        } else if (filterPredicate instanceof Not) {
            jsonExpressionTree.put("op", CppOperator.NOT.getCode());
            JSONObject predicate = new JSONObject();
            buildExprTree(((Not) filterPredicate).child(), predicate);
            jsonExpressionTree.put("predicate", predicate);
        } else if (filterPredicate instanceof EqualTo) {
            jsonExpressionTree.put("op", CppOperator.EQ.getCode());
            EqualTo eq = (EqualTo) filterPredicate;
            jsonExpressionTree.put("field", eq.attribute());
            addTypeAndLiteral(jsonExpressionTree, eq.attribute(), eq.value());
            leafIndex++;
        } else if (filterPredicate instanceof GreaterThan) {
            jsonExpressionTree.put("op", CppOperator.GT.getCode());
            GreaterThan gt = (GreaterThan) filterPredicate;
            jsonExpressionTree.put("field", gt.attribute());
            addTypeAndLiteral(jsonExpressionTree, gt.attribute(), gt.value());
            leafIndex++;
        } else if (filterPredicate instanceof GreaterThanOrEqual) {
            jsonExpressionTree.put("op", CppOperator.GT_EQ.getCode());
            GreaterThanOrEqual gte = (GreaterThanOrEqual) filterPredicate;
            jsonExpressionTree.put("field", gte.attribute());
            addTypeAndLiteral(jsonExpressionTree, gte.attribute(), gte.value());
            leafIndex++;
        } else if (filterPredicate instanceof LessThan) {
            jsonExpressionTree.put("op", CppOperator.LT.getCode());
            LessThan lt = (LessThan) filterPredicate;
            jsonExpressionTree.put("field", lt.attribute());
            addTypeAndLiteral(jsonExpressionTree, lt.attribute(), lt.value());
            leafIndex++;
        } else if (filterPredicate instanceof LessThanOrEqual) {
            jsonExpressionTree.put("op", CppOperator.LT_EQ.getCode());
            LessThanOrEqual lte = (LessThanOrEqual) filterPredicate;
            jsonExpressionTree.put("field", lte.attribute());
            addTypeAndLiteral(jsonExpressionTree, lte.attribute(), lte.value());
            leafIndex++;
        } else if (filterPredicate instanceof IsNotNull) {
            jsonExpressionTree.put("op", CppOperator.IS_NOT_NULL.getCode());
            jsonExpressionTree.put("field", ((IsNotNull) filterPredicate).attribute());
            leafIndex++;
        } else if (filterPredicate instanceof IsNull) {
            jsonExpressionTree.put("op", CppOperator.IS_NULL.getCode());
            jsonExpressionTree.put("field", ((IsNull) filterPredicate).attribute());
            leafIndex++;
        } else if (filterPredicate instanceof In) {
            jsonExpressionTree.put("op", CppOperator.IN.getCode());
            In in = (In) filterPredicate;
            jsonExpressionTree.put("field", in.attribute());

            Pair<String, ParquetPredicateDataType> typePair = getParquetPredicateDataType(
                    in.attribute(), requiredSchema);
            jsonExpressionTree.put("type", convertToCxxPredicateDataType(typePair.getValue()));

            JSONArray literalArray = new JSONArray();
            for (Object val : in.values()) {
                literalArray.put(getLiteralValue(val));
            }
            jsonExpressionTree.put("literal", literalArray);
            leafIndex++;
        } else {
            throw new UnsupportedOperationException("Unsupported Parquet filter operation: "
                    + filterPredicate.getClass().getSimpleName());
        }
    }

    private void addTypeAndLiteral(JSONObject json, String fieldName, Object literal) {
        Pair<String, ParquetPredicateDataType> typePair = getParquetPredicateDataType(fieldName, requiredSchema);
        json.put("type", convertToCxxPredicateDataType(typePair.getValue()));
        json.put("literal", getLiteralValue(literal));
    }

    private int convertToCxxPredicateDataType(ParquetPredicateDataType javaType) {
        switch (javaType) {
            case LONG: return 2; // C++ PredicateDataType::Long
            case FLOAT: return 8; // C++ PredicateDataType::Double
            case STRING: return 3; // C++ PredicateDataType::String
            case DATE: return 4; // C++ PredicateDataType::Date32
            case DECIMAL: return 5; // C++ PredicateDataType::Decimal
            case TIMESTAMP: return 9; // C++ PredicateDataType::Timestamp
            case BOOLEAN: return 6; // C++ PredicateDataType::Bool
            default: throw new UnsupportedOperationException("Unsupported type: " + javaType);
        }
    }

    private String getLiteralValue(Object literal) {
        // For null literal, the predicate will not be pushed down.
        if (literal == null) {
            throw new UnsupportedOperationException("Parquet filter does not support null literal");
        }

        // For Decimal Type, we use the special string format to represent, which is "$decimalVal
        // $precision $scale".
        // e.g., Decimal(9, 3) = 123456.789, it outputs "123456.789 9 3".
        // e.g., Decimal(9, 3) = 123456.7, it outputs "123456.700 9 3".
        if (literal instanceof BigDecimal) {
            BigDecimal value = (BigDecimal) literal;
            int precision = value.precision();
            int scale = value.scale();
            String[] split = value.toString().split("\\.");
            String padded = "";
            if (scale > 0) {
                padded = split.length > 1 ? split[1] : "";
                padded = String.format("%1$-" + scale + "s", padded).replace(' ', '0');
            }
            return split[0] + (scale > 0 ? "." + padded : "") + " " + precision + " " + scale;
        }

        // For Date Type, spark uses Gregorian in default, Parquet no need to convert.
        if (literal instanceof LocalDate) {
            return String.valueOf(((LocalDate) literal).toEpochDay());
        }

        if (literal instanceof String || literal instanceof Integer || literal instanceof Long
                || literal instanceof Boolean || literal instanceof Byte || literal instanceof Short
                || literal instanceof Double) {
            return literal.toString();
        }

        throw new UnsupportedOperationException("Unsupported Parquet literal type: "
                + literal.getClass().getSimpleName());
    }

    private String buildVecPredicateCondition(Filter filterPredicate) {
        Map<String, Integer> nameToIndex = IntStream.range(0, includedColumns.size())
                .boxed()
                .collect(Collectors.toMap(includedColumns::get, i -> i));
        return buildPredicateCondition(filterPredicate, nameToIndex).reduce().toString();
    }

    private PredicateCondition buildPredicateCondition(Filter filterPredicate, Map<String, Integer> nameToIndex) {
        if (filterPredicate instanceof And) {
            return new AndPredicateCondition(buildPredicateCondition(((And) filterPredicate).left(), nameToIndex),
                    buildPredicateCondition(((And) filterPredicate).right(), nameToIndex));
        } else if (filterPredicate instanceof Or) {
            return new OrPredicateCondition(buildPredicateCondition(((Or) filterPredicate).left(), nameToIndex),
                    buildPredicateCondition(((Or) filterPredicate).right(), nameToIndex));
        } else if (filterPredicate instanceof Not) {
            return new NotPredicateCondition(buildPredicateCondition(((Not) filterPredicate).child(), nameToIndex));
        } else if (filterPredicate instanceof EqualTo) {
            return buildLeafPredicateCondition(PredicateOperatorType.EQUAL_TO, ((EqualTo) filterPredicate).attribute(),
                    ((EqualTo) filterPredicate).value(), nameToIndex);
        } else if (filterPredicate instanceof GreaterThan) {
            return buildLeafPredicateCondition(PredicateOperatorType.GREATER_THAN,
                    ((GreaterThan) filterPredicate).attribute(), ((GreaterThan) filterPredicate).value(), nameToIndex);
        } else if (filterPredicate instanceof GreaterThanOrEqual) {
            return buildLeafPredicateCondition(
                    PredicateOperatorType.GREATER_THAN_OR_EQUAL,
                    ((GreaterThanOrEqual) filterPredicate).attribute(),
                    ((GreaterThanOrEqual) filterPredicate).value(), nameToIndex);
        } else if (filterPredicate instanceof LessThan) {
            return buildLeafPredicateCondition(PredicateOperatorType.LESS_THAN,
                    ((LessThan) filterPredicate).attribute(), ((LessThan) filterPredicate).value(), nameToIndex);
        } else if (filterPredicate instanceof LessThanOrEqual) {
            return buildLeafPredicateCondition(
                    PredicateOperatorType.LESS_THAN_OR_EQUAL,
                    ((LessThanOrEqual) filterPredicate).attribute(),
                    ((LessThanOrEqual) filterPredicate).value(), nameToIndex);
        } else if (filterPredicate instanceof IsNotNull) {
            return buildLeafPredicateCondition(PredicateOperatorType.IS_NOT_NULL,
                    ((IsNotNull) filterPredicate).attribute(), "-1", nameToIndex);
        } else if (filterPredicate instanceof IsNull) {
            return buildLeafPredicateCondition(PredicateOperatorType.IS_NULL,
                    ((IsNull) filterPredicate).attribute(), "-1", nameToIndex);
        } else {
            throw new UnsupportedOperationException("Unsupported Parquet vec predicate operation: "
                    + filterPredicate.getClass().getSimpleName());
        }
    }

    private PredicateCondition buildLeafPredicateCondition(PredicateOperatorType op, String attribute, Object literal,
                                                           Map<String, Integer> nameToIndex) {
        Integer index = nameToIndex.get(attribute);
        if (index == null) {
            throw new UnsupportedOperationException("Attribute not found: " + attribute);
        }
        if (op == PredicateOperatorType.IS_NOT_NULL || op == PredicateOperatorType.IS_NULL) {
            return new LeafPredicateCondition(op, index, DataType.DataTypeId.OMNI_INT, "-1");
        }
        DataType.DataTypeId dataType = getSupportPredicateDataType(attribute);
        String value = getLiteralValue(literal);
        return new LeafPredicateCondition(op, index, dataType, value);
    }

    private DataType.DataTypeId getSupportPredicateDataType(String attribute) {
        StructField field = requiredSchema.apply(attribute);
        org.apache.spark.sql.types.DataType dataType = field.dataType();
        if (dataType instanceof ByteType) {
            return DataType.DataTypeId.OMNI_BYTE;
        } else if (dataType instanceof ShortType) {
            return DataType.DataTypeId.OMNI_SHORT;
        } else if (dataType instanceof IntegerType) {
            return DataType.DataTypeId.OMNI_INT;
        } else if (dataType instanceof LongType) {
            return DataType.DataTypeId.OMNI_LONG;
        } else if (dataType instanceof DoubleType) {
            return DataType.DataTypeId.OMNI_DOUBLE;
        } else if (dataType instanceof DateType) {
            return DataType.DataTypeId.OMNI_DATE32;
        } else if (dataType instanceof BooleanType) {
            return DataType.DataTypeId.OMNI_BOOLEAN;
        } else {
            throw new UnsupportedOperationException("Unsupported Parquet vec data type: "
                    + dataType.getClass().getSimpleName());
        }
    }
}
