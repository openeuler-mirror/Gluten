/*
 * Copyright (C) 2025-2025. Huawei Technologies Co., Ltd. All rights reserved.
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
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gluten.expression.OmniExpressionAdaptor;
import org.apache.orc.impl.writer.TimestampTreeWriter;
import org.apache.spark.sql.catalyst.util.CharVarcharUtils;
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
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * OrcColumnarBatchScanReader
 *
 * @since 2025/6/17
 */
public class OrcPushFilterBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(OrcPushFilterBuilder.class);

    private static final Pattern CHAR_TYPE = Pattern.compile("char\\(\\s*(\\d+)\\s*\\)");

    private static final int MAX_LEAF_THRESHOLD = 256;

    /**
     * ORC reader native pointer
     */
    public long reader;

    /**
     * ORC record reader native pointer
     */
    public long recordReader;

    /**
     * ORC batch reader native pointer
     */
    public long batchReader;

    /**
     * All field names of ORC table schema
     */
    public ArrayList<String> allFieldsNames = new ArrayList<String>();

    /**
     * Column index array to mark whether it need to read
     */
    public int[] colsToGet;

    /**
     * Actual column names list to read for ORC scan
     */
    public ArrayList<String> includedColumns = new ArrayList<>();

    private boolean hasNativeSupportTimestampRebase;

    // max threshold for leaf node
    private int leafIndex;

    // spark data schema
    private StructType dataSchema;

    // spark required schema
    private StructType requiredSchema;

    private int[] vecTypeIds;
    private List<DataType> dataTypes = new ArrayList<>();


    public OrcPushFilterBuilder(StructType dataSchema, StructType requiredSchema) {
        this.dataSchema = dataSchema;
        this.requiredSchema = requiredSchema;
        for (StructField field : dataSchema.fields()) {
            allFieldsNames.add(field.name());
        }
        initDataColIds();
    }

    /**
     * Padding zero for decimal fractional part to align the specified scale length
     *
     * @param decimalStrArray decimal value split by dot, array length 1 means integer, 2 means have fractional part
     * @param decimalScale specified decimal scale length
     * @return padded fractional part string with zero
     */
    public String padZeroForDecimals(String[] decimalStrArray, int decimalScale) {
        String decimalVal = "";
        if (decimalStrArray.length == 2) {
            decimalVal = decimalStrArray[1];
        }
        // If the length of the formatted number string is insufficient, pad '0's.
        return String.format("%1$-" + decimalScale + "s", decimalVal).replace(' ', '0');
    }

    private long formatSecs(long secs) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long epoch;
        try {
            epoch = dateFormat.parse(TimestampTreeWriter.BASE_TIMESTAMP_STRING).getTime()
                    / TimestampTreeWriter.MILLIS_PER_SECOND;
        } catch (ParseException e) {
            throw new IllegalArgumentException("Failed to parse base timestamp string", e);
        }
        return secs - epoch;
    }

    private long formatNanos(int nanos) {
        if (nanos == 0) {
            return 0;
        } else if (nanos % 100 != 0) {
            return ((long) nanos) << 3;
        } else {
            int nanoVal = nanos;
            nanoVal /= 100;
            int trailingZeros = 1;
            while (nanoVal % 10 == 0 && trailingZeros < 7) {
                nanoVal /= 10;
                trailingZeros += 1;
            }
            return ((long) nanoVal) << 3 | trailingZeros;
        }
    }

    private void addJulianGregorianInfo(JSONObject job) {
        TimestampUtil instance = TimestampUtil.getInstance();
        JulianGregorianRebase julianObject = instance.getJulianObject(TimeZone.getDefault().getID());
        if (julianObject == null) {
            return;
        }
        job.put("tz", julianObject.getTz());
        job.put("switches", StringUtils.join(julianObject.getSwitches(), ','));
        job.put("diffs", StringUtils.join(julianObject.getDiffs(), ','));
        hasNativeSupportTimestampRebase = true;
    }

    private void initDataColIds() {
        // find requiredS fieldNames
        String[] requiredfieldNames = requiredSchema.fieldNames();
        // save valid cols and numbers of valid cols
        colsToGet = new int[requiredfieldNames.length];
        includedColumns = new ArrayList<>();
        // collect read cols types
        ArrayList<Integer> typeBuilder = new ArrayList<>();
        Stack<String> prefix = new Stack<>();
        for (int i = 0; i < requiredfieldNames.length; i++) {
            String target = requiredfieldNames[i];
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
                        OmniExpressionAdaptor.sparkTypeToOmniTypeWithComplex(field.dataType(), field.metadata());

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
     * Build ORC push down filter json string, convert Spark Filter to native supported JSON format
     *
     * @param pushedFilter Spark filter condition to push down
     * @param canVecPredicateFilter whether enable vector predicate filter
     * @param shouldFilterPushDown whether enable filter push down to native layer
     * @param shouldFilterWhileDecode whether enable native ORC filter-while-decode path
     * @return JSON string of filter condition
     */
    public String buildPushFilterJson(Filter pushedFilter,
                                      Boolean canVecPredicateFilter, Boolean shouldFilterPushDown,
                                      Boolean shouldFilterWhileDecode) {
        JSONObject job = new JSONObject();
        try {
            if (pushedFilter != null) {
                if (shouldFilterPushDown != null && shouldFilterPushDown) {
                    JSONObject jsonExpressionTree = new JSONObject();
                    JSONObject jsonLeaves = new JSONObject();
                    boolean canPushDownFlag = canPushDown(pushedFilter, jsonExpressionTree, jsonLeaves);
                    if (canPushDownFlag) {
                        job.put("expressionTree", jsonExpressionTree);
                        job.put("leaves", jsonLeaves);
                    }
                }
                if (canVecPredicateFilter != null && canVecPredicateFilter) {
                    String vecPredicateCondition = buildVecPredicateCondition(pushedFilter);
                    if (vecPredicateCondition != null) {
                        job.put("vecPredicateCondition", vecPredicateCondition);
                    }
                }
            }
        } catch (IllegalArgumentException | UnsupportedOperationException e) {
            LOGGER.debug(e.getMessage());
        }

        job.put("allColumns", StringUtils.join(allFieldsNames, ","));
        job.put("includedColumns", StringUtils.join(includedColumns, ","));
        // T0: filter-while-decode global switch, passed to native ReaderOptions::ParseEnhanceJson.
        job.put("filterWhileDecode", shouldFilterWhileDecode != null && shouldFilterWhileDecode);
        addJulianGregorianInfo(job);
        return job.toString();
    }

    /**
     * Convert Julian calendar days to Gregorian calendar days for Int vector
     *
     * @param intVec Int vector of date value in Julian days
     * @param rowNumber total row count of current vector
     */
    public void convertJulianToGreGorian(IntVec intVec, long rowNumber) {
        int gregorianValue;
        for (int rowIndex = 0; rowIndex < rowNumber; rowIndex++) {
            gregorianValue = RebaseDateTime.rebaseJulianToGregorianDays(intVec.get(rowIndex));
            intVec.set(rowIndex, gregorianValue);
        }
    }

    /**
     * Convert Julian calendar timestamp micros to Gregorian calendar timestamp micros for Long vector
     *
     * @param longVec Long vector of timestamp value in Julian micros
     * @param rowNumber total row count of current vector
     */
    public void convertJulianToGregorianMicros(LongVec longVec, long rowNumber) {
        long gregorianValue;
        for (int rowIndex = 0; rowIndex < rowNumber; rowIndex++) {
            gregorianValue = RebaseDateTime.rebaseJulianToGregorianMicros(longVec.get(rowIndex));
            longVec.set(rowIndex, gregorianValue);
        }
    }


    enum OrcOperator {
        OR,
        AND,
        NOT,
        LEAF,
        CONSTANT
    }

    enum OrcLeafOperator {
        EQUALS,
        NULL_SAFE_EQUALS,
        LESS_THAN,
        LESS_THAN_EQUALS,
        IN,
        BETWEEN, // not use, spark transfers it to gt and lt
        IS_NULL
    }

    enum OrcPredicateDataType {
        LONG, // all of integer types
        FLOAT, // float and double
        STRING, // string, char, varchar
        DATE,
        DECIMAL,
        TIMESTAMP,
        BOOLEAN,
        STRUCT
    }

    private Pair<String, OrcPredicateDataType> getOrcPredicateDataType(String attribute, StructType inputSchema) {
        if (attribute.contains(".")) {
            String[] args = attribute.split("\\.");
            StructField field = inputSchema.apply(args[0]);
            if (field.dataType() instanceof StructType) {
                StructType struct = (StructType) field.dataType();
                return getOrcPredicateDataType(args[1], struct);
            } else {
                throw new UnsupportedOperationException("Unsupported orc push down filter data type: "
                        + field.dataType().getClass().getSimpleName());
            }
        }
        StructField field = inputSchema.apply(attribute);
        return Pair.of(attribute, getType(field));
    }

    private OrcPredicateDataType getType(StructField field) {
        org.apache.spark.sql.types.DataType dataType = field.dataType();
        if (dataType instanceof ByteType || dataType instanceof ShortType || dataType instanceof IntegerType
                || dataType instanceof LongType) {
            return OrcPredicateDataType.LONG;
        } else if (dataType instanceof DoubleType) {
            return OrcPredicateDataType.FLOAT;
        } else if (dataType instanceof StringType) {
            if (isCharType(field.metadata())) {
                throw new UnsupportedOperationException("Unsupported orc push down filter data type: char");
            }
            return OrcPredicateDataType.STRING;
        } else if (dataType instanceof DateType) {
            return OrcPredicateDataType.DATE;
        } else if (dataType instanceof DecimalType) {
            return OrcPredicateDataType.DECIMAL;
        } else if (dataType instanceof BooleanType) {
            return OrcPredicateDataType.BOOLEAN;
        } else if (dataType instanceof StructType) {
            return OrcPredicateDataType.STRUCT;
        } else {
            throw new UnsupportedOperationException("Unsupported orc push down filter data type: "
                    + dataType.getClass().getSimpleName());
        }
    }

    // Check the type whether is char type, which orc native does not support push down
    private boolean isCharType(Metadata metadata) {
        if (metadata != null && !"{}".equals(metadata.toString())) {
            String rawTypeString = CharVarcharUtils.getRawTypeString(metadata).getOrElse(null);
            if (rawTypeString != null) {
                Matcher matcher = CHAR_TYPE.matcher(rawTypeString);
                return matcher.matches();
            }
        }
        return false;
    }

    private boolean canPushDown(Filter pushedFilter, JSONObject jsonExpressionTree,
                                JSONObject jsonLeaves) {
        try {
            getExprJson(pushedFilter, jsonExpressionTree, jsonLeaves);
            if (leafIndex > MAX_LEAF_THRESHOLD) {
                throw new UnsupportedOperationException("leaf node nums is " + leafIndex
                        + ", which is bigger than max threshold " + MAX_LEAF_THRESHOLD + ".");
            }
            return true;
        } catch (IllegalArgumentException | UnsupportedOperationException e) {
            LOGGER.error("Unable to push down orc filter because {}", e.getMessage());
            return false;
        }
    }

    private void getExprJson(Filter filterPredicate, JSONObject jsonExpressionTree,
                             JSONObject jsonLeaves) {
        if (filterPredicate instanceof And) {
            addChildJson(jsonExpressionTree, jsonLeaves, OrcOperator.AND,
                    ((And) filterPredicate).left(), ((And) filterPredicate).right());
        } else if (filterPredicate instanceof Or) {
            addChildJson(jsonExpressionTree, jsonLeaves, OrcOperator.OR,
                    ((Or) filterPredicate).left(), ((Or) filterPredicate).right());
        } else if (filterPredicate instanceof Not) {
            addChildJson(jsonExpressionTree, jsonLeaves, OrcOperator.NOT,
                    ((Not) filterPredicate).child());
        } else if (filterPredicate instanceof EqualTo) {
            addToJsonExpressionTree("leaf-" + leafIndex, jsonExpressionTree, false);
            addLiteralToJsonLeaves("leaf-" + leafIndex, OrcLeafOperator.EQUALS, jsonLeaves,
                    ((EqualTo) filterPredicate).attribute(), ((EqualTo) filterPredicate).value(), null);
            leafIndex++;
        } else if (filterPredicate instanceof GreaterThan) {
            addToJsonExpressionTree("leaf-" + leafIndex, jsonExpressionTree, true);
            addLiteralToJsonLeaves("leaf-" + leafIndex, OrcLeafOperator.LESS_THAN_EQUALS, jsonLeaves,
                    ((GreaterThan) filterPredicate).attribute(), ((GreaterThan) filterPredicate).value(), null);
            leafIndex++;
        } else if (filterPredicate instanceof GreaterThanOrEqual) {
            addToJsonExpressionTree("leaf-" + leafIndex, jsonExpressionTree, true);
            addLiteralToJsonLeaves("leaf-" + leafIndex, OrcLeafOperator.LESS_THAN, jsonLeaves,
                    ((GreaterThanOrEqual) filterPredicate).attribute(),
                    ((GreaterThanOrEqual) filterPredicate).value(), null);
            leafIndex++;
        } else if (filterPredicate instanceof LessThan) {
            addToJsonExpressionTree("leaf-" + leafIndex, jsonExpressionTree, false);
            addLiteralToJsonLeaves("leaf-" + leafIndex, OrcLeafOperator.LESS_THAN, jsonLeaves,
                    ((LessThan) filterPredicate).attribute(), ((LessThan) filterPredicate).value(), null);
            leafIndex++;
        } else if (filterPredicate instanceof LessThanOrEqual) {
            addToJsonExpressionTree("leaf-" + leafIndex, jsonExpressionTree, false);
            addLiteralToJsonLeaves("leaf-" + leafIndex, OrcLeafOperator.LESS_THAN_EQUALS, jsonLeaves,
                    ((LessThanOrEqual) filterPredicate).attribute(), ((LessThanOrEqual) filterPredicate).value(), null);
            leafIndex++;
            // For IsNotNull/IsNull/In, pass literal = "" to native to avoid throwing exception.
        } else if (filterPredicate instanceof IsNotNull) {
            addToJsonExpressionTree("leaf-" + leafIndex, jsonExpressionTree, true);
            addLiteralToJsonLeaves("leaf-" + leafIndex, OrcLeafOperator.IS_NULL, jsonLeaves,
                    ((IsNotNull) filterPredicate).attribute(), null, null);
            leafIndex++;
        } else if (filterPredicate instanceof IsNull) {
            addToJsonExpressionTree("leaf-" + leafIndex, jsonExpressionTree, false);
            addLiteralToJsonLeaves("leaf-" + leafIndex, OrcLeafOperator.IS_NULL, jsonLeaves,
                    ((IsNull) filterPredicate).attribute(), null, null);
            leafIndex++;
        } else if (filterPredicate instanceof In) {
            addToJsonExpressionTree("leaf-" + leafIndex, jsonExpressionTree, false);
            addLiteralToJsonLeaves("leaf-" + leafIndex, OrcLeafOperator.IN, jsonLeaves,
                    ((In) filterPredicate).attribute(), null, Arrays.stream(((In) filterPredicate).values()).toArray());
            leafIndex++;
        } else {
            throw new UnsupportedOperationException("Unsupported orc push down filter operation: "
                    + filterPredicate.getClass().getSimpleName());
        }
    }

    private void addLiteralToJsonLeaves(String leaf, OrcLeafOperator leafOperator, JSONObject jsonLeaves,
                                        String name, Object literal, Object[] literals) {
        JSONObject leafJson = new JSONObject();
        leafJson.put("op", leafOperator.ordinal());
        Pair<String, OrcPredicateDataType> args = getOrcPredicateDataType(name, requiredSchema);
        leafJson.put("name", args.getKey());

        if (literal != null) {
            leafJson.put("type", args.getValue().ordinal());
            leafJson.put("literal", getLiteralValue(literal));
        } else {
            leafJson.put("type", 2);
            leafJson.put("literal", "");
        }

        ArrayList<String> literalList = new ArrayList<>();
        if (literals != null) {
            for (Object lit : literals) {
                literalList.add(getLiteralValue(lit));
            }
        }
        leafJson.put("literalList", literalList);
        jsonLeaves.put(leaf, leafJson);
    }

    private void addToJsonExpressionTree(String leaf, JSONObject jsonExpressionTree, boolean isAddNot) {
        if (isAddNot) {
            jsonExpressionTree.put("op", OrcOperator.NOT.ordinal());
            ArrayList<JSONObject> child = new ArrayList<>();
            JSONObject subJson = new JSONObject();
            subJson.put("op", OrcOperator.LEAF.ordinal());
            subJson.put("leaf", leaf);
            child.add(subJson);
            jsonExpressionTree.put("child", child);
        } else {
            jsonExpressionTree.put("op", OrcOperator.LEAF.ordinal());
            jsonExpressionTree.put("leaf", leaf);
        }
    }

    private void addChildJson(JSONObject jsonExpressionTree, JSONObject jsonLeaves,
                              OrcOperator orcOperator, Filter... filters) {
        jsonExpressionTree.put("op", orcOperator.ordinal());
        ArrayList<Object> child = new ArrayList<>();
        for (Filter filter : filters) {
            JSONObject subJson = new JSONObject();
            getExprJson(filter, subJson, jsonLeaves);
            child.add(subJson);
        }
        jsonExpressionTree.put("child", child);
    }

    private String getLiteralValue(Object literal) {
        // For null literal, the predicate will not be pushed down.
        if (literal == null) {
            throw new UnsupportedOperationException("Unsupported orc push down filter for literal is null");
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
            if (scale == 0) {
                return split[0] + " " + precision + " " + scale;
            } else {
                String padded = padZeroForDecimals(split, scale);
                return split[0] + "." + padded + " " + precision + " " + scale;
            }
        }
        // For Date Type, spark uses Gregorian in default but orc uses Julian, which should be converted.
        if (literal instanceof LocalDate) {
            int epochDay = Math.toIntExact(((LocalDate) literal).toEpochDay());
            int rebased = RebaseDateTime.rebaseGregorianToJulianDays(epochDay);
            return String.valueOf(rebased);
        }
        if (literal instanceof String) {
            return (String) literal;
        }
        if (literal instanceof Integer || literal instanceof Long || literal instanceof Boolean
                || literal instanceof Byte || literal instanceof Short || literal instanceof Double) {
            return literal.toString();
        }
        throw new UnsupportedOperationException("Unsupported orc push down filter data type: "
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
            return buildLeafPredicateCondition(PredicateOperatorType.GREATER_THAN_OR_EQUAL,
                    ((GreaterThanOrEqual) filterPredicate).attribute(),
                    ((GreaterThanOrEqual) filterPredicate).value(), nameToIndex);
        } else if (filterPredicate instanceof LessThan) {
            return buildLeafPredicateCondition(PredicateOperatorType.LESS_THAN,
                    ((LessThan) filterPredicate).attribute(), ((LessThan) filterPredicate).value(), nameToIndex);
        } else if (filterPredicate instanceof LessThanOrEqual) {
            return buildLeafPredicateCondition(PredicateOperatorType.LESS_THAN_OR_EQUAL,
                    ((LessThanOrEqual) filterPredicate).attribute(),
                    ((LessThanOrEqual) filterPredicate).value(), nameToIndex);
        } else if (filterPredicate instanceof IsNotNull) {
            return buildLeafPredicateCondition(PredicateOperatorType.IS_NOT_NULL,
                    ((IsNotNull) filterPredicate).attribute(), "-1", nameToIndex);
        } else if (filterPredicate instanceof IsNull) {
            return buildLeafPredicateCondition(PredicateOperatorType.IS_NULL,
                    ((IsNull) filterPredicate).attribute(), "-1", nameToIndex);
        } else {
            throw new UnsupportedOperationException("Unsupported orc vec predicate operation: "
                    + filterPredicate.getClass().getSimpleName());
        }
    }

    private PredicateCondition buildLeafPredicateCondition(PredicateOperatorType op, String attribute, Object literal,
                                                           Map<String, Integer> nameToIndex) {
        Integer index = nameToIndex.get(attribute);
        if (index == null) {
            throw new UnsupportedOperationException("Attribute is not found in nameToIndex. attribute: " + attribute);
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
            throw new UnsupportedOperationException("Unsupported orc vec predicate data type: "
                    + dataType.getClass().getSimpleName());
        }
    }
}