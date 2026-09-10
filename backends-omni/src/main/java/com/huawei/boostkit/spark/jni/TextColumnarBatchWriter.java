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

package com.huawei.boostkit.spark.jni;

import com.huawei.boostkit.write.jni.TextColumnarBatchJniWriter;

import nova.hetu.omniruntime.vector.Vec;

import org.apache.gluten.vectorized.OmniColumnVector;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.json.JSONObject;
import org.json.JSONArray;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Writes Spark columnar batches with the native Text writer.
 *
 * @since 2026/08/28
 */
public class TextColumnarBatchWriter {
    private final TextColumnarBatchJniWriter jniWriter = new TextColumnarBatchJniWriter();
    private long writer;

    /**
     * Initializes a native Text writer for the specified output path and schema.
     *
     * @param path output file path
     * @param dataSchema schema of the data columns to write
     * @param nativeConf native Text writer options
     */
    public void initializeWriterJava(
            Path path, StructType dataSchema, Map<String, String> nativeConf) {
        URI uri = path.toUri();
        JSONObject options = new JSONObject();
        options.put("uri", path.toString());
        options.put("host", uri.getHost() == null ? "" : uri.getHost());
        options.put("scheme", uri.getScheme() == null ? "" : uri.getScheme());
        options.put("port", uri.getPort());
        options.put("path", uri.getPath() == null ? "" : uri.getPath());
        nativeConf.forEach((key, value) -> options.put(key, value));
        StringJoiner types = new StringJoiner("\u001f");
        JSONArray names = new JSONArray();
        for (StructField field : dataSchema.fields()) {
            types.add(field.dataType().catalogString());
            names.put(field.name());
        }
        options.put("text_schema_types", types.toString());
        options.put("text_schema_names", names.toString());
        writer = jniWriter.initializeWriter(options);
    }

    /**
     * Writes all rows from the data columns of a columnar batch.
     *
     * @param dataColumnIds flags identifying data columns in the batch
     * @param batch columnar batch to write
     * @throws IllegalArgumentException if the column mask is invalid or a data column is not an
     *         {@link OmniColumnVector}
     */
    public void write(boolean[] dataColumnIds, ColumnarBatch batch) {
        write(dataColumnIds, batch, 0, batch.numRows());
    }

    /**
     * Writes a row range from the data columns of a columnar batch.
     *
     * @param dataColumnIds flags identifying data columns in the batch
     * @param batch columnar batch to write
     * @param startPos inclusive start row index
     * @param endPos exclusive end row index
     * @throws IllegalArgumentException if the column mask is invalid or a data column is not an
     *         {@link OmniColumnVector}
     */
    public void write(
            boolean[] dataColumnIds, ColumnarBatch batch, long startPos, long endPos) {
        int[] dataColumnIndexes = findDataColumns(dataColumnIds, batch.numCols());
        long[] nativeVectors = new long[dataColumnIndexes.length];
        for (int index = 0; index < dataColumnIndexes.length; ++index) {
            ColumnVector columnVector = batch.column(dataColumnIndexes[index]);
            if (!(columnVector instanceof OmniColumnVector)) {
                throw new IllegalArgumentException("Text writer requires OmniColumnVector input.");
            }
            OmniColumnVector omniVector = (OmniColumnVector) columnVector;
            Vec vector = omniVector.getVec();
            nativeVectors[index] = vector.getNativeVector();
        }
        jniWriter.write(writer, nativeVectors, startPos, endPos);
    }

    private int[] findDataColumns(boolean[] dataColumnIds, int columnCount) {
        if (dataColumnIds.length != columnCount) {
            throw new IllegalArgumentException("Text writer column mask does not match the batch.");
        }
        List<Integer> indexes = new ArrayList<>();
        for (int index = 0; index < dataColumnIds.length; ++index) {
            if (dataColumnIds[index]) {
                indexes.add(index);
            }
        }
        if (indexes.isEmpty()) {
            throw new IllegalArgumentException("Text writer requires at least one data column.");
        }
        return indexes.stream().mapToInt(Integer::intValue).toArray();
    }

    /**
     * Closes the native Text writer if it has been initialized.
     */
    public void close() {
        if (writer != 0) {
            long writerToClose = writer;
            writer = 0;
            jniWriter.close(writerToClose);
        }
    }
}
